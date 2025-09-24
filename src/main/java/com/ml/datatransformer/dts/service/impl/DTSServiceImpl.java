package com.ml.datatransformer.dts.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.dto.DTSResponse;
import com.ml.datatransformer.dts.pipeline.DTSPipeline;
import com.ml.datatransformer.dts.pipeline.impl.DTSExecutor;
import com.ml.datatransformer.dts.pipeline.impl.OperatorFactory;
import com.ml.datatransformer.dts.pipeline.model.Collector;
import com.ml.datatransformer.dts.pipeline.model.MapProcessor;
import com.ml.datatransformer.dts.pipeline.model.Processor;
import com.ml.datatransformer.dts.pipeline.model.Publisher;
import com.ml.datatransformer.dts.pipeline.task.FlowDefinition;
import com.ml.datatransformer.dts.pipeline.task.OperatorStep;
import com.ml.datatransformer.dts.pipeline.task.TaskDefinition;
import com.ml.datatransformer.dts.pipeline.task.TaskRegistry;
import com.ml.datatransformer.dts.service.DTSService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * DTSServiceImpl（批并发版本）
 * - 使用 OperatorFactory 构造 collector（批量）、processors（单条 Map→Map）、publisher（单条）
 * - 使用 DTSPipelineBatch 对 collector 返回的每条数据启动虚拟线程并发执行
 * - 返回 List<String>（每条结果 JSON）的整体 JSON 数组字符串
 */
@Service
@RequiredArgsConstructor
public class DTSServiceImpl implements DTSService {

    private final TaskRegistry taskRegistry;
    private final OperatorFactory operatorFactory;
    private final DTSExecutor dtsExecutor;

    private static final ObjectMapper OM = new ObjectMapper();

    @Override
    public DTSResponse execute(DTSRequest request) {
        TaskDefinition task = taskRegistry.get(request.getRuleId());
        if (task == null || task.getFlows() == null || task.getFlows().isEmpty()) {
            return new DTSResponse(request.getRuleId(), "no task or empty flows");
        }
        // 当前按第一个 flow 执行；如需多个 flow 可扩展策略（串行/并行/条件）
        FlowDefinition flow = task.getFlows().get(0);

        // 1) 批量 collector：I -> List<Map<String,Object>>
        Collector<DTSRequest, List<Map<String, Object>>> batchCollector = buildBatchCollector(task);

        // 2) 单条 processors（Map -> Map）
        List<Processor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        for (OperatorStep s : safe(flow.getClean()))     processors.add(cast(operatorFactory.createClean(s.getType(), s.getConfig())));
        for (OperatorStep s : safe(flow.getCalculate())) processors.add(cast(operatorFactory.createCalculate(s.getType(), s.getConfig())));

        // 3) 单条 publisher（Map -> String）：默认将每条结果转 JSON 字符串
        Publisher<Map<String, Object>, String> publisher = new Publisher<Map<String, Object>, String>() {
            @Override
            public String publish(Map<String, Object> result) {
                try {
                    return OM.writeValueAsString(result);
                } catch (Exception e) {
                    return String.valueOf(result);
                }
            }

            @Override
            public String name() {
                return "publish:json";
            }
        };

        // 4) 组装并发批流水线，并发执行任务
        DTSPipeline<DTSRequest, Map<String, Object>, String> pipeline =
                new DTSPipeline<>(batchCollector, processors, publisher, dtsExecutor);

        try {
            List<String> outputs = pipeline.execute(request);
            // 返回整体 JSON 数组，便于前端/调用方消费
            String jsonArray = OM.writeValueAsString(outputs);
            return new DTSResponse(request.getRuleId(), jsonArray);
        } catch (Exception e) {
            return new DTSResponse(request.getRuleId(), "error:" + e.getMessage());
        }
    }

    private Collector<DTSRequest, List<Map<String, Object>>> buildBatchCollector(TaskDefinition task) {
        OperatorStep collectStep = task.getCollect();
        if (collectStep != null && collectStep.getType() != null) {
            Collector<DTSRequest, ?> raw = (Collector<DTSRequest, ?>)
                    operatorFactory.createCollector(collectStep.getType(), collectStep.getConfig());

            return new Collector<DTSRequest, List<Map<String, Object>>>() {
                @Override
                public List<Map<String, Object>> collect(DTSRequest req) {
                    Object r = raw.collect(req);
                    if (r instanceof List<?> ls) {
                        if (ls.isEmpty()) return Collections.emptyList();
                        Object first = ls.get(0);
                        if (first instanceof Map) {
                            return (List<Map<String, Object>>) r;
                        } else {
                            List<Map<String, Object>> wrapped = new ArrayList<>(ls.size());
                            for (Object it : ls) {
                                Map<String, Object> ctx = new HashMap<>(4);
                                ctx.put("payload", it);
                                ctx.put("ruleId", req == null ? null : req.getRuleId());
                                wrapped.add(ctx);
                            }
                            return wrapped;
                        }
                    }
                    if (r instanceof Map<?, ?> m) {
                        Map<String, Object> ctx = new HashMap<>();
                        m.forEach((k, v) -> ctx.put(String.valueOf(k), v));
                        ctx.putIfAbsent("ruleId", req == null ? null : req.getRuleId());
                        return List.of(ctx);
                    }
                    Map<String, Object> one = new HashMap<>();
                    one.put("payload", r);
                    one.put("ruleId", req == null ? null : req.getRuleId());
                    return List.of(one);
                }

                @Override
                public String name() {
                    return "collector:adapter";
                }
            };
        }

        // 无 collect 配置时：把请求封装为单条
        return new Collector<DTSRequest, List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> collect(DTSRequest req) {
                Map<String, Object> one = new HashMap<>(4);
                one.put("payload", req == null ? null : req.getPayload());
                one.put("ruleId", req == null ? null : req.getRuleId());
                return List.of(one);
            }

            @Override
            public String name() {
                return "collector:fallback";
            }
        };
    }

    private <T> List<T> safe(List<T> list) { return list == null ? Collections.emptyList() : list; }

    @SuppressWarnings("unchecked")
    private Processor<Map<String, Object>, Map<String, Object>> cast(MapProcessor p) {
        return (Processor<Map<String, Object>, Map<String, Object>>) p;
    }
}