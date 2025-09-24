package com.ml.datatransformer.dts.pipeline.impl;


import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.pipeline.impl.calculate.AddOperator;
import com.ml.datatransformer.dts.pipeline.impl.clean.ExtractJsonOperator;
import com.ml.datatransformer.dts.pipeline.impl.clean.ExtractSliceOperator;
import com.ml.datatransformer.dts.pipeline.impl.clean.TypeCastOperator;
import com.ml.datatransformer.dts.pipeline.impl.collect.DbCollector;
import com.ml.datatransformer.dts.pipeline.impl.collect.EsCollector;
import com.ml.datatransformer.dts.pipeline.impl.publish.EsPublisher;
import com.ml.datatransformer.dts.pipeline.model.Collector;
import com.ml.datatransformer.dts.pipeline.model.MapProcessor;
import com.ml.datatransformer.dts.pipeline.model.Publisher;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * OperatorFactory
 * - 根据 type + config 实例化各层算子
 * - 采集/发布算子需要外部依赖，通过构造注入
 * - 配置的时候可以拓展 if
 */
@Component
@RequiredArgsConstructor
public class OperatorFactory {

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchOperations esOps;

    public Collector<DTSRequest, ?> createCollector(String type, Map<String, Object> config) {
        if ("es".equalsIgnoreCase(type)) return new EsCollector(esOps, config);
        if ("db".equalsIgnoreCase(type)) return new DbCollector(jdbcTemplate, config);
        return new Collector<DTSRequest, String>() {
            @Override
            public String collect(DTSRequest input) {
                return input == null ? "" : input.getPayload();
            }
            @Override
            public String name() {
                return "collect:noop";
            }
        };
    }

    public MapProcessor createClean(String type, Map<String, Object> config) {
        if ("extract_json".equalsIgnoreCase(type)) return new ExtractJsonOperator(config);
        if ("extract_slice".equalsIgnoreCase(type)) return new ExtractSliceOperator(config);
        if ("type_cast".equalsIgnoreCase(type)) return new TypeCastOperator(config);
        return new MapProcessor() {
            @Override
            public Map<String, Object> process(Map<String, Object> data) { return data; }
            @Override
            public String name() { return "clean:noop"; }
        };
    }

    public MapProcessor createCalculate(String type, Map<String, Object> config) {
        if ("add".equalsIgnoreCase(type)) return new AddOperator(config);
        return new MapProcessor() {
            @Override
            public Map<String, Object> process(Map<String, Object> data) { return data; }
            @Override
            public String name() { return "calculate:noop"; }
        };
    }


    public Publisher<?, ?> createPublisher(String type, Map<String, Object> config) {
        if ("es".equalsIgnoreCase(type)) return new EsPublisher(esOps, config);
        return new Publisher<List<Map<String, Object>>, Boolean>() {
            @Override
            public Boolean publish(List<Map<String, Object>> result) { return Boolean.TRUE; }
            @Override
            public String name() { return "publish:noop"; }
        };
    }
}