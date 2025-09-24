package com.ml.datatransformer.dts.pipeline;

import com.ml.datatransformer.dts.pipeline.impl.DTSExecutor;
import com.ml.datatransformer.dts.pipeline.model.Collector;
import com.ml.datatransformer.dts.pipeline.model.Pipeline;
import com.ml.datatransformer.dts.pipeline.model.Processor;
import com.ml.datatransformer.dts.pipeline.model.Publisher;
import com.ml.datatransformer.dts.pipeline.model.Stage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * DTSPipelineBatch
 * - collector: I -> List<M>
 * - processors: (M -> M) 作用于单条
 * - publisher: M -> R 作用于单条
 * - execute: 并发（虚拟线程）处理整批，返回 List<R>
 *
 * @param <I> 输入类型（如 DTSRequest）
 * @param <M> 单条中间类型（如 Map<String,Object>）
 * @param <R> 单条发布结果类型（如 String/Boolean/DTO）
 */
public class DTSPipeline<I, M, R> implements Pipeline<I, List<R>> {

    private final Collector<I, List<M>> collector;
    private final List<Processor<M, M>> processors;
    private final Publisher<M, R> publisher;
    private final DTSExecutor dtsExecutor;

    public DTSPipeline(Collector<I, List<M>> collector,
                            List<Processor<M, M>> processors,
                            Publisher<M, R> publisher,
                            DTSExecutor dtsExecutor) {
        this.collector = Objects.requireNonNull(collector, "collector must not be null");
        this.processors = processors == null ? List.of() : new ArrayList<>(processors);
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.dtsExecutor = Objects.requireNonNull(dtsExecutor, "dtsExecutor must not be null");
    }

    @Override
    public List<R> execute(I input) {
        List<M> batch = collector.collect(input);
        if (batch == null || batch.isEmpty()) return List.of();

        List<Callable<R>> tasks = new ArrayList<>(batch.size());
        for (M item : batch) {
            tasks.add(() -> {
                M current = item;
                for (Processor<M, M> p : processors) {
                    current = p.process(current);
                }
                return publisher.publish(current);
            });
        }

        try {
            List<Future<R>> futures = new ArrayList<>(tasks.size());
            for (Callable<R> t : tasks) {
                futures.add(dtsExecutor.submit(t)); // 虚拟线程提交
            }
            List<R> results = new ArrayList<>(futures.size());
            for (Future<R> f : futures) {
                results.add(f.get());
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException("Batch pipeline failed", e);
        }
    }

    @Override
    public List<Stage> stages() {
        List<Stage> s = new ArrayList<>(1 + processors.size() + 1);
        s.add(collector);
        s.addAll(processors);
        s.add(publisher);
        return Collections.unmodifiableList(s);
    }
}