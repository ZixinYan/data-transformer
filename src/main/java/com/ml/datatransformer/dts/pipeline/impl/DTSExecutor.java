package com.ml.datatransformer.dts.pipeline.impl;

import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.*;

/**
 * DTSExecutor
 * 使用虚拟线程执行任务；若 JDK 不支持则自动回退到普通线程池。
 */
@Component
public class DTSExecutor implements AutoCloseable {

    private final ExecutorService executor;

    public DTSExecutor() {
        this.executor = createExecutor();
    }

    private ExecutorService createExecutor() {
        /*
        try {
            // 使用反射检测方法是否存在，避免编译错误
            Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
            // 如果方法存在，说明是 JDK 21+
            Object threadBuilder = ofVirtualMethod.invoke(null);
            Method nameMethod = threadBuilder.getClass().getMethod("name", String.class, long.class);
            Object namedBuilder = nameMethod.invoke(threadBuilder, "dts-vt-", 0L);
            Method factoryMethod = namedBuilder.getClass().getMethod("factory");
            ThreadFactory factory = (ThreadFactory) factoryMethod.invoke(namedBuilder);
            return Executors.newThreadPerTaskExecutor(factory);
        } catch (Exception e) {
            // 回退到传统线程池
            return Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r);
                t.setName("dts-norm-" + t.getId());
                t.setDaemon(true);
                return t;
            });
        }

         */
        return Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("dts-norm-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}