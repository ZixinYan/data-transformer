package com.ml.datatransformer.dts.pipeline.model;

public interface Publisher<R, O> extends Stage {
    O publish(R result);
} 