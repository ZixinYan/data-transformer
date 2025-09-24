package com.ml.datatransformer.dts.pipeline.model;

public interface Processor<D, R> extends Stage {
    R process(D data);
} 