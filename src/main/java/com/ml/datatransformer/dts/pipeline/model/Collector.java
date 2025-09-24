package com.ml.datatransformer.dts.pipeline.model;

public interface Collector<DTSRequest, D> extends Stage {
    D collect(DTSRequest input);
}