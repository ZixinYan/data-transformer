package com.ml.datatransformer.dts.pipeline.task;

import lombok.Data;

import java.util.Map;

@Data
public class OperatorStep {
    private String type;
    private Map<String, Object> config;
} 