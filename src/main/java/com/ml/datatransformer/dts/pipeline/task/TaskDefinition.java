package com.ml.datatransformer.dts.pipeline.task;

import lombok.Data;

import java.util.List;

@Data
public class TaskDefinition {
    private String type;
    private String name;
    private OperatorStep collect;
    private List<FlowDefinition> flows;
} 