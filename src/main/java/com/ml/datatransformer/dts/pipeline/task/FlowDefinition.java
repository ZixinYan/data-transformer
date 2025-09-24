package com.ml.datatransformer.dts.pipeline.task;

import lombok.Data;

import java.util.List;

@Data
public class FlowDefinition {
    private List<OperatorStep> clean;
    private List<OperatorStep> calculate;
    private List<OperatorStep> publish;
} 