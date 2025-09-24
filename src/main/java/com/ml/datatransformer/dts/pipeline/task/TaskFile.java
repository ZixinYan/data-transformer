package com.ml.datatransformer.dts.pipeline.task;

import lombok.Data;

import java.util.List;

@Data
public class TaskFile {
    private List<TaskDefinition> tasks;
} 