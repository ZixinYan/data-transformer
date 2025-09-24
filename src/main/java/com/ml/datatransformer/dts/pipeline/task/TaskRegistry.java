package com.ml.datatransformer.dts.pipeline.task;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskRegistry {
    private final Map<String, TaskDefinition> nameToTask = new ConcurrentHashMap<>();

    public void register(TaskDefinition def) {
        if (def == null || def.getName() == null) {
            return;
        }
        nameToTask.put(def.getName(), def);
    }

    public TaskDefinition get(String name) {
        return nameToTask.get(name);
    }
} 