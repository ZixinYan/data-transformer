package com.ml.datatransformer.dts.pipeline.task;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TaskAutoLoader {

    private final YamlTaskLoader yamlTaskLoader;

    @PostConstruct
    public void init() {
        yamlTaskLoader.load();
    }
} 