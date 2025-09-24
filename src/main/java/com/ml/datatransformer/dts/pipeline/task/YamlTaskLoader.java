package com.ml.datatransformer.dts.pipeline.task;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class YamlTaskLoader {

    private final TaskRegistry registry;

    @Value("${dts.taskPath:}")
    private String taskPath;

    public void load() {
        if (taskPath == null || taskPath.isEmpty()) {
            log.warn("dts.taskPath is empty, skip loading tasks");
            return;
        }
        Path path = Path.of(taskPath);
        if (!Files.exists(path)) {
            log.warn("task file not exists: {}", taskPath);
            return;
        }
        try (InputStream is = Files.newInputStream(path)) {
            Yaml yaml = new Yaml();
            Iterable<Object> objects = yaml.loadAll(is);
            for (Object obj : objects) {
                if (obj instanceof List) {
                    List<?> list = (List<?>) obj;
                    for (Object item : list) {
                        TaskDefinition def = toTaskDefinition(item);
                        if (def != null) {
                            registry.register(def);
                        }
                    }
                } else {
                    TaskDefinition def = toTaskDefinition(obj);
                    if (def != null) {
                        registry.register(def);
                    }
                }
            }
            log.info("YAML tasks loaded from {}", taskPath);
        } catch (Exception e) {
            log.error("Failed to load YAML tasks from {}", taskPath, e);
        }
    }

    private TaskDefinition toTaskDefinition(Object obj) {
        //依赖 YAML 结构与 OperatorStep/FlowDefinition 字段名称一致
        try {
            var map = (java.util.Map<String, Object>) obj;
            TaskDefinition def = new TaskDefinition();
            def.setType((String) map.get("type"));
            def.setName((String) map.get("name"));
            Object collect = map.get("collect");
            if (collect instanceof java.util.Map) {
                OperatorStep step = new OperatorStep();
                var m = (java.util.Map<String, Object>) collect;
                step.setType((String) m.get("type"));
                step.setConfig((java.util.Map<String, Object>) m.get("config"));
                def.setCollect(step);
            }
            Object flows = map.get("flows");
            if (flows instanceof List) {
                List<?> flowsList = (List<?>) flows;
                java.util.ArrayList<FlowDefinition> converted = new java.util.ArrayList<>();
                for (Object flowObj : flowsList) {
                    if (flowObj instanceof java.util.Map) {
                        FlowDefinition fd = new FlowDefinition();
                        var fm = (java.util.Map<String, Object>) flowObj;
                        fd.setClean(readSteps(fm.get("clean")));
                        fd.setCalculate(readSteps(fm.get("calculate")));
                        fd.setPublish(readSteps(fm.get("publish")));
                        converted.add(fd);
                    }
                }
                def.setFlows(converted);
            }
            return def;
        } catch (Exception e) {
            log.error("convert task definition error", e);
            return null;
        }
    }

    private List<OperatorStep> readSteps(Object obj) {
        if (!(obj instanceof List)) {
            return java.util.Collections.emptyList();
        }
        List<?> list = (List<?>) obj;
        java.util.ArrayList<OperatorStep> steps = new java.util.ArrayList<>();
        for (Object item : list) {
            if (item instanceof java.util.Map) {
                var m = (java.util.Map<String, Object>) item;
                OperatorStep step = new OperatorStep();
                step.setType((String) m.get("type"));
                step.setConfig((java.util.Map<String, Object>) m.get("config"));
                steps.add(step);
            }
        }
        return steps;
    }
} 