package com.ml.datatransformer.dts.pipeline.impl.clean;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ml.datatransformer.dts.pipeline.model.MapProcessor;

import java.util.*;

public class ExtractJsonOperator implements MapProcessor {
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> keys;
    private final String sourceField;

    public ExtractJsonOperator() {
        this.keys = Collections.emptyList();
        this.sourceField = "payload";
    }

    public ExtractJsonOperator(Map<String, Object> config) {
        Object k = config == null ? null : (config.get("key_name") != null ? config.get("key_name") : config.get("keys"));
        if (k instanceof List) {
            List<String> tmp = new ArrayList<>();
            for (Object o : (List<?>) k) tmp.add(String.valueOf(o));
            this.keys = tmp;
        } else {
            this.keys = Collections.emptyList();
        }
        Object sf = config == null ? null : config.getOrDefault("source_field", "payload");
        this.sourceField = String.valueOf(sf);
    }

    @Override
    public Map<String, Object> process(Map<String, Object> data) {
        if (data == null) return new HashMap<>();
        Object payload = data.get(sourceField);
        if (!(payload instanceof String)) return data;
        try {
            Map<String, Object> json = mapper.readValue((String) payload, new TypeReference<Map<String, Object>>() {});
            if (keys.isEmpty()) {
                data.putAll(json);
            } else {
                for (String key : keys) {
                    if (json.containsKey(key)) data.put(key, json.get(key));
                }
            }
        } catch (Exception ignored) {}
        return data;
    }

    @Override
    public String name() { return "clean:extract_json"; }
}
