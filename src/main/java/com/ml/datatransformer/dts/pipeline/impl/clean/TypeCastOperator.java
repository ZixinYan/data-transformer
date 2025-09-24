package com.ml.datatransformer.dts.pipeline.impl.clean;

import com.ml.datatransformer.dts.pipeline.model.MapProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TypeCastOperator implements MapProcessor {
    private final Map<String, String> typeMap;

    public TypeCastOperator() {
        this.typeMap = Collections.emptyMap();
    }

    public TypeCastOperator(Map<String, Object> config) {
        Object tm = config == null ? null : config.get("type_map");
        if (tm instanceof Map) {
            Map<String, String> tmp = new HashMap<>();
            ((Map<?, ?>) tm).forEach((k, v) -> tmp.put(String.valueOf(k), String.valueOf(v)));
            this.typeMap = tmp;
        } else {
            this.typeMap = Collections.emptyMap();
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> data) {
        for (Map.Entry<String, String> e : typeMap.entrySet()) {
            String field = e.getKey();
            String target = e.getValue();
            Object val = data.get(field);
            if (val == null) continue;
            try {
                switch (target.toLowerCase()) {
                    case "int":
                    case "integer":
                        data.put(field, Integer.parseInt(String.valueOf(val)));
                        break;
                    case "long":
                        data.put(field, Long.parseLong(String.valueOf(val)));
                        break;
                    case "double":
                    case "float":
                        data.put(field, Double.parseDouble(String.valueOf(val)));
                        break;
                    case "boolean":
                        data.put(field, Boolean.parseBoolean(String.valueOf(val)));
                        break;
                    case "string":
                    default:
                        data.put(field, String.valueOf(val));
                }
            } catch (Exception ignored) {}
        }
        return data;
    }

    @Override
    public String name() { return "clean:type_cast"; }
}
