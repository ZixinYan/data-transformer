package com.ml.datatransformer.dts.pipeline.impl.clean;

import com.ml.datatransformer.dts.pipeline.model.MapProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExtractSliceOperator implements MapProcessor {
    private final String keyName;
    private final List<String> targetFields;
    private final int length;
    private final String strategy; // DISCARD|PAD

    public ExtractSliceOperator() {
        this.keyName = "";
        this.targetFields = Collections.emptyList();
        this.length = 1;
        this.strategy = "DISCARD";
    }

    @SuppressWarnings("unchecked")
    public ExtractSliceOperator(Map<String, Object> config) {
        this.keyName = String.valueOf(config.getOrDefault("key_name", ""));
        Object t = config.get("target_fields");
        List<String> tmp = new ArrayList<>();
        if (t instanceof List) {
            for (Object o : (List<?>) t) tmp.add(String.valueOf(o));
        }
        this.targetFields = tmp;
        Object l = config.getOrDefault("length", 1);
        this.length = Integer.parseInt(String.valueOf(l));
        this.strategy = String.valueOf(config.getOrDefault("strategy", "DISCARD"));
    }

    @Override
    public Map<String, Object> process(Map<String, Object> data) {
        Object v = data.get(keyName);
        if (!(v instanceof List)) return data;
        List<?> list = (List<?>) v;
        List<?> slice;
        if (list.size() >= length) {
            slice = list.subList(0, length);
        } else {
            if ("PAD".equalsIgnoreCase(strategy)) {
                List<Object> padded = new ArrayList<>(list);
                while (padded.size() < length) padded.add(null);
                slice = padded;
            } else {
                slice = list;
            }
        }
        if (targetFields.isEmpty()) {
            data.put(keyName + "_slice", slice);
        } else if (targetFields.size() == 1) {
            data.put(targetFields.get(0), slice);
        } else {
            for (int i = 0; i < Math.min(targetFields.size(), slice.size()); i++) {
                data.put(targetFields.get(i), slice.get(i));
            }
        }
        return data;
    }

    @Override
    public String name() { return "clean:extract_slice"; }
}
