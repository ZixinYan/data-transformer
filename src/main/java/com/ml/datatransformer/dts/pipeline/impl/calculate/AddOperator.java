package com.ml.datatransformer.dts.pipeline.impl.calculate;

import com.ml.datatransformer.dts.pipeline.model.MapProcessor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AddOperator
 * 对多个字段求和，并将结果写入指定字段。
 *
 * 配置示例：
 * {
 *   "fields_name": ["scheme_rate","interchange_rate","markup_rate"],
 *   "result_name": "total_rate"
 * }
 *
 * 规则：
 * - 按 fields_name 顺序累加；字段缺失或无法解析为数字则忽略
 * - 使用 BigDecimal 进行精确求和；空集合不写入结果
 */
public class AddOperator implements MapProcessor {

    private final List<String> fieldsName;
    private final String resultName;

    public AddOperator() {
        this.fieldsName = List.of();
        this.resultName = "sum";
    }

    public AddOperator(Map<String, Object> config) {
        this.fieldsName = toStringList(config, "fields_name");
        Object rn = config == null ? null : config.get("result_name");
        this.resultName = rn == null ? "sum" : String.valueOf(rn);
    }

    @Override
    public Map<String, Object> process(Map<String, Object> data) {
        if (fieldsName.isEmpty()) return data;

        BigDecimal total = BigDecimal.ZERO;
        boolean hasAny = false;

        for (String f : fieldsName) {
            Object v = data.get(f);
            BigDecimal num = toBigDecimal(v);
            if (num != null) {
                total = total.add(num);
                hasAny = true;
            }
        }

        if (hasAny) {
            data.put(resultName, total);
        }
        return data;
    }

    private BigDecimal toBigDecimal(Object v) {
        if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof Number) return new BigDecimal(((Number) v).toString());
        try {
            return new BigDecimal(String.valueOf(v));
        } catch (Exception e) {
            return null;
        }
    }

    private List<String> toStringList(Map<String, Object> cfg, String key) {
        Object v = cfg == null ? null : cfg.get(key);
        List<String> out = new ArrayList<>();
        if (v instanceof List<?> list) {
            for (Object o : list) out.add(String.valueOf(o));
        }
        return out;
    }

    @Override
    public String name() {
        return "calculate:add";
    }
}