package com.ml.datatransformer.dts.pipeline.impl.collect;


import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.pipeline.model.Collector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

/**
 * DbCollector
 * 从关系型数据库中采集记录。
 *
 * 配置项（config）示例：
 * - sql: "SELECT id, name, amount FROM t_order WHERE create_time BETWEEN ? AND ? LIMIT ?"
 * - params: ["${start_time}", "${end_time}", 200]  // 支持从 request.payload 或 data map 解析占位符，这里示例简化仅透传
 * - limit: 200
 *
 *
 * - 返回 List<Map<String,Object>>，列名为 Map 的 key
 */
@Slf4j
@RequiredArgsConstructor
public class DbCollector implements Collector<DTSRequest, List<Map<String, Object>>> {

    private final JdbcTemplate jdbcTemplate;
    private final Map<String, Object> config;

    @Override
    public List<Map<String, Object>> collect(DTSRequest input) {
        String sql = Objects.toString(config.getOrDefault("sql", ""), "");
        if (sql.isEmpty()) {
            log.warn("DbCollector sql is empty");
            return Collections.emptyList();
        }
        Object rawParams = config.get("params");
        Object limitObj = config.get("limit");
        Integer limit = limitObj == null ? null : Integer.valueOf(String.valueOf(limitObj));

        Object[] params = parseParams(rawParams, input);
        if (limit != null && !sql.toLowerCase(Locale.ROOT).contains("limit")) {
            sql = sql + " LIMIT " + limit;
        }
        log.debug("DbCollector executing sql: {}, params: {}", sql, Arrays.toString(params));
        return jdbcTemplate.query(sql, rs -> {
            List<Map<String, Object>> rows = new ArrayList<>();
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>(cols);
                for (int i = 1; i <= cols; i++) {
                    String col = rs.getMetaData().getColumnLabel(i);
                    row.put(col, rs.getObject(i));
                }
                rows.add(row);
            }
            return rows;
        }, params);
    }

    private Object[] parseParams(Object rawParams, DTSRequest request) {
        if (!(rawParams instanceof List<?> list)) return new Object[0];
        List<Object> out = new ArrayList<>(list.size());
        for (Object item : list) {
            // request.payload 解析，可在此扩展 JSON 解析和 ${} 占位符替换
            out.add(item);
        }
        return out.toArray();
    }

    @Override
    public String name() {
        return "collect:db";
    }
}