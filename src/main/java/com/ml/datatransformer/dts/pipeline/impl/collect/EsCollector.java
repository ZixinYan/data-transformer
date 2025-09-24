package com.ml.datatransformer.dts.pipeline.impl.collect;


import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.pipeline.model.Collector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;

import java.util.*;

/**
 * EsCollector
 * 使用 Spring Data Elasticsearch 的 ElasticsearchOperations 采集数据。
 *
 * 配置项（config）示例：
 * - index: "card_fee_predict"
 * - query: {"match_all": {}}                 // 这里接受一个 Map 形式的 DSL，再由 NativeQuery 构造
 * - size: 200
 * - page: 0
 * - source_includes: ["field1","field2"]     // 可选
 * - source_excludes: ["big_field"]           // 可选
 *
 * 约定：
 * - 返回 List<Map<String,Object>>，每条为 _source
 */
@Slf4j
@RequiredArgsConstructor
public class EsCollector implements Collector<DTSRequest, List<Map<String, Object>>> {

    private final ElasticsearchOperations esOps;
    private final Map<String, Object> config;

    @Override
    public List<Map<String, Object>> collect(DTSRequest input) {
        String index = Objects.toString(config.getOrDefault("index", ""), "");
        if (index.isEmpty()) {
            log.warn("EsCollector index is empty");
            return Collections.emptyList();
        }
        int size = parseInt(config.get("size"), 200);
        int page = parseInt(config.get("page"), 0);

        NativeQueryBuilder qb = new NativeQueryBuilder();
        qb.withPageable(PageRequest.of(page, size));

        String[] includes = toStringArray(config.get("source_includes"));
        String[] excludes = toStringArray(config.get("source_excludes"));
        if (includes.length > 0 || excludes.length > 0) {
            qb.withSourceFilter(new FetchSourceFilter(true,includes, excludes));
        }

        Object rawQuery = config.get("query");
        if (rawQuery == null) {
            qb.withQuery(q -> q.matchAll(ma -> ma));
        } else {
            // 这里可以根据具体 DSL 需求转换 rawQuery 到 co.elastic.clients.elasticsearch._types.query_dsl.Query
            // 给出一个兜底 match_all
            qb.withQuery(q -> q.matchAll(ma -> ma));
        }

        NativeQuery query = qb.build();
        List<SearchHit<Map>> hits = esOps.search(query, Map.class, IndexCoordinates.of(index)).getSearchHits();
        List<Map<String, Object>> result = new ArrayList<>(hits.size());
        for (SearchHit<Map> h : hits) {
            Map<String, Object> src = new LinkedHashMap<>();
            if (h.getContent() != null) src.putAll(h.getContent());
            result.add(src);
        }
        log.debug("EsCollector fetched {} docs from index {}", result.size(), index);
        return result;
    }

    private int parseInt(Object v, int dft) {
        if (v == null) return dft;
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception e) { return dft; }
    }

    private String[] toStringArray(Object v) {
        if (!(v instanceof List<?> list)) return new String[0];
        return list.stream().map(String::valueOf).toArray(String[]::new);
    }

    @Override
    public String name() {
        return "collect:es";
    }
}