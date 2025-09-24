package com.ml.datatransformer.dts.pipeline.impl.publish;

import com.ml.datatransformer.dts.pipeline.model.Publisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * EsPublisher
 * 将一批 Map 文档写入到指定 ES 索引。
 *
 * 配置项（config）示例：
 * - index: "card_fee_aggregation"      // 必填
 * - id_field: "id"                     // 可选，若提供则使用该字段作为文档 _id
 * - create_index_if_missing: true      // 可选，默认 true
 */
@Slf4j
@RequiredArgsConstructor
public class EsPublisher implements Publisher<List<Map<String, Object>>, Boolean> {

    private final ElasticsearchOperations esOps;
    private final Map<String, Object> config;

    @Override
    public Boolean publish(List<Map<String, Object>> result) {
        if (result == null || result.isEmpty()) {
            log.debug("EsPublisher no documents to publish");
            return Boolean.TRUE;
        }
        String index = Objects.toString(config.getOrDefault("index", ""), "");
        if (index.isEmpty()) {
            log.warn("EsPublisher index is empty");
            return Boolean.FALSE;
        }
        String idField = config.get("id_field") == null ? null : String.valueOf(config.get("id_field"));
        boolean createIfMissing = !"false".equalsIgnoreCase(String.valueOf(config.getOrDefault("create_index_if_missing", "true")));

        IndexCoordinates coords = IndexCoordinates.of(index);
        if (createIfMissing) {
            IndexOperations io = esOps.indexOps(coords);
            if (!io.exists()) {
                io.create();
            }
        }

        List<IndexQuery> queries = new ArrayList<>(result.size());
        for (Map<String, Object> docMap : result) {
            Document document = Document.from(docMap);
            IndexQueryBuilder qb = new IndexQueryBuilder().withObject(document);
            if (idField != null && docMap.containsKey(idField)) {
                qb.withId(String.valueOf(docMap.get(idField)));
            }
            queries.add(qb.build());
        }

        try {
            esOps.bulkIndex(queries, coords);
            log.debug("EsPublisher indexed {} documents into index {}", queries.size(), index);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("EsPublisher bulk index error, index={}", index, e);
            return Boolean.FALSE;
        }
    }

    @Override
    public String name() {
        return "publish:es";
    }
}