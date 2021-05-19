package com.example.elasticsearchudemy;

import com.example.elasticsearchudemy.access.ESClientProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

import static com.example.elasticsearchudemy.util.TestConstants.DATE;
import static com.example.elasticsearchudemy.util.TestConstants.MOVIES;
import static com.example.elasticsearchudemy.util.TestConstants.PROPERTIES;
import static com.example.elasticsearchudemy.util.TestConstants.TITLE;
import static com.example.elasticsearchudemy.util.TestConstants.TYPE;
import static com.example.elasticsearchudemy.util.TestConstants.YEAR;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class CommonTest {
    @Autowired
    private ESClientProvider provider;

    protected RestHighLevelClient client;

    protected ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    protected void setUp(){
        client = provider.restHighLevelClient();
    }

    protected void deleteIfExists(String index) throws Exception {
        if (exists(index)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);

            // Delete index if exists
            try {
                client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected boolean exists(String index) throws Exception {
        GetIndexRequest request = new GetIndexRequest(index);

        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);

        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    protected void mapping(String index) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();

        request.indices(index);

        GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);

        Map<String, MappingMetadata> mapping = getMappingResponse.mappings();

        log.info("Mapping:");

        for (String key : mapping.keySet()) {
            MappingMetadata metadata = mapping.get(key);

            log.info(key + " - " + metadata.source().string());
        }
    }

    protected void settings(String index) throws IOException {
        GetSettingsRequest request = new GetSettingsRequest().indices(index);

        request.includeDefaults(true);

        GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);

        Settings indexSettings = response.getIndexToSettings().get(index);

        log.info("Index settings:");

        for (String key : indexSettings.keySet()) {
            log.info(String.format("%s = %s", key, indexSettings.get(key)));
        }
    }

    protected SearchResponse searchAll(String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        printHits(searchResponse);

        return searchResponse;
    }

    protected SearchResponse search(String index, String field, String query) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(field, query);

        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        printHits(searchResponse);

        return searchResponse;
    }



    protected void flush(String index) throws Exception {
        FlushRequest request = new FlushRequest(index);

        client.indices().flush(request, RequestOptions.DEFAULT);
    }

    private void printHits(SearchResponse searchResponse) {
        SearchHits hits = searchResponse.getHits();

        log.info("Search results:");
        log.info("");

        for (SearchHit hit : hits) {
            Map<String, Object> map = hit.getSourceAsMap();

            log.info("=================================================");

            map.entrySet().stream()
                .forEach((entry) -> log.info(String.format("%s = %s", entry.getKey(), entry.getValue())));

            log.info("=================================================");
            log.info("");
        }
    }

    protected void tearDown() throws IOException {
        client.close();
    }
}
