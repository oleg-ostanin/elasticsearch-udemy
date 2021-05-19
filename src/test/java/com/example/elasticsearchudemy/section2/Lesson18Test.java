package com.example.elasticsearchudemy.section2;

import com.example.elasticsearchudemy.CommonTest;
import com.example.elasticsearchudemy.access.ESClientProvider;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
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
import static com.example.elasticsearchudemy.util.TestConstants.GENRE;
import static com.example.elasticsearchudemy.util.TestConstants.IMAX;
import static com.example.elasticsearchudemy.util.TestConstants.MOVIES;
import static com.example.elasticsearchudemy.util.TestConstants.PROPERTIES;
import static com.example.elasticsearchudemy.util.TestConstants.SCI_FI;
import static com.example.elasticsearchudemy.util.TestConstants.TITLE;
import static com.example.elasticsearchudemy.util.TestConstants.TYPE;
import static com.example.elasticsearchudemy.util.TestConstants.YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class Lesson18Test extends CommonSection2Test {


    @BeforeEach
    protected void setUp(){
        super.setUp();
    }

    @Test
    public void createInsertDelete() throws Exception {
        createIndex(MOVIES);

        boolean exists = exists(MOVIES);

        assertTrue(exists);

        createRequest(MOVIES, 109487L);

        IndexRequest insertItemRequest = createRequest(MOVIES, 109487L);

        IndexResponse response = client.index(insertItemRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponse = search(MOVIES);

        assertEquals(searchResponse.getHits().getHits().length, 1);

        log.info(searchResponse.toString());

        deleteIfExists(MOVIES);
    }

    protected void createIndex(String index) throws Exception {
        deleteIfExists(index);

        CreateIndexRequest request = new CreateIndexRequest(index);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(YEAR);
                {
                    builder.field(TYPE, DATE);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        request.mapping(builder);

        client.indices().create(request, RequestOptions.DEFAULT);

        mapping(index);

        settings(index);
    }

    private IndexRequest createRequest(String index, Long id) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.array(GENRE, IMAX, SCI_FI);
        builder.field(TITLE, "Interstellar");
        builder.field(YEAR, 2014);

        builder.endObject();

        return new IndexRequest(index)
            .id(String.valueOf(id))
            .source(builder);
    }

    @AfterEach
    protected void tearDown() throws IOException {
        super.tearDown();
    }
}
