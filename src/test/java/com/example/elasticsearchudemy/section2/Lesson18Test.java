package com.example.elasticsearchudemy.section2;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static com.example.elasticsearchudemy.util.TestConstants.GENRE;
import static com.example.elasticsearchudemy.util.TestConstants.IMAX;
import static com.example.elasticsearchudemy.util.TestConstants.MOVIES;
import static com.example.elasticsearchudemy.util.TestConstants.SCI_FI;
import static com.example.elasticsearchudemy.util.TestConstants.TITLE;
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

        IndexRequest insertItemRequest = createRequest(MOVIES, 109487L);

        IndexResponse response = client.index(insertItemRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponse = searchAll(MOVIES);

        assertEquals(1, searchResponse.getHits().getHits().length);

        log.info(searchResponse.toString());

        deleteIfExists(MOVIES);
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
