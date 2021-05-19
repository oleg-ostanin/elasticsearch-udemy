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
public class Lesson20Test extends CommonSection2Test {
    private static final String INTERSTELLAR_FOO = "InterstellarFoo";
    private static final String INTERSTELLAR = "Interstellar";


    @BeforeEach
    protected void setUp(){
        super.setUp();
    }

    @Test
    public void createInsertUpdateCorrectDelete() throws Exception {
        createIndex(MOVIES);

        boolean exists = exists(MOVIES);

        assertTrue(exists);

        IndexRequest insertItemRequest = createRequest(MOVIES, 109487L, INTERSTELLAR);

        IndexResponse response = client.index(insertItemRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponse = searchAll(MOVIES);

        assertEquals(searchResponse.getHits().getHits().length, 1);

        log.info(searchResponse.toString());

        IndexRequest updateItemRequest = createRequest(MOVIES, 109487L, INTERSTELLAR_FOO);

        IndexResponse updateResponse = client.index(updateItemRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponseAfterUpdate = searchAll(MOVIES);

        assertEquals(searchResponseAfterUpdate.getHits().getHits().length, 1);

        String title = searchResponseAfterUpdate.getHits().getAt(0).getSourceAsMap().get(TITLE).toString();

        assertEquals(INTERSTELLAR_FOO, title);

        log.info(searchResponseAfterUpdate.toString());

        IndexRequest correctTitleRequest = createRequest(MOVIES, 109487L, INTERSTELLAR);

        IndexResponse correctResponse = client.index(correctTitleRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.UPDATED, correctResponse.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponseAfterCorrect = searchAll(MOVIES);

        assertEquals(searchResponseAfterCorrect.getHits().getHits().length, 1);

        String titleCorrected = searchResponseAfterCorrect.getHits().getAt(0).getSourceAsMap().get(TITLE).toString();

        assertEquals(INTERSTELLAR, titleCorrected);

        log.info(searchResponseAfterCorrect.toString());

        deleteIfExists(MOVIES);
    }

    private IndexRequest createRequest(String index, Long id, String title) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.array(GENRE, IMAX, SCI_FI);
        builder.field(TITLE, title);
        builder.field(YEAR, 2014);

        builder.endObject();

        return new IndexRequest(index)
            .id(String.valueOf(id))
            .source(builder);
    }

    private IndexRequest createRequestWithTitleOnly(String index, Long id, String title) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.field(TITLE, title);

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
