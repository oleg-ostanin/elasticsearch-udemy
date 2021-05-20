package com.example.elasticsearchudemy.section2;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
public class Lesson20WithUpdateRequestsTest extends CommonSection2Test {
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

        Thread.sleep(1000L);

        SearchResponse searchResponse = searchAll(MOVIES);

        assertEquals(1, searchResponse.getHits().getHits().length);

        log.info(searchResponse.toString());

        UpdateRequest updateItemRequest = updateRequest(MOVIES, 109487L, INTERSTELLAR_FOO);

        UpdateResponse updateResponse = client.update(updateItemRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

        flush(MOVIES);

        Thread.sleep(1000L);

        SearchResponse searchResponseAfterUpdate = searchAll(MOVIES);

        assertEquals(1, searchResponseAfterUpdate.getHits().getHits().length);

        String title = searchResponseAfterUpdate.getHits().getAt(0).getSourceAsMap().get(TITLE).toString();

        assertEquals(INTERSTELLAR_FOO, title);

        log.info(searchResponseAfterUpdate.toString());

        UpdateRequest correctTitleRequest = updateRequestWithTitleOnly(MOVIES, 109487L, INTERSTELLAR);

        UpdateResponse correctResponse = client.update(correctTitleRequest, RequestOptions.DEFAULT);

        assertEquals(DocWriteResponse.Result.UPDATED, correctResponse.getResult());

        flush(MOVIES);

        Thread.sleep(1000L);

        SearchResponse searchResponseAfterCorrect = searchAll(MOVIES);

        assertEquals(1, searchResponseAfterCorrect.getHits().getHits().length);

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

    private UpdateRequest updateRequest(String index, Long id, String title) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.array(GENRE, IMAX, SCI_FI);
        builder.field(TITLE, title);
        builder.field(YEAR, 2014);

        builder.endObject();

        return new UpdateRequest(index, String.valueOf(id)).doc(builder);
    }

    private UpdateRequest updateRequestWithTitleOnly(String index, Long id, String title) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.field(TITLE, title);

        builder.endObject();

        return new UpdateRequest(index, String.valueOf(id)).doc(builder);
    }

    @AfterEach
    protected void tearDown() throws IOException {
        super.tearDown();
    }
}
