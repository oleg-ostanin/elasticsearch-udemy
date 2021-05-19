package com.example.elasticsearchudemy.section2;

import com.example.elasticsearchudemy.util.CreateInfo;
import com.example.elasticsearchudemy.util.IndexInfo;
import com.example.elasticsearchudemy.util.MovieInfo;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ClassLoaderUtils;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.example.elasticsearchudemy.util.TestConstants.GENRE;
import static com.example.elasticsearchudemy.util.TestConstants.MOVIES;
import static com.example.elasticsearchudemy.util.TestConstants.TITLE;
import static com.example.elasticsearchudemy.util.TestConstants.YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class Lesson21Test extends CommonSection2Test {
    private static final String MOVIE_LIST = "lesson_19_movies.txt";

    @BeforeEach
    protected void setUp(){
        super.setUp();
    }

    @Test
    public void insertBulkDelete() throws Exception {
        createIndex(MOVIES);

        boolean exists = exists(MOVIES);

        assertTrue(exists);

        BulkResponse response = executeBulkRequest(MOVIE_LIST);

        assertFalse(response.hasFailures());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponse = searchAll(MOVIES);

        assertEquals(searchResponse.getHits().getHits().length, 5);

        log.info(searchResponse.toString());

        SearchResponse searchForDarkResponse = search(MOVIES, TITLE, "Dark");

        //todo get using title
        String darkKnightId = searchForDarkResponse.getHits().getAt(0).getId();

        DeleteResponse deleteResponse = executeDelete(MOVIES, darkKnightId);

        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        flush(MOVIES);

        //we need this because otherwise search will not return any hits
        Thread.sleep(1000L);

        SearchResponse searchResponseAfterDelete = searchAll(MOVIES);

        assertEquals(searchResponseAfterDelete.getHits().getHits().length, 4);

        log.info(searchResponse.toString());

        deleteIfExists(MOVIES);
    }

    private DeleteResponse executeDelete(String index, String id) throws Exception {
        DeleteRequest request = new DeleteRequest(index).id(id);

        return client.delete(request, RequestOptions.DEFAULT);
    }

    @AfterEach
    protected void tearDown() throws IOException {
        super.tearDown();
    }
}
