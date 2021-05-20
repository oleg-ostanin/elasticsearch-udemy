package com.example.elasticsearchudemy.section2;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static com.example.elasticsearchudemy.util.TestConstants.MOVIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class Lesson19Test extends CommonSection2Test {
    private static final String MOVIE_LIST = "lesson_19_movies.txt";

    @BeforeEach
    protected void setUp(){
        super.setUp();
    }

    @Test
    public void bulk() throws Exception {
        createIndex(MOVIES);

        boolean exists = exists(MOVIES);

        assertTrue(exists);

        BulkResponse response = executeBulkRequest(MOVIE_LIST);

        assertFalse(response.hasFailures());

        flush(MOVIES);

        Thread.sleep(1000L);

        SearchResponse searchResponse = searchAll(MOVIES);

        assertEquals(5, searchResponse.getHits().getHits().length);

        log.info(searchResponse.toString());

        deleteIfExists(MOVIES);
    }

    @AfterEach
    protected void tearDown() throws IOException {
        super.tearDown();
    }
}
