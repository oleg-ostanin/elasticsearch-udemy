package com.example.elasticsearchudemy.section2;

import com.example.elasticsearchudemy.CommonTest;
import com.example.elasticsearchudemy.util.CreateInfo;
import com.example.elasticsearchudemy.util.IndexInfo;
import com.example.elasticsearchudemy.util.MovieInfo;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
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
public class CommonSection2Test extends CommonTest {

    protected void setUp(){
        super.setUp();
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

    protected BulkResponse executeBulkRequest(String resource) throws Exception {
        URL url = ClassLoaderUtils.getDefaultClassLoader().getResource(resource);
        Path path = Paths.get(url.toURI());
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

        int size = lines.size() / 2;

        BulkRequest request = new BulkRequest();

        for(int i = 0; i < size; i++) {
            String first = lines.get(i * 2);
            String second = lines.get(i * 2 + 1);

            CreateInfo createInfo = mapper.readValue(first, CreateInfo.class);
            MovieInfo movieInfo = mapper.readValue(second, MovieInfo.class);

            request.add(indexRequest(createInfo.getCreate(), movieInfo));
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        return bulkResponse;
    }

    private IndexRequest indexRequest(IndexInfo indexInfo, MovieInfo movieInfo) throws Exception {
        //Can't set .type() because of failures. .type(String type) is deprecated anyway.
//        IndexRequest request = new IndexRequest(indexInfo.getIndex())
//            .id(indexInfo.getId()).type(indexInfo.getType());


        IndexRequest request = new IndexRequest(indexInfo.getIndex())
            .id(indexInfo.getId());

        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        builder.array(GENRE, movieInfo.getGenre());
        builder.field(TITLE, movieInfo.getTitle());
        builder.field(YEAR, movieInfo.getYear());

        builder.endObject();

        request.source(builder);

        return request;
    }

    protected void tearDown() throws IOException {
        super.tearDown();
    }
}
