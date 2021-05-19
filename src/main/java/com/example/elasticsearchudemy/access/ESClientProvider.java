package com.example.elasticsearchudemy.access;

import org.elasticsearch.client.RestHighLevelClient;

/**
 * Elasticsearch rest high level client provider
 */
public interface ESClientProvider {
    /**
     *
     * @return Elasticsearch rest high level client
     */
    RestHighLevelClient restHighLevelClient();
}
