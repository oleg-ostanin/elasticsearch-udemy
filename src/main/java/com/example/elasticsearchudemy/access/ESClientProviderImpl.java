package com.example.elasticsearchudemy.access;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

/**
 * Elasticsearch rest high level client provider implementation
 */
@Component
public class ESClientProviderImpl implements ESClientProvider, BeanPostProcessor, DisposableBean {
    private static final String LOCALHOST = "localhost";
    private static final String HTTP = "http";
    private static final int PORT_9200 = 9200;
    private static final int PORT_9201 = 9201;

    private RestHighLevelClient restHighLevelClient;

    /**
     * {@inheritDoc}
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(LOCALHOST, PORT_9200, HTTP),
                        new HttpHost(LOCALHOST, PORT_9201, HTTP)));

        return bean;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RestHighLevelClient restHighLevelClient() {
        return restHighLevelClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws Exception {
        restHighLevelClient.close();
    }
}
