package com.jhr.boot.elasticsearch;

import com.jhr.boot.elasticsearch.config.ElasticSearchProperties;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.util.Optional;

/**
 * ElasticSearch自动配置类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ElasticSearchProperties.class)
public class ElasticSearchAutoConfiguration {

    @Bean
    public RestHighLevelClient restHighLevelClient(ElasticSearchProperties properties) {
        HwRestClient hwRestClient;
        String confPath = properties.getConfPath();
        // 测试环境
        // confPath = "D:\\Resource\\Data\\conf\\";
        if (Optional.ofNullable(confPath).isPresent()) {
            hwRestClient = new HwRestClient(confPath);
        } else {
            hwRestClient = new HwRestClient();
        }
        return new RestHighLevelClient(hwRestClient.getRestClientBuilder());
    }

    @Bean
    public ElasticsearchRestTemplate elasticsearchTemplate(RestHighLevelClient restHighLevelClient) {
        return new ElasticsearchRestTemplate(restHighLevelClient);
    }
}
