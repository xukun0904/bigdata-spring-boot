package com.jhr.boot.elasticsearch.sql;

import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.jhr.boot.elasticsearch.sql.config.ElasticSearchProperties;
import com.jhr.boot.elasticsearch.sql.config.LoadProperties;
import org.elasticsearch.client.transport.TransportClient;
import org.nlpcn.es4sql.SearchDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * ElasticSearch自动配置类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ElasticSearchProperties.class)
public class ElasticSearchSqlAutoConfiguration {

    public static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSqlAutoConfiguration.class);

    @Bean
    public TransportClient transportClient(ElasticSearchProperties properties) {
        try {
            String confPath = properties.getConfPath();
            // 测试环境
            // confPath = "D:\\Resource\\Data\\conf\\";
            com.huawei.fusioninsight.elasticsearch.transport.common.Configuration configuration = LoadProperties.loadProperties(confPath);
            ClientFactory.initConfiguration(configuration);
        } catch (IOException e) {
            LOGGER.error("初始化transportClient失败！");
        }
        return ClientFactory.getClient();
    }

    @Bean
    public SearchDao searchDao(TransportClient transportClient) {
        return new SearchDao(transportClient);
    }
}
