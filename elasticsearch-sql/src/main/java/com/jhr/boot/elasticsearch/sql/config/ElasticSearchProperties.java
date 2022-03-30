package com.jhr.boot.elasticsearch.sql.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * ElasticSearch配置类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
@ConfigurationProperties(prefix = ElasticSearchProperties.ES_PREFIX)
public class ElasticSearchProperties {
    public static final String ES_PREFIX = "essql";

    /**
     * 安全登录配置文件位置，默认是部署应用同层目录conf位置
     */
    private String confPath;

    public String getConfPath() {
        return confPath;
    }

    public void setConfPath(String confPath) {
        this.confPath = confPath;
    }
}
