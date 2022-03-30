package com.jhr.boot.hbase.starter.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.ConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 利用SPI机制覆盖Phoenix下的ConfigurationFactory类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
public class SecurityConfigurationFactory implements ConfigurationFactory {
    public static final Logger LOG = LoggerFactory.getLogger(SecurityConfigurationFactory.class);

    private static Configuration configuration;

    public static void setConfiguration(Configuration configuration) {
        LOG.debug("设置Phoenix安全配置成功！");
        SecurityConfigurationFactory.configuration = configuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Configuration getConfiguration(Configuration conf) {
        return conf;
    }
}
