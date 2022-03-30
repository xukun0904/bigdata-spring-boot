package com.jhr.boot.hbase.starter;

import com.jhr.boot.hbase.starter.config.HBaseProperties;
import com.jhr.boot.hbase.starter.config.SecurityConfigurationFactory;
import com.jhr.boot.hbase.starter.core.HBaseClient;
import com.jhr.boot.hbase.starter.security.LoginUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;

/**
 * HBase自动配置类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(HBaseProperties.class)
@ConditionalOnProperty(prefix = HBaseProperties.HBASE_PREFIX, name = "username")
public class HBaseAutoConfiguration {
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    public static final Logger LOG = LoggerFactory.getLogger(HBaseAutoConfiguration.class);
    public static final String CONF = "conf";
    private static final String HBASE_CONFIGURATION_BEAN_NAME = "hbaseConfiguration";

    @Bean(HBASE_CONFIGURATION_BEAN_NAME)
    public org.apache.hadoop.conf.Configuration configuration(HBaseProperties hBaseProperties) {
        String username = hBaseProperties.getUsername();
        String hadoopHome = hBaseProperties.getHadoopHome();
        String path = Optional.ofNullable(hBaseProperties.getConfPath()).orElseGet(() -> {
            // 获取程序同层目录下
            return System.getProperty("user.dir") + File.separator + CONF;
        });
        // 测试路径
        /*username = "hivehdfs";
        path = "D:\\Resource\\Data\\conf";
        hadoopHome = "D:\\Software\\hadoop_2_8_1";*/
        try {
            LOG.debug("安全登录用户名：{}", username);
            LOG.debug("加载配置目录：{}", path);
            // 设置临时的hadoop环境变量，之后程序会去这个目录下的\bin目录下找winutils.exe工具，windows连接hadoop时会用到
            Optional.ofNullable(hadoopHome).ifPresent(home -> {
                LOG.debug("hadoop环境目录：{}", home);
                System.setProperty("hadoop.home.dir", home);
            });
            // 执行此步时，会去resources目录下找相应的配置文件，例如hbase-site.xml
            org.apache.hadoop.conf.Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
            init(conf, path);
            LOG.debug("hbase初始化成功！");
            login(conf, path, username);
            LOG.debug("hbase登录成功！");
            // 设置phoenix配置类，使用同一个配置类，解决重复创建，创建不正确的问题
            SecurityConfigurationFactory.setConfiguration(conf);
            return conf;
        } catch (IOException e) {
            LOG.error(MessageFormat.format("创建Configuration失败，username：{0}，hadoopHome：{1}，path：{2}",
                    username, hadoopHome, path), e);
        }
        return null;
    }

    @Bean
    public HBaseClient hbaseClient() {
        return new HBaseClient();
    }

    @Bean("phoenixDataSource")
    @ConditionalOnProperty(prefix = HBaseProperties.HBASE_PREFIX, name = "phoenix", havingValue = "true")
    public DataSource phoenixDataSource(@Qualifier(HBASE_CONFIGURATION_BEAN_NAME) org.apache.hadoop.conf.Configuration configuration) {
        HikariDataSource dataSource = new HikariDataSource();
        String jdbcUrl = "jdbc:phoenix:" + configuration.get("hbase.zookeeper.quorum");
        LOG.debug("jdbcUrl：{}", jdbcUrl);
        dataSource.setJdbcUrl(jdbcUrl);
        return dataSource;
    }

    private void login(org.apache.hadoop.conf.Configuration conf, String path, String username) throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userKeytabFile = path + File.separator + "user.keytab";
            String krb5File = path + File.separator + "krb5.conf";
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, username, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                    ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(username, userKeytabFile, krb5File, conf);
        }
    }

    private void init(org.apache.hadoop.conf.Configuration conf, String path) {
        // 使用HBaseConfiguration的单例方法实例化
        conf.addResource(new Path(path + File.separator + "core-site.xml"), false);
        conf.addResource(new Path(path + File.separator + "hdfs-site.xml"), false);
        conf.addResource(new Path(path + File.separator + "hbase-site.xml"), false);
    }
}
