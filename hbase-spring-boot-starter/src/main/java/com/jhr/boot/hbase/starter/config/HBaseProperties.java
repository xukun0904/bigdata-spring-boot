package com.jhr.boot.hbase.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * HBase配置类
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
@ConfigurationProperties(prefix = HBaseProperties.HBASE_PREFIX)
public class HBaseProperties {
    public static final String HBASE_PREFIX = "hbase";

    /**
     * 安全登录用户名
     */
    private String username;

    /**
     * 安全登录配置文件位置，默认是部署应用同层目录conf位置
     */
    private String confPath;

    /**
     * windows连接hadoop时会用到，设置临时的hadoop环境变量，之后程序会去这个目录下的\bin目录下找winutils.exe工具
     */
    private String hadoopHome;

    /**
     * 是否开启phoenix，默认不开启，开启后可使用jdbcTemplate操作
     */
    private boolean phoenix;

    public boolean isPhoenix() {
        return phoenix;
    }

    public void setPhoenix(boolean phoenix) {
        this.phoenix = phoenix;
    }

    public String getHadoopHome() {
        return hadoopHome;
    }

    public void setHadoopHome(String hadoopHome) {
        this.hadoopHome = hadoopHome;
    }

    public String getConfPath() {
        return confPath;
    }

    public void setConfPath(String confPath) {
        this.confPath = confPath;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
