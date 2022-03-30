package com.jhr.boot.elasticsearch.sql.annotation;

/**
 * @author xukun
 * @since 1.0.0.RELEASE
 */
public @interface SqlDocument {

    String indexName();

    String idName() default "id";
}
