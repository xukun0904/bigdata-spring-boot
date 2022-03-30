package com.jhr.boot.elasticsearch.sql.core;

import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ES Simple ORM Interface
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
public interface BaseDao<T, ID> {

    /**
     * 根据SQL查询
     *
     * @param sql sql
     * @return 集合
     */
    List<Map<String, Object>> search(String sql);

    /**
     * 根据主键查询记录
     *
     * @param id 主键
     * @return entity
     */
    Optional<T> findById(ID id);

    /**
     * 执行SQL
     *
     * @param sql sql
     * @return result
     */
    ObjectResult execute(String sql);

    /**
     * 查询所有记录
     *
     * @return entities
     */
    Iterable<T> findAll();

    /**
     * 查询所有记录并排序
     *
     * @param sort 排序规则
     * @return entities
     */
    Iterable<T> findAll(Sort sort);

    /**
     * 根据sql返回封装后的查询结果
     *
     * @param sql sql
     * @return list
     */
    List<T> findAll(String sql);

    /**
     * 根据主键集合查询
     *
     * @param ids 主键集合
     * @return list
     */
    Iterable<T> findAllByIds(Iterable<ID> ids);

    /**
     * 分页查询
     *
     * @param pageable 分页参数
     * @return list
     */
    Iterable<T> findAll(Pageable pageable);

    /**
     * 所有条数
     *
     * @return total
     */
    long count();

    /**
     * 删除表中所有记录
     */
    void deleteAll();

    /**
     * 根据主键删除
     *
     * @param id 主键
     */
    void deleteById(ID id);
}
