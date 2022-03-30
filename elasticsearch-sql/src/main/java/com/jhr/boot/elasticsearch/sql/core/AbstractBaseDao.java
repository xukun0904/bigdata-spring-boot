package com.jhr.boot.elasticsearch.sql.core;

import com.jhr.boot.elasticsearch.sql.annotation.SqlDocument;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.util.ReflectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.text.MessageFormat;
import java.util.*;

/**
 * ES Common Implement Class
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
public abstract class AbstractBaseDao<T, ID> implements BaseDao<T, ID> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractBaseDao.class);

    @Autowired
    protected SearchDao searchDao;

    protected Class<T> entityClass;

    protected Class<T> idClass;

    protected String tableName;

    protected String idName;

    public AbstractBaseDao() {
        ParameterizedType pt = (ParameterizedType) this.getClass().getGenericSuperclass();
        this.entityClass = (Class<T>) pt.getActualTypeArguments()[0];
        this.idClass = (Class<T>) pt.getActualTypeArguments()[1];
        // 获取表名
        SqlDocument document = entityClass.getAnnotation(SqlDocument.class);
        this.tableName = document.indexName();
        this.idName = document.idName();
        // 获取id字段名称
        /*Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Id.class)) {
                this.idName = field.getName();
                break;
            }
        }*/
        LOGGER.debug("BaseDao初始化成功，tableName：{}，idName：{}", tableName, idName);
    }

    @Override
    public List<Map<String, Object>> search(String sql) {
        ObjectResult result = execute(sql);
        if (result != null) {
            List<String> headers = result.getHeaders();
            List<List<Object>> lines = result.getLines();
            List<Map<String, Object>> list = new ArrayList<>(lines.size());
            for (List<Object> line : lines) {
                Map<String, Object> map = new HashMap<>(headers.size());
                for (int i = 0; i < headers.size(); i++) {
                    String fieldName = headers.get(i);
                    Object value = line.get(i);
                    map.put(fieldName, value);
                }
                list.add(map);
            }
            return list;
        }
        return null;
    }

    @Override
    public Optional<T> findById(ID id) {
        // 拼接SQL
        String sql = "SELECT * FROM " + tableName + " WHERE " + idName + " = " + getIdString(id);
        ObjectResult result = execute(sql);
        if (result != null) {
            List<String> headers = result.getHeaders();
            List<List<Object>> lines = result.getLines();
            // 初始化对象
            T entity = BeanUtils.instantiateClass(entityClass);
            for (int i = 0; i < headers.size(); i++) {
                String fieldName = headers.get(i);
                Object value = lines.get(0).get(i);
                // 通过反射设置属性值
                PropertyDescriptor propertyDescriptor = BeanUtils.getPropertyDescriptor(entityClass, fieldName);
                Method writeMethod = propertyDescriptor.getWriteMethod();
                ReflectionUtils.invokeMethod(writeMethod, entity, value);
            }
            return Optional.of(entity);
        }
        return Optional.empty();
    }

    private String getIdString(ID id) {
        if (idClass.equals(String.class)) {
            return "'" + id + "'";
        } else {
            return String.valueOf(id);
        }
    }

    @Override
    public ObjectResult execute(String sql) {
        ObjectResult result = null;
        try {
            LOGGER.debug("执行sql：{}", sql);
            // 1.解释SQL
            QueryAction queryAction = searchDao.explain(sql);
            // 2.执行
            Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
            // 3.格式化查询结果
            result = new ObjectResultsExtractor(false, false, false, queryAction)
                    .extractResults(execution, true);
        } catch (Exception e) {
            LOGGER.error(MessageFormat.format("获取查询结果失败，sql：{0}", sql), e);
        }
        return result;
    }

    @Override
    public Iterable<T> findAll() {
        return findAll(Sort.unsorted());
    }

    @Override
    public Iterable<T> findAll(Sort sort) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(tableName);
        appendSortCondition(sort, sb);
        return findAll(sb.toString());
    }

    private void appendSortCondition(Sort sort, StringBuilder sb) {
        if (sort != null && sort.isSorted()) {
            sb.append(" ORDER BY ");
            for (Sort.Order order : sort) {
                String property = order.getProperty();
                sb.append(property).append(order.isAscending() ? " ASC " : " DESC ").append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
    }

    @Override
    public List<T> findAll(String sql) {
        ObjectResult result = execute(sql);
        if (result != null) {
            List<String> headers = result.getHeaders();
            List<List<Object>> lines = result.getLines();
            List<T> list = new ArrayList<>(lines.size());
            for (List<Object> line : lines) {
                // 初始化对象
                T entity = BeanUtils.instantiateClass(entityClass);
                for (int i = 0; i < headers.size(); i++) {
                    String fieldName = headers.get(i);
                    Object value = line.get(i);
                    // 通过反射设置属性值
                    PropertyDescriptor propertyDescriptor = BeanUtils.getPropertyDescriptor(entityClass, fieldName);
                    Method writeMethod = propertyDescriptor.getWriteMethod();
                    ReflectionUtils.invokeMethod(writeMethod, entity, value);
                }
                list.add(entity);
            }
            return list;
        }
        return null;
    }

    @Override
    public Iterable<T> findAllByIds(Iterable<ID> ids) {
        if (ids != null) {
            StringBuilder sb = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE ")
                    .append(idName).append(" IN ( ");
            for (ID id : ids) {
                sb.append(getIdString(id)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return findAll(sb.toString());
        }
        return null;
    }

    @Override
    public Iterable<T> findAll(Pageable pageable) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(tableName);
        if (pageable != null && pageable.isPaged()) {
            sb.append(" limit ").append(pageable.getOffset()).append(",").append(pageable.getPageSize());
            Sort sort = pageable.getSort();
            appendSortCondition(sort, sb);
        }
        return findAll(sb.toString());
    }

    @Override
    public long count() {
        String alias = "total";
        List<Map<String, Object>> search = search("SELECT COUNT(*) AS " + alias + " FROM " + tableName);
        Double d = (Double) search.get(0).get(alias);
        return d.intValue();
    }

    @Override
    public void deleteAll() {
        execute("DELETE FROM " + tableName);
    }

    @Override
    public void deleteById(ID id) {
        execute("DELETE FROM " + tableName + " WHERE " + idName + " = " + getIdString(id));
    }
}
