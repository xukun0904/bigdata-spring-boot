package com.jhr.boot.hbase.starter.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.hindex.client.HIndexAdmin;
import org.apache.hadoop.hbase.hindex.client.impl.HIndexClient;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification;
import org.apache.hadoop.hbase.hindex.common.TableIndices;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * HBase常用底层操作方法
 *
 * @author xukun
 * @since 1.0.0.RELEASE
 */
public class HBaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    @Autowired
    public Configuration configuration;

    /**
     * 如果频频的创建Configuration实例，会导致创建很多不必要的HConnection实例，很容易达到ZooKeeper的连接数上限
     */
    private volatile Connection connection;

    @PostConstruct
    public void init() {
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            LOG.error("初始化hbase连接失败！", e);
        }
    }

    /**
     * 获得连接，保证connection只有一个
     *
     * @return Connection
     */
    public Connection getConnection() {
        try {
            // 双重检测机制
            if (connection == null || connection.isClosed()) {
                // 同步锁
                synchronized (Connection.class) {
                    if (connection == null || connection.isClosed()) {
                        connection = ConnectionFactory.createConnection(configuration);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("获取hbase连接失败！", e);
        }
        return connection;
    }

    /**
     * 获取Admin,用完需要关闭
     *
     * @return Admin
     * @throws IOException IOException
     */
    private Admin getAdmin() throws IOException {
        Connection conn = getConnection();
        return conn.getAdmin();
    }

    /**
     * 获取table,用完需要关闭
     *
     * @param tableName 表名
     * @return Table
     * @throws IOException IOException
     */
    private Table getTable(String tableName) throws IOException {
        Connection conn = getConnection();
        return conn.getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public void closeConnect() {
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                LOG.error("关闭hbase连接失败！", e);
            }
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return 表是否存在
     */
    public boolean isTableExist(String tableName) {
        boolean result = false;
        try (Admin admin = getAdmin()) {
            result = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.error(MessageFormat.format("判断表是否存在失败，tableName：{0}", tableName), e);
        }
        return result;
    }

    /**
     * 预分区创建表
     *
     * @param tableName   表名
     * @param familyNames 列簇名
     * @param splitKeys   预分期region
     */
    public void createTable(String tableName, byte[][] splitKeys, String... familyNames) {
        List<HColumnDescriptor> columnDescriptors = new ArrayList<>();
        for (String familyName : familyNames) {
            columnDescriptors.add(new HColumnDescriptor(familyName));
        }
        createTable(tableName, splitKeys, columnDescriptors);
    }

    /**
     * 预分区创建表
     *
     * @param tableName         表名
     * @param columnDescriptors 列簇
     * @param splitKeys         预分期region
     */
    public void createTable(String tableName, byte[][] splitKeys, List<HColumnDescriptor> columnDescriptors) {
        // 判断表是否存在
        if (isTableExist(tableName)) {
            LOG.error("表已存在，tableName：{}", tableName);
            return;
        }
        try (Admin admin = getAdmin()) {
            // 创建表属性对象,表名需要转字节
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (HColumnDescriptor columnDescriptor : columnDescriptors) {
                // 设置编码算法，HBase提供了DIFF，FAST_DIFF，PREFIX三种编码算法。
                columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                /*
                  设置文件压缩方式，HBase默认提供了GZ和SNAPPY两种压缩算法
                  其中GZ的压缩率高，但压缩和解压性能低，适用于冷数据
                  SNAPPY压缩率低，但压缩解压性能高，适用于热数据
                  建议默认开启SNAPPY压缩
                 */
                columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                tableDescriptor.addFamily(columnDescriptor);
            }
            // 根据对表的配置，创建表
            admin.createTable(tableDescriptor, splitKeys);
            LOG.debug("表创建成功，tableName：{}", tableName);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("创建表失败，tableName：{0}", tableName), e);
        }
    }

    /**
     * 自定义获取分区splitKeys
     *
     * @param keys 自定义key
     * @return splitKeys
     */
    public byte[][] getSplitKeys(String[] keys) {
        if (keys == null) {
            // 默认为10个分区
            keys = new String[]{"1|", "2|", "3|", "4|",
                    "5|", "6|", "7|", "8|", "9|"};
        }
        byte[][] splitKeys = new byte[keys.length][];
        // 升序排序
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (String key : keys) {
            rows.add(Bytes.toBytes(key));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    /**
     * 获取分区
     *
     * @param startKey   startKey
     * @param endKey     endKey
     * @param numRegions 分区数
     * @return 分区
     */
    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    /**
     * 创建表
     *
     * @param tableName   表名
     * @param familyNames 列族名集合
     */
    public void createTable(String tableName, String... familyNames) {
        createTable(tableName, null, familyNames);
    }

    /**
     * 创建表
     *
     * @param tableName         表名
     * @param columnDescriptors 列族集合
     */
    public void createTable(String tableName, List<HColumnDescriptor> columnDescriptors) {
        createTable(tableName, null, columnDescriptors);
    }

    /**
     * 查询库中所有表的表名
     *
     * @return 表名集合
     */
    public List<String> getAllTableNames() {
        try (Admin admin = getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            return Arrays.stream(tableNames).map(TableName::getNameAsString).collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("查询库中所有表的表名失败！", e);
        }
        return null;
    }

    /**
     * 查询库中表的所有的列簇
     *
     * @param tableName 表名
     * @return 列簇名集合
     */
    public List<String> getAllFamilyNames(String tableName) {
        HColumnDescriptor[] columnFamilies = null;
        List<String> familyNameList = null;
        try (Table table = getTable(tableName)) {
            columnFamilies = table.getTableDescriptor().getColumnFamilies();
        } catch (IOException e) {
            LOG.error(MessageFormat.format("查询库中表的所有的列簇失败，tableName：{0}", tableName), e);
        }
        if (null != columnFamilies && columnFamilies.length > 0) {
            familyNameList = Arrays.stream(columnFamilies).map(HColumnDescriptor::getNameAsString).collect(Collectors.toList());
        }
        return familyNameList;
    }

    /**
     * 新增记录
     * put 't1', '1001', 'cf1:name', 'zhangsan'
     * put 't1', '1001', 'cf1:age', '23'
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 列
     * @param value     值
     */
    public void addData(String tableName, String rowKey, String family, String qualifier, String value) {
        // 向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        // 向Put对象中组装数据
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        List<Put> puts = Collections.singletonList(put);
        puts(tableName, puts);
    }

    /**
     * 插入数据
     *
     * @param tableName 表名
     * @param puts      数据
     */
    private void puts(String tableName, List<Put> puts) {
        try (Table table = getTable(tableName)) {
            // put(final Put put) vs put(final List<Put> puts) 在性能上有明显的弱势
            table.put(puts);
            LOG.debug("插入数据成功，tableName：{}", tableName);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("插入数据失败，tableName：{0}", tableName), e);
        }
    }

    /**
     * 批量新增一行记录
     * columns需要与values一一对应
     *
     * @param tableName  表名
     * @param rowKey     行键
     * @param family     列簇
     * @param qualifiers 多个列
     * @param values     多个值
     */
    public void addRowData(String tableName, String rowKey, String family, List<String>
            qualifiers, List<String> values) {
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < qualifiers.size(); i++) {
            // 向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            // 向Put对象中组装数据
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifiers.get(i)), Bytes.toBytes(values.get(i)));
            puts.add(put);
        }
        puts(tableName, puts);
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     * ResultScanner使用完后关闭
     *
     * @param tableName 表名
     * @param scan      scan
     * @return ResultScanner
     */
    public ResultScanner getResultScanner(String tableName, Scan scan) {
        ResultScanner result = null;
        try (Table table = getTable(tableName)) {
            // setCaching设置的值为每次rpc的请求记录数，默认是1；cache大可以优化性能，但是太大了会花费很长的时间进行一次传输。
            scan.setCaching(1000);
            // 使用HTable得到ResultScanner实现类的对象
            result = table.getScanner(scan);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("获取ResultScanner失败，tableName:{0}", tableName), e);
        }
        return result;
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     * ResultScanner使用完后关闭
     *
     * @param tableName 表名
     * @param startKey  起始Key
     * @param stopKey   stopKey
     * @return ResultScanner
     */
    public ResultScanner getResultScanner(String tableName, String startKey, String stopKey) {
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));
        return getResultScanner(tableName, scan);
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     * ResultScanner使用完后关闭
     *
     * @param tableName 表名
     * @return ResultScanner
     */
    public ResultScanner getResultScanner(String tableName) {
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        return getResultScanner(tableName, scan);
    }

    /**
     * 根据过滤条件全表扫描（Scan）
     * ResultScanner使用完后关闭
     *
     * @param tableName 表名
     * @param filter    过滤条件
     * @return ResultScanner
     */
    public ResultScanner getResultScanner(String tableName, Filter filter) {
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        scan.setFilter(filter);
        return getResultScanner(tableName, scan);
    }

    /**
     * 根据过滤条件全表扫描（Scan）
     * scan "t1"
     *
     * @param tableName 表名
     * @param filter    过滤条件
     * @return Cell[]集合
     */
    public List<Cell[]> getAllRows(String tableName, Filter filter) {
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        scan.setFilter(filter);
        return getAllRows(tableName, scan);
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     *
     * @param tableName 表名
     * @param startKey  起始Key
     * @param stopKey   EndKey
     * @return Cell[]集合
     */
    public List<Cell[]> getAllRows(String tableName, String startKey, String stopKey) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));
        return getAllRows(tableName, scan);
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     *
     * @param tableName 表名
     * @return Cell[]集合
     */
    public List<Cell[]> getAllRows(String tableName) {
        Scan scan = new Scan();
        return getAllRows(tableName, scan);
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     *
     * @param tableName 表名
     * @param scan      scan
     * @return Cell[]集合
     */
    public List<Cell[]> getAllRows(String tableName, Scan scan) {
        List<Cell[]> rows = null;
        ResultScanner resultScanner = getResultScanner(tableName, scan);
        if (resultScanner != null) {
            rows = new ArrayList<>();
            for (Result result : resultScanner) {
                rows.add(result.rawCells());
            }
        }
        return rows;
    }

    /**
     * 全表扫描（Scan）
     * scan "t1"
     *
     * @param tableName 表名
     * @param scan      scan
     * @return data
     */
    public Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
        // <rowKey,对应的行数据>
        Map<String, Map<String, String>> data = new HashMap<>();
        ResultScanner resultScanner = getResultScanner(tableName, scan);
        if (resultScanner != null) {
            for (Result result : resultScanner) {
                // 每一行数据
                Map<String, String> columnMap = new HashMap<>();
                String rowKey = null;
                for (Cell cell : result.rawCells()) {
                    if (rowKey == null) {
                        rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    }
                    columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if (rowKey != null) {
                    data.put(rowKey, columnMap);
                }
            }
        }
        return data;
    }

    /**
     * 通过行前缀过滤器查询数据
     *
     * @param tableName 表名
     * @param prefix    以prefix开始的行键
     * @return data
     */
    public Map<String, Map<String, String>> queryDataRowStartWithPrefix(String tableName, String prefix) {
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
        return this.queryData(tableName, scan);
    }

    /**
     * 通过列前缀过滤器查询数据
     *
     * @param tableName 表名
     * @param prefix    以prefix开始的列名
     * @return data
     */
    public Map<String, Map<String, String>> queryDataColumnStartWithPrefix(String tableName, String prefix) {
        Scan scan = new Scan();
        scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(prefix)));
        return this.queryData(tableName, scan);
    }

    /**
     * 查询行键中包含特定字符的数据
     *
     * @param tableName 表名
     * @param keyword   包含指定关键词的行键
     * @return data
     */
    public Map<String, Map<String, String>> queryDataRowContainsKeyword(String tableName, String keyword) {
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(keyword)));
        return this.queryData(tableName, scan);
    }

    /**
     * 查询列名中包含特定字符的数据
     *
     * @param tableName 表名
     * @param keyword   包含指定关键词的列名
     * @return data
     */
    public Map<String, Map<String, String>> queryDataColumnContainsKeyword(String tableName, String keyword) {
        Scan scan = new Scan();
        scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(keyword)));
        return this.queryData(tableName, scan);
    }

    /**
     * 获取单行记录（get）
     * get "t1","1001"
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return Result
     */
    public Result getRow(String tableName, String rowKey) {
        Result result = null;
        try (Table table = getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            result = table.get(get);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("获取单行记录失败，tableName：{0}，rowKey：{1}", tableName, rowKey), e);
        }
        return result;
    }

    /**
     * 根据tableName和rowKey精确查询一行的数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return data
     */
    public Map<String, String> getRowData(String tableName, String rowKey) {
        // 返回的键值对
        Map<String, String> data = new HashMap<>();
        Result result = getRow(tableName, rowKey);
        if (result != null) {
            for (Cell cell : result.rawCells()) {
                data.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
        return data;
    }

    /**
     * 根据tableName、rowKey、familyName、column查询指定单元格的数据
     *
     * @param tableName  表名
     * @param rowKey     rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     * @return ColumnValue
     */
    public String getColumnValue(String tableName, String rowKey, String familyName, String qualifier) {
        String columnValue = null;
        // 获取表
        try (Table table = getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
                if (cell != null) {
                    columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            LOG.error(MessageFormat.format("查询指定单元格的数据失败，tableName：{0}，rowKey：{1}，familyName：{2}，qualifier：{3}"
                    , tableName, rowKey, familyName, qualifier), e);
        }
        return columnValue;
    }

    /**
     * 根据tableName、rowKey、familyName、column查询指定单元格多个版本的数据
     *
     * @param tableName  表名
     * @param rowKey     rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     * @param versions   需要查询的版本数
     * @return 多个版本数据
     */
    public List<String> getColumnValuesByVersion(String tableName, String rowKey, String familyName,
                                                 String qualifier, int versions) {
        // 返回数据
        List<String> list = new ArrayList<>(versions);
        // 获取表
        try (Table table = getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
            // 读取多少个版本
            get.setMaxVersions(versions);
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                for (Cell cell : result.listCells()) {
                    list.add(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        } catch (IOException e) {
            LOG.error(MessageFormat.format("查询指定单元格多个版本的数据失败，tableName：{0}，rowKey：{1}，familyName：{2}，qualifier：{3}"
                    , tableName, rowKey, familyName, qualifier), e);
        }
        return list;
    }

    /**
     * 获取多行记录（get）
     *
     * @param tableName 表名
     * @param rowKeys   行键
     * @return Result[]
     */
    public Result[] getRows(String tableName, List<String> rowKeys) {
        Result[] results = null;
        try (Table table = getTable(tableName)) {
            List<Get> gets = new ArrayList<>();
            for (String rowKey : rowKeys) {
                gets.add(new Get(Bytes.toBytes(rowKey)));
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            LOG.error(MessageFormat.format("获取多行记录失败，tableName：{0}", tableName), e);
        }
        return results;
    }

    /**
     * 根据限定符获取单行记录（get）
     * get "t1","1001","cf1:name"
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 限定符
     */
    public Result getRowQualifier(String tableName, String rowKey, String family, String qualifier) {
        Result result = null;
        try (Table table = getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            result = table.get(get);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("根据限定符获取单行记录失败，tableName：{0}，rowKey：{1}，family：{2}，qualifier：{3}"
                    , tableName, rowKey, family, qualifier), e);
        }
        return result;
    }

    /**
     * 删除指定的单元格
     *
     * @param tableName  表名
     * @param rowKey     rowKey
     * @param familyName 列簇名
     * @param qualifier  列名
     */
    public void deleteColumn(String tableName, String rowKey, String familyName, String qualifier) {
        try (Table table = getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 设置待删除的列
            delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
            table.delete(delete);
            LOG.debug("删除指定的单元格成功，tableName：{}，rowKey：{}，familyName：{}，qualifier：{}", tableName, rowKey, familyName, qualifier);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("删除指定的单元格失败，tableName：{0}，rowKey：{1}，familyName：{2}，column：{3}"
                    , tableName, rowKey, familyName, qualifier), e);
        }
    }

    /**
     * 删除指定的列簇
     *
     * @param tableName  表名
     * @param familyName 列簇
     */
    public void deleteColumnFamily(String tableName, String familyName) {
        try (Admin admin = getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(familyName));
                LOG.debug("删除指定的列族成功，tableName：{}，familyName：{}", tableName, familyName);
            }
        } catch (IOException e) {
            LOG.error(MessageFormat.format("删除指定的列族失败,tableName：{0},familyName：{1}"
                    , tableName, familyName), e);
        }
    }

    /**
     * 删除多行数据
     *
     * @param tableName 表名
     * @param rowKeys   行键
     */
    public void deleteMultiRow(String tableName, String... rowKeys) {
        try (Table table = getTable(tableName)) {
            List<Delete> deleteList = new ArrayList<>();
            for (String row : rowKeys) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            LOG.debug("删除多行数据成功，tableName：{}，rowKeys：{}", tableName, rowKeys);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("删除多行数据失败，tableName：{0}，rowKeys：{1}", tableName, Arrays.toString(rowKeys)), e);
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void dropTable(String tableName) {
        if (!isTableExist(tableName)) {
            LOG.error("表不存在，tableName：{}", tableName);
            return;
        }
        try (Admin admin = getAdmin()) {
            TableName tableNameValue = TableName.valueOf(tableName);
            admin.disableTable(tableNameValue);
            admin.deleteTable(tableNameValue);
            LOG.debug("删除表成功，tableName：{}", tableName);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("删除表成功失败，tableName：{0}", tableName), e);
        }
    }

    /**
     * 修改表
     *
     * @param tableName            表名
     * @param appendColumnFamilies 列簇名
     */
    public void modifyTable(String tableName, String... appendColumnFamilies) {
        List<HColumnDescriptor> appendColumnDescriptors = new ArrayList<>();
        for (String appendColumnFamily : appendColumnFamilies) {
            appendColumnDescriptors.add(new HColumnDescriptor(appendColumnFamily));
        }
        modifyTable(tableName, appendColumnDescriptors);
    }

    /**
     * 修改表
     *
     * @param tableName               表名
     * @param appendColumnDescriptors 列簇
     */
    public void modifyTable(String tableName, List<HColumnDescriptor> appendColumnDescriptors) {
        try (Admin admin = getAdmin()) {
            TableName tableNameValue = TableName.valueOf(tableName);
            HTableDescriptor htd = admin.getTableDescriptor(tableNameValue);
            for (HColumnDescriptor columnDescriptor : appendColumnDescriptors) {
                if (!htd.hasFamily(columnDescriptor.getName())) {
                    // 设置编码算法，HBase提供了DIFF，FAST_DIFF，PREFIX三种编码算法。
                    columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                    /*
                      设置文件压缩方式，HBase默认提供了GZ和SNAPPY两种压缩算法
                      其中GZ的压缩率高，但压缩和解压性能低，适用于冷数据
                      SNAPPY压缩率低，但压缩解压性能高，适用于热数据
                      建议默认开启SNAPPY压缩
                     */
                    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                    htd.addFamily(columnDescriptor);
                }
            }
            admin.disableTable(tableNameValue);
            admin.modifyTable(tableNameValue, htd);
            admin.enableTable(tableNameValue);
            LOG.debug("修改表成功，tableName：{}", tableName);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("修改表失败，tableName：{0}", tableName), e);
        }
    }

    public HIndexAdmin getIndexAdmin() throws IOException {
        Admin admin = getAdmin();
        return HIndexClient.newHIndexAdmin(admin);
    }

    /**
     * 创建并启用索引
     *
     * @param indexName  索引名
     * @param tableName  表名
     * @param familyName 列族
     * @param names      索引（多个为联合索引）
     */
    public void createIndex(String indexName, String tableName, String familyName, String... names) {
        try (HIndexAdmin indexAdmin = getIndexAdmin()) {
            TableIndices tableIndices = new TableIndices();
            HIndexSpecification indexSpecification = new HIndexSpecification(indexName);
            for (String name : names) {
                indexSpecification.addIndexColumn(new HColumnDescriptor(familyName), name,
                        HIndexSpecification.ValueType.STRING, null);
            }
            tableIndices.addIndex(indexSpecification);
            TableName tableNameValue = TableName.valueOf(tableName);
            indexAdmin.addIndices(tableNameValue, tableIndices);
            indexAdmin.enableIndices(tableNameValue, Collections.singletonList(indexName));
            LOG.debug("创建并启用索引成功，indexName：{}，tableName：{}，familyName：{}，names：{}",
                    indexName, tableName, familyName, names);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("创建并启用索引成功，indexName：{0}，tableName：{1}，familyName：{2}，names：{3}",
                    indexName, tableName, familyName, Arrays.toString(names)), e);
        }
    }

    /**
     * 删除索引
     *
     * @param tableName  表名
     * @param indexNames 索引名
     */
    public void dropIndex(String tableName, String... indexNames) {
        try (HIndexAdmin indexAdmin = getIndexAdmin()) {
            indexAdmin.dropIndices(TableName.valueOf(tableName), Arrays.asList(indexNames));
            LOG.debug("删除索引成功，tableName：{}，indexNames：{}", tableName, indexNames);
        } catch (IOException e) {
            LOG.error(MessageFormat.format("删除索引失败，tableName：{0}，indexNames：{1}", tableName, Arrays.toString(indexNames)), e);
        }
    }
}
