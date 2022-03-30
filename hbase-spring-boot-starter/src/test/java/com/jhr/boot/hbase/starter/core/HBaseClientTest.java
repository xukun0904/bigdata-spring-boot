package com.jhr.boot.hbase.starter.core;

import com.jhr.boot.hbase.starter.HBaseAutoConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HBaseAutoConfiguration.class})
public class HBaseClientTest {

    @Autowired
    private HBaseClient hbaseClient;

    private final String tableName = "student";

    private final String familyName = "info";

    @Test
    public void testCreateTable() {
        hbaseClient.createTable(tableName, familyName);
    }

    @Test
    public void testIsTableExist() {
        assert hbaseClient.isTableExist(tableName);
    }

    @Test
    public void testGetAllTableNames() {
        System.out.println(hbaseClient.getAllTableNames());
    }

    @Test
    public void testGetAllFamilyNames() {
        System.out.println(hbaseClient.getAllFamilyNames(tableName));
    }

    @Test
    public void testAddData() {
        hbaseClient.addData(tableName, "012005000201", familyName, "name", "张三");
        hbaseClient.addData(tableName, "012005000201", familyName, "age", "18");
        hbaseClient.addData(tableName, "012005000201", familyName, "gender", "男");

        hbaseClient.addData(tableName, "012005000202", familyName, "name", "李四");
        hbaseClient.addData(tableName, "012005000202", familyName, "age", "19");
        hbaseClient.addData(tableName, "012005000202", familyName, "gender", "女");

        hbaseClient.addData(tableName, "012006000201", familyName, "name", "王五");
        hbaseClient.addData(tableName, "012006000201", familyName, "age", "22");
        hbaseClient.addData(tableName, "012006000201", familyName, "gender", "男");
    }

    @Test
    public void testAddRowData() {
        // service.addRowData(tableName, "112006000201", "address", Arrays.asList("country", "province", "city"), Arrays.asList("中国", "北京", "北京市"));
        hbaseClient.addRowData(tableName, "012006000202", familyName,
                Arrays.asList("name", "age", "gender"), Arrays.asList("赵六3", "23", "女"));
    }

    @Test
    public void testGetAllRows() {
        List<Cell[]> rows = hbaseClient.getAllRows(tableName);
        for (Cell[] row : rows) {
            for (Cell cell : row) {
                System.out.print("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.print("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
                System.out.print("Timestamp:" + cell.getTimestamp());
                System.out.println();
            }
        }
    }

    @Test
    public void testQueryDataRowStartWithPrefix() {
        System.out.println(hbaseClient.queryDataRowStartWithPrefix(tableName, "01200500020"));
    }

    @Test
    public void testQueryDataColumnStartWithPrefix() {
        System.out.println(hbaseClient.queryDataColumnStartWithPrefix(tableName, "ag"));
    }

    @Test
    public void testQueryDataRowContainsKeyword() {
        System.out.println(hbaseClient.queryDataRowContainsKeyword(tableName, "00600"));
    }

    @Test
    public void testQueryDataColumnContainsKeyword() {
        System.out.println(hbaseClient.queryDataColumnContainsKeyword(tableName, "am"));
    }

    @Test
    public void testGetRow() {
        Result result = hbaseClient.getRow(tableName, "012005000202");
        for (Cell cell : result.rawCells()) {
            System.out.print("RowKey:" + Bytes.toString(result.getRow()) + "\t");
            System.out.print("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
            System.out.print("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
            System.out.print("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
            System.out.print("Timestamp:" + cell.getTimestamp());
            System.out.println();
        }
    }

    @Test
    public void testGetRowData() {
        System.out.println(hbaseClient.getRowData(tableName, "012006000202"));
    }

    @Test
    public void testGetRowQualifier() {
        Result result = hbaseClient.getRowQualifier(tableName, "012005000202", familyName, "name");
        for (Cell cell : result.rawCells()) {
            System.out.print("RowKey:" + Bytes.toString(result.getRow()) + "\t");
            System.out.print("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
            System.out.print("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
            System.out.print("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
            System.out.print("Timestamp:" + cell.getTimestamp());
            System.out.println();
        }
    }

    @Test
    public void testDropTable() {
        hbaseClient.dropTable(tableName);
    }

    @Test
    public void testGetAllRowsByFilter() {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("李四"));
        List<Cell[]> rows = hbaseClient.getAllRows(tableName, singleColumnValueFilter);
        for (Cell[] row : rows) {
            for (Cell cell : row) {
                System.out.print("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.print("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
                System.out.print("Timestamp:" + cell.getTimestamp());
                System.out.println();
            }
        }
    }

    @Test
    public void testGetColumnValue() {
        System.out.println(hbaseClient.getColumnValue(tableName, "012006000202", familyName, "name"));
    }

    @Test
    public void testGetColumnValuesByVersion() {
        System.out.println(hbaseClient.getColumnValuesByVersion(tableName, "012006000202", familyName, "name", 3));
    }

    @Test
    public void testGetRows() {
        Result[] rows = hbaseClient.getRows(tableName, Arrays.asList("012005000201", "012006000202"));
        for (Result row : rows) {
            for (Cell cell : row.rawCells()) {
                System.out.print("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.print("Value:" + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
                System.out.print("Timestamp:" + cell.getTimestamp());
                System.out.println();
            }
        }
    }

    @Test
    public void testDeleteColumn() {
        hbaseClient.deleteColumn(tableName, "112006000202", "address", "test1");
    }

    @Test
    public void testDeleteColumnFamily() {
        hbaseClient.deleteColumnFamily(tableName, "address");
    }

    @Test
    public void testDeleteMultiRow() {
        hbaseClient.deleteMultiRow(tableName, "012005000201", "012006000202");
    }

    @Test
    public void testModifyTable() {
        hbaseClient.modifyTable(tableName, "address");
    }

    @Test
    public void testCreateIndex() {
        hbaseClient.createIndex("name_index", tableName, familyName, "name");
    }

    @Test
    public void testDropIndex() {
        hbaseClient.dropIndex(tableName, "name_index");
    }

    @Test
    public void testMultithreading() {
        // 多线程测试
        hbaseClient.closeConnect();
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Assert.assertNotNull(hbaseClient.getConnection());
            }).start();
        }
        while (true) {
        }
    }
}
