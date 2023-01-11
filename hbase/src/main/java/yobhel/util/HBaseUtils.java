package yobhel.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import yobhel.entiy.HBaseColumn;
import yobhel.entiy.HBaseColumnFamily;
import yobhel.entiy.HBaseRow;

import java.io.IOException;
import java.util.*;

/**
 * 类描述：HBase工具类
 *
 * @author yezhimin
 * @date 2023-01-10 17:20
 **/
public class HBaseUtils {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    /**
     *     基础概念
     *     NamespaceDescriptor:维护命名空间的信息,但是namespace，一般用shell来建立
     *     Admin:提供了一个接口来管理 HBase 数据库的表信息
     *     HTableDescriptor:维护了表的名字及其对应表的列族,通过HTableDescriptor对象设置表的特性
     *     HColumnDescriptor:维护着关于列族的信息,可以通过HColumnDescriptor对象设置列族的特性
     */
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判斷表是否存在
     * <br>存在則返回true
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }


    /**
     * 創建表
     * <p>
     * 参数tableName为表的名称，字符串数组fields为存储记录各个域名称的数组。<br>
     * 要求当HBase已经存在名为tableName的表时，先删除原有的表，然后再<br>
     * 创建新的表  field：列族<br>
     *
     * @param tableName 表名
     * @param fields    列族名
     * @throws IOException
     */
    public static void createTable(String tableName, String[] fields) throws IOException {
        if (isTableExist(tableName)) {
            System.out.println(tableName + " table is alreadly exist...");
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName + " table is deleted...");
        }
        TableDescriptorBuilder hTableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String str : fields) {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(str);
            hTableDescriptor.setColumnFamily(cf);
        }
        admin.createTable(hTableDescriptor.build());
        System.out.println(tableName + " table is created!");
        admin.close();
    }

    /**
     * 判断列族是否存在
     *
     * @param tableName
     * @param columnFamily
     * @return
     * @throws IOException
     */
    public static boolean isExistFamily(String tableName, String columnFamily) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            TableDescriptor descriptor = admin.getDescriptor(tablename);
            return descriptor.hasColumnFamily(Bytes.toBytes(columnFamily));
        } else {
            System.out.println("this table: " + tableName + " does not exist");
        }
        return Boolean.FALSE;
    }

    /**
     * 添加列族
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @throws IOException
     */
    public static void addColumnFamily(String tableName, String columnFamily) throws IOException {
        TableName tb = TableName.valueOf(tableName);
        if (admin.tableExists(tb)) {
            admin.disableTable(tb);
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(columnFamily);
            admin.addColumnFamily(tb, cf);
            admin.enableTable(tb);
        } else {
            System.out.println("this table: " + tableName + " does not exist");
        }
    }

    /**
     * 插入记录（单行单列族-多列多值）
     *
     * @param tableName     表名
     * @param row           行名
     * @param columnFamilys 列族名
     * @param columns       列名（数组）
     * @param values        值（数组）（且需要和列一一对应）
     * @throws IOException
     */
    public static void insertRecords(String tableName, String row, String columnFamilys, String[] columns, String[] values) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamilys), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    /**
     * 插入记录（单行单列族-单列单值）
     *
     * @param tableName    表名
     * @param rowName      行名
     * @param columnFamily 列族名
     * @param column       列名
     * @param value        值
     * @throws IOException
     */
    public static void insertOneRecord(String tableName, String rowName, String columnFamily, String column, String value) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Put put = new Put(Bytes.toBytes(rowName));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowName   行名
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowName.getBytes());
        table.delete(d);
    }

    /**
     * 删除单行单列族记录
     *
     * @param tableName    表名
     * @param rowName      行名
     * @param columnFamily 列族名
     * @throws IOException
     */
    public static void deleteColumnFamily(String tableName, String rowName, String columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowName.getBytes()).addFamily(Bytes.toBytes(columnFamily));
        table.delete(d);
    }

    /**
     * 删除单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowName      行名
     * @param columnFamily 列族名
     * @param column       列名
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String rowName, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowName.getBytes()).addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(d);
    }

    /**
     * 根据行获取行中的所有列族
     * @param tableName 表名
     * @param rowName 行名
     * @return
     * @throws IOException
     */
    public static Set getColumnFamilysByRow(String tableName, String rowName) throws IOException {
        Set result = new HashSet();
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Get g = new Get(rowName.getBytes());
        Result rs = table.get(g);
        for (Cell cell : rs.rawCells()) {
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            result.add(family);
        }
        return result;
    }

    /**
     * 获取table的所有rowkey
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Set getRowsByTableName(String tableName) throws IOException {
        Set resultSet = new HashSet();
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                resultSet.add(row);
            }
        }
        return resultSet;
    }

    /**
     * 查找单行单列族单列记录
     * @param tableName 表名
     * @param rowName 行名
     * @param columnFamily 列族名
     * @param column 列名
     * @return
     * @throws IOException
     */
    public static String getColumn(String tableName, String rowName, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Get g = new Get(rowName.getBytes());
        g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result rs = table.get(g);
        return Bytes.toString(rs.value());
    }

    /**
     * 根据 tablename, rowKey ,columnFamily获取记录
     * @param tableName 表名
     * @param rowName 行名
     * @param columnFamily 列族名
     * @return
     * @throws IOException
     */
    public static List<HBaseColumn> getByColumnFamily(String tableName, String rowName, String columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Get g = new Get(rowName.getBytes());
        g.addFamily(Bytes.toBytes(columnFamily));
        Result rs = table.get(g);
        List<HBaseColumn> resultList = new ArrayList<HBaseColumn>();
        for (Cell cell : rs.rawCells()) {
            HBaseColumn hBaseColumn = new HBaseColumn();
            String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            hBaseColumn.setTableName(tableName);
            hBaseColumn.setRowName(rowName);
            hBaseColumn.setColumnFamilyName(columnFamily);
            hBaseColumn.setColumnName(colName);
            hBaseColumn.setValue(value);
            resultList.add(hBaseColumn);
        }
        return resultList;
    }

    /**
     * 查找一行记录
     * @param tableName 表名
     * @param rowName 行名
     * @return
     * @throws IOException
     */
    public static List<HBaseColumnFamily> getByRow(String tableName, String rowName) throws IOException {
        List<HBaseColumnFamily> list = new ArrayList<HBaseColumnFamily>();
        Set columnFamilys = getColumnFamilysByRow(tableName, rowName);
        Iterator iterator = columnFamilys.iterator();
        if (iterator != null) {
            //遍历列族
            while (iterator.hasNext()) {
                //获取列族名
                String columnFamilyName = iterator.next().toString();
                HBaseColumnFamily hBaseColumnFamily = new HBaseColumnFamily();
                hBaseColumnFamily.setTableName(tableName);
                hBaseColumnFamily.setRowName(rowName);
                hBaseColumnFamily.setColumnFamilyName(columnFamilyName);
                //获取列族里的所有列
                List<HBaseColumn> columns = getByColumnFamily(tableName, rowName, columnFamilyName);
                hBaseColumnFamily.setHBaseColumns(columns);
                list.add(hBaseColumnFamily);
            }
        }
        return list;
    }

    /**
     * 根据表名获取记录
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<HBaseRow> getByTable(String tableName) throws IOException {
        List<HBaseRow> result = new ArrayList<HBaseRow>();
        //获取表中所有的行row
        Set rowSet = getRowsByTableName(tableName);
        Iterator iterator = rowSet.iterator();
        if (iterator != null) {
            //遍历行
            while (iterator.hasNext()) {
                //获取行名
                String rowName = iterator.next().toString();
                HBaseRow hBaseRow = new HBaseRow();
                hBaseRow.setTableName(tableName);
                hBaseRow.setRowName(rowName);
                List<HBaseColumnFamily> familyList = new ArrayList<HBaseColumnFamily>();
                familyList = getByRow(tableName, rowName);
                hBaseRow.setHBaseColumnFamilies(familyList);
                result.add(hBaseRow);
            }
        }
        return result;
    }

    /**
     * 查询表中所有行（Scan方式）
     *
     * @param tablename
     * @return String
     */
    public static String scanAllRecord(String tablename) throws IOException {
        String record = "";
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                    StringBuffer stringBuffer = new StringBuffer().append(row).append("\t")
                            .append(family).append("\t")
                            .append(colName).append("\t")
                            .append(value).append("\n");
                    String str = stringBuffer.toString();
                    record += str;
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return record;
    }

    /**
     * 根据rowkey关键字查询报告记录
     *
     * @param tablename
     * @param rowKeyword
     * @return
     */
    public static List scanReportDataByRowKeyword(String tablename, String rowKeyword) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        //添加行键过滤器，根据关键字匹配
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKeyword));
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyword));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                //TODO 此处根据业务来自定义实现
                list.add(null);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return list;
    }


    /**
     * 根据rowkey关键字和时间戳范围查询报告记录
     *
     * @param tablename
     * @param rowKeyword
     * @return
     */
    public static List scanReportDataByRowKeywordTimestamp(String tablename, String rowKeyword, Long minStamp, Long maxStamp) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        //添加scan的时间范围
        scan.setTimeRange(minStamp, maxStamp);
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyword));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                //TODO 此处根据业务来自定义实现
                list.add(null);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }

    /**
     * 删除表操作
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }


    public static void main(String[] args) throws Exception {
        // 创建表
//        HBaseUtils.createTable("user", new String[]{"base_info","other_info"});
        // 判斷表是否存在
//        System.out.println(HBaseUtils.isTableExist("user"));
        // 判断列族是否存在
//        System.out.println(HBaseUtils.isExistFamily("user", "base_info"));
//        System.out.println(HBaseUtils.isExistFamily("user", "base_info33333"));
        // 添加列族
//        HBaseUtils.addColumnFamily("user", "base_info333");
        // 插入记录（单行单列族-多列多值）
//        HBaseUtils.insertRecords("user", "1", "base_info", new String[]{"name", "age"}, new String[]{"张三", "3"});
        // 插入记录（单行单列族-单列单值）
//        HBaseUtils.insertOneRecord("user", "1", "other_info", "sex", "男");
        // 删除一行记录
//        HBaseUtils.deleteRow("user", "1");
        // 删除单行单列族记录
//        HBaseUtils.deleteColumnFamily("user","1","base_info");
        // 删除单行单列族单列记录
//        HBaseUtils.deleteColumn("user", "1", "other_info", "sex");
        // 根据行获取行中的所有列族
//        System.out.println(HBaseUtils.getColumnFamilysByRow("user","1"));
        // 获取table的所有rowkey
//        System.out.println(HBaseUtils.getRowsByTableName("user"));
        // 查找单行单列族单列记录
//        System.out.println(HBaseUtils.getColumn("user","1","base_info","name"));
        // 根据 tablename, rowKey ,columnFamily获取记录
//        System.out.println(HBaseUtils.getByColumnFamily("user","1","base_info"));
        // 查找一行记录
//        System.out.println(HBaseUtils.getByRow("user","1"));
        // 根据表名获取记录
//        System.out.println(HBaseUtils.getByTable("user"));
        // 查询表中所有行（Scan方式）
//        System.out.println(HBaseUtils.scanAllRecord("user"));
        // 删除表操作
//        HBaseUtils.deleteTable("user");

    }

}
