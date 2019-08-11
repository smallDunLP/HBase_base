package basedemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.util.List;

public class HBaseDemo {

    private Configuration conf = null;
    private Connection conn = null;
    private Admin admin = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "t01,t02,t03,t04,t05");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 测试用得方法
     *
     * @throws IOException 抛出得异常
     */
    @Test
    public void baseOptions() throws IOException {
        createTable("testeve","info");
    }

    /**
     * 创建表
     * @param tableName  表名
     * @param columnFamily  列簇名
     * @throws IOException  抛出得异常
     */
    public void createTable(String tableName,String columnFamily) throws IOException {
        if(judgeTableExists(tableName)){
            return;
        }else{
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            ColumnFamilyDescriptor cfd = cfdb.build();
            tdb.setColumnFamily(cfd);
            TableDescriptor td = tdb.build();
            admin.createTable(td);
        }

    }

    /**
     * get得到数据
     *
     * @param tableName 表名
     * @param rowkey    rowkey
     * @throws IOException 抛出得异常
     */
    public void get(String tableName, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);

        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }


    /**
     * 插入数据
     *
     * @param tableName     表名
     * @param rowkey        rowkey
     * @param coulumnFamily 列簇名
     * @param qualifier     列名
     * @param value         值
     * @throws IOException 抛出得异常
     */
    public void insetData(String tableName, String rowkey,
                          String coulumnFamily, String qualifier, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes(coulumnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }


    /**
     * scan得到数据
     *
     * @param tableName        表名
     * @param columnFamilyName 列簇名
     * @throws IOException 异常抛出
     */
    public void scanData(String tableName, String columnFamilyName) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamilyName));

        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result res : resultScanner) {
            List<Cell> cells = res.listCells();
            for (Cell cell : cells) {
                byte[] rowBytes = CellUtil.cloneRow(cell);
                byte[] familyBytes = CellUtil.cloneFamily(cell);
                byte[] qualifireBytes = CellUtil.cloneQualifier(cell);
                byte[] valueBytes = CellUtil.cloneValue(cell);
                System.out.println("rowkey: " + Bytes.toString(rowBytes)
                        + "\t" + "ColumnFimaly :" + Bytes.toString(familyBytes) + ":" + Bytes.toString(qualifireBytes)
                        + "\t" + "Value :" + Bytes.toString(valueBytes)
                );
            }
        }
        table.close();
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return 返回表是否存在
     */
    public boolean judgeTableExists(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    @After
    public void destory() {
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
