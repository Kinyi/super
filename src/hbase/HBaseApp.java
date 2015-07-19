package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseApp {
	//创建表、插入记录、查询一条记录、遍历所有的记录、删除表
	public static final String TABLE_NAME = "table1";
	public static final String FAMILY_NAME = "family1";
	public static final String ROW_KEY = "rowkey1";
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://hadoop0:9000/hbase");
		//使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "hadoop0");
		//创建表、删除表使用    HBaseAdmin
		final HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		createTable(hBaseAdmin);
		
		//插入记录、查询一条记录、遍历所有的记录    HTable
		final HTable hTable = new HTable(conf, TABLE_NAME);
		//插入记录
		//putRecord(hTable);
		//获取一条记录
		//getRecord(hTable);
		//获取所有记录
		//scanTable(hTable);
		hTable.close();
		
		//deleteTable(hBaseAdmin);
	}
	private static void scanTable(final HTable hTable) throws IOException {
		Scan scan = new Scan();
		final ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			final byte[] value = result.getValue(FAMILY_NAME.getBytes(), "age".getBytes());
			System.out.println(result+"\t"+new String(value));
		}
	}
	private static void getRecord(final HTable hTable) throws IOException {
		Get get = new Get(ROW_KEY.getBytes());
		final Result result = hTable.get(get);
		final byte[] value = result.getValue(FAMILY_NAME.getBytes(), "age".getBytes());
		System.out.println(result+"\t"+new String(value));
	}
	private static void putRecord(final HTable hTable) throws IOException {
		Put put = new Put(ROW_KEY.getBytes());
		put.add(FAMILY_NAME.getBytes(), "age".getBytes(), "25".getBytes());
		hTable.put(put);
	}
	private static void deleteTable(final HBaseAdmin hBaseAdmin)
			throws IOException {
		hBaseAdmin.disableTable(TABLE_NAME);
		hBaseAdmin.deleteTable(TABLE_NAME);
	}
	private static void createTable(final HBaseAdmin hBaseAdmin)
			throws IOException {
		if(!hBaseAdmin.tableExists(TABLE_NAME)){
			HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
			tableDescriptor.addFamily(family);
			hBaseAdmin.createTable(tableDescriptor);
		}
	}
}