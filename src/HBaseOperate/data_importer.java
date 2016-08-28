package HBaseOperate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class data_importer {
	// HBaseConfiguration
	public static Configuration conf = HBaseConfiguration.create();
	static{
		conf.set("hbase.zookeeper.quorum","192.168.1.101,192.168.1.102,192.168.1.106");
		conf.set("hbase.zookeeper.property.clientPort","2181");
	}
	
	
	@SuppressWarnings({ "resource", "deprecation" })
	public void creat(String tablename, byte[] columnFamily) throws Exception 
	{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			return;
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}
	
	// 添加一条数据，通过HTable Put为已经存在的表来添加数据
	@SuppressWarnings("deprecation")
	public void put(String tablename, byte[] rowkey, byte[] columnFamily, byte[] column, byte[] data)throws Exception 
	{
		HTable table = new HTable(conf, tablename);
		Put p1 = new Put(rowkey);
		p1.add(columnFamily, column, data);
		table.put(p1);
		//System.out.println("put ok!");
		table.close();
	}

	// get a record of data from table
	@SuppressWarnings("deprecation")
	public void get(String tablename, String row) throws IOException
	{
		HTable table = new HTable(conf, tablename);
		Get g = new Get(Bytes.toBytes(row));
		Result result = table.get(g);
		System.out.println("Get: " + result);
		table.close();
	}

	// 显示所有数据，通过HTable Scan来获取已有表的信息
	@SuppressWarnings({ "deprecation", "resource" })
	public void scan(String tablename) throws Exception 
	{
		HTable table = new HTable(conf, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			System.out.println("Scan: " + r);
		}
		table.close();
	}
}
