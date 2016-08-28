package HBaseOperate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

//this class is designed to operate HBase
public class HBaseOperate {
	// HBaseConfiguration
	public static Configuration conf = HBaseConfiguration.create();
	static{
		conf.set("hbase.zookeeper.quorum","192.168.1.101,192.168.1.102,192.168.1.106");
		conf.set("hbase.zookeeper.property.clientPort","2181");
	}

	// HBaseAdmin HTableDescriptor
	@SuppressWarnings("deprecation")
	public static String creat(String tablename, String columnFamily) throws Exception {
		@SuppressWarnings({ "resource" })
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			return "table Exists";
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			//System.out.println("create table success!");
			return "create table success!";
		}
	}
	
	@SuppressWarnings("deprecation")
	public static String drop(String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			return "Delete Table succeed!";
		}
		else
		{
			return "Table doesn't exists!";
		}
	}
}
