package CsvFileImporter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.swt.widgets.Text;

import HBaseOperate.data_importer;
import Tools.qualifier_generator;
import Tools.rowkey_generator;

//this class is designed to import data in CSV file
public class CsvDataImporter {
	
	//define event types
	public static final String[] tables={"wqx","ysp","zbtz","xltz","bdsbxs","sdsbxs","bdqx","sdqx",
			"qx","byqcsbjbfd","byqtxjddl","sbdhwrxcw","emsbdzzbsj"};
	
	//this function is designed to import data in CSV file into HBase
	@SuppressWarnings("resource")
	public void csv_importer(String data_path,String encode_table_path,int event_type,HashMap<String,Integer> codetable,
			String table_name,String columnFamily,Text file_data,Text encode_data) throws IOException
	{
		if(!data_path.isEmpty())
		{
			if(data_path.contains("csv"))
			{
				data_importer di=new data_importer();
				byte[] bytecolumnFamily=Bytes.toBytes(columnFamily);
				try {
					di.creat(table_name, bytecolumnFamily);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				RandomAccessFile raf=new RandomAccessFile(data_path,"rw");
				String str=null;
				int i=0;
				LinkedList<String> temp=new LinkedList<String>();
				
				//读取第一行内容
				String itemline=raf.readLine();
				String[] itemsline=null;
				if(!itemline.isEmpty())
				{
					itemsline=itemline.split(",");
					for(int j=0;j<itemsline.length;j++)
					{
						if(!codetable.containsKey(itemsline[j]))
						{
							System.out.println("数据表表头格式错误！");
							return;
						}
					}
				}
				
				//从第二行开始对数据进行导入
				while((str=raf.readLine())!=null)
				{
					i++;
					temp.add(str);
					if(i>=100)
					{
						i=0;
						for(int j=0;j<temp.size();j++)
						{
							String line=temp.get(j);
							String[] items=line.split(",");
							
							long event_time=System.currentTimeMillis()/1000;
							int[] a=new int[4];
							for(int m=0;m<4;m++)
							{
								Random random=new Random();
								a[m]=random.nextInt(65536);
							}
							rowkey_generator rkg=new rowkey_generator(a[0],a[1],event_type,event_time,a[2],a[3]);
							
							for(int k=0;k<items.length;k++)
							{
								byte[] bytedata=Bytes.toBytes(items[k]);
								int code=codetable.get(itemsline[k]);
								qualifier_generator qg=new qualifier_generator(event_type,code);
								try {
									di.put(table_name, rkg.row_key, bytecolumnFamily, qg.qualifier, bytedata);
									file_data.append(items[k]+"\n");
									encode_data.append(bytedata+"\n");
									
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
						temp=new LinkedList<String>();
					}
				}
				System.out.println("导入完成！");
			}
			else
			{
				file_data.setText("数据文件应为 CSV格式文件!\n");
			}
		}
		else
		{
			file_data.setText("数据文件路径为空!\n");
		}
	}
	
	public int GetEventType(String data_path,Text alert)
	{
		int event_type=0;
		
		for(int i=0;i<tables.length;i++)
		{
			if(data_path.contains(tables[i]))
			{
				event_type=i+1;
				break;
			}
		}
		return event_type;
	}
	
	public HashMap<String,Integer> ReadEncodeTable(String path,Text encode_data)
	{
		RandomAccessFile raf;
		HashMap<String, Integer> encodeTable=new HashMap<String,Integer>();
		if(!path.isEmpty()&&path.contains("csv"))
		{
			if(path.contains("csv"))
			{
				try {
					raf = new RandomAccessFile(path,"rw");
					String str=null;
		
					while((str=raf.readLine())!=null)
					{
						String[] tmp=str.split(",");
						String key=tmp[1];
						int value=Integer.parseInt(tmp[2]);
						encodeTable.put(key, value);
						encode_data.append(key+"\t"+value+"\n");
					}
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				encode_data.setText("映射表应为CSV文件!\n");
			}
		}
		else
		{
			encode_data.setText("映射表文件路径为空!\n");
		}
		
		return encodeTable;
	}
}
