package OracleConnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Oracle_Connector {

	public String url,db_name,tb_name,user,pwd;
	
	public Oracle_Connector(String ur,String db,String tb,String us,String p)
	{
		url=ur;
		db_name=db;
		tb_name=tb;
		user=us;
		pwd=p;
	}
	
	public HashMap<Integer,HashMap<String,String>> Connect() throws SQLException
	{
		//<row_id,<key,value>>pairs
		HashMap<Integer,HashMap<String,String>> results=new HashMap<Integer,HashMap<String,String>>();
		String dburl="jdbc:oracle:thin:@"+url+":1521:"+db_name;
		String sql="select * from "+tb_name;
		
		Connection con=DriverManager.getConnection(dburl,user,pwd);
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		int ColumnNum=rsmd.getColumnCount();
		//get the column names of the table
		Map<Integer,String> ColumnNames=new HashMap<Integer,String>();
		for(int i=1;i<=ColumnNum;i++)
		{
			ColumnNames.put(i,rsmd.getColumnName(i));
		}
		
		int count=0;
		while(rs.next())
		{
			count++;
			HashMap<String,String> keyvalue=new HashMap<String,String>();
			for(int i=1;i<=ColumnNum;i++)
			{
				String colName=ColumnNames.get(i);
				String colValue=rs.getString(i).trim();
				keyvalue.put(colName, colValue);
			}
			results.put(count, keyvalue);
		}
		
		con.close();
		return results;
	}
	
	public HashMap<String,String> ReadEncodeTable() throws SQLException
	{
		HashMap<String,String> results=new HashMap<String,String>();
		String dburl="jdbc:oracle:thin:@"+url+":1521:"+db_name;
		String sql="select * from "+tb_name+"_REFLECT";
		
		Connection con=DriverManager.getConnection(dburl,user,pwd);
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(sql);
		while(rs.next())
		{
			String key=rs.getString(2);
			String code=rs.getString(3);
			//System.out.println(key+"\t"+code);
			results.put(key,code);
		}
		con.close();
		return results;
	}
	
	public HashMap<String,String> ReadEventtypeEncodeTable() throws SQLException
	{
		HashMap<String,String> results=new HashMap<String,String>();
		String dburl="jdbc:oracle:thin:@"+url+":1521:"+db_name;
		String sql="select * from "+"event_type_reflect";
		
		Connection con=DriverManager.getConnection(dburl,user,pwd);
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(sql);
		while(rs.next())
		{
			String key=rs.getString(2).trim();
			String code=rs.getString(3).trim();
			//System.out.println(key+"\t"+code);
			results.put(key,code);
		}
		con.close();
		return results;
	}
	
	
	public static void main(String args[]) throws SQLException
	{
		Oracle_Connector oc=new Oracle_Connector("localhost","orcl","ysp","system","Liubin275288");
		/*
		HashMap<Integer,HashMap<String,String>> results=oc.Connect();
		Iterator<Entry<Integer,HashMap<String,String>>> it1=results.entrySet().iterator();
		int rows=0;
		while(it1.hasNext())
		{
			rows++;
			Entry<Integer,HashMap<String,String>> entry=it1.next();
			HashMap<String,String> kv=entry.getValue();
			Iterator<Entry<String,String>> it2=kv.entrySet().iterator();
			int count=0;
			while(it2.hasNext())
			{
				count++;
				Entry<String,String> ent=it2.next();
				System.out.println(count+"\t"+ent.getKey()+"\t"+ent.getValue());
			}
			System.out.println(rows);
		}
		*/
		
		oc.ReadEventtypeEncodeTable();
		/*
		Iterator<Entry<String,String>> encode=oc.ReadEncodeTable().entrySet().iterator();
		int count=0;
		while(encode.hasNext())
		{
			count++;
			Entry<String,String> entry=encode.next();
			String key=entry.getKey();
			String value=entry.getValue();
			System.out.println(count+"\t"+key+"\t"+value);
		}
		*/
	}
}
