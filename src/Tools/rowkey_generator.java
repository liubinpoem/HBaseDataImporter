package Tools;

public class rowkey_generator {
	public byte[] row_key=new byte[18];
	
	public rowkey_generator(int device_type,int device_num,int event_type,
			long event_time,int monitor_device_type,int monitor_device_num)
	{
		generate_event_type(event_type);
		generate_device_id(device_type,device_num);
		generate_event_time(event_time);
		generate_monitordevice_type(monitor_device_type);
		generate_monitordevice_num(monitor_device_num);
	}
	
	//generate the device id of row_key
	public void generate_device_id(int device_type,int device_num)
	{
		row_key[0]=(byte)((device_type>>8)&0xff);
		row_key[1]=(byte)(device_type&0xff);
		row_key[2]=(byte)((device_num>>24)&0xff);
		row_key[3]=(byte)((device_num>>16)&0xff);
		row_key[4]=(byte)((device_num>>8)&0xff);
		row_key[5]=(byte)(device_num&0xff);
		//System.out.println("device_type:"+device_type);
		//System.out.println("device_num:"+device_num);
	}
		
	//transform event type to byte array
	public void generate_event_type(int event_type)
	{
		row_key[6]=(byte)((event_type>>>8)&0xff);
		row_key[7]=(byte)(event_type&0xff);
		//System.out.println("eventtype:"+event_type);
	}
	
	//generate the event time
	public void generate_event_time(long event_time)
	{
		row_key[8]=(byte)((event_time>>24)&0xff);
		row_key[9]=(byte)((event_time>>16)&0xff);
		row_key[10]=(byte)((event_time>>8)&0xff);
		row_key[11]=(byte)(event_time&0xff);
		//System.out.println("event_time:"+event_time);
	}
	
	//generate the monitor_device type
	public void generate_monitordevice_type(int mdevice_type)
	{
		row_key[12]=(byte)((mdevice_type>>8)&0xff);
		row_key[13]=(byte)(mdevice_type&0xff);
		//System.out.println("monitor_device_type:"+mdevice_type);
	}
	
	//generate the monitor_device num
	public void generate_monitordevice_num(int mdevice_num)
	{
		row_key[14]=(byte)((mdevice_num>>24)&0xff);
		row_key[15]=(byte)((mdevice_num>>16)&0xff);
		row_key[16]=(byte)((mdevice_num>>8)&0xff);
		row_key[17]=(byte)(mdevice_num&0xff);
		//System.out.println("monitor_device_num:"+mdevice_num);
	}
	
	/*
	public static void main(String args[])
	{
		long event_time=System.currentTimeMillis()/1000;
		int[] a={1,2,3,4,5};
		rowkey_generator rkg=new rowkey_generator(a[0],a[1],a[2],event_time,a[3],a[4]);
		
		rowkey_decoder rd=new rowkey_decoder(rkg.row_key);
	}
	*/
}
