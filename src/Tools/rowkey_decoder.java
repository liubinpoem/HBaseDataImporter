package Tools;

public class rowkey_decoder {
	
	public byte[] rowkey=new byte[18];
	
	public rowkey_decoder(byte[] rk)
	{
		rowkey=rk;
		decode_event_type();
		decode_device_type();
		decode_device_num();
		decode_event_type();
		decode_event_time();
		decode_monitor_device_type();
		decode_monitor_device_num();
	}
	
	public void decode_event_type()
	{
		int event_type=0;
		for(int i=6;i<8;i++)
		{
			event_type<<=8;
			event_type|= (rowkey[i] & 0xff);  
		}
		System.out.println("event_type:"+event_type);
	}
	
	public void decode_device_type()
	{
		int devicetype=0;
		for(int i=0;i<2;i++)
		{
			devicetype<<=8;
			devicetype|= (rowkey[i] & 0xff);  
		}
		System.out.println("devicetype:"+devicetype);
	}
	
	public void decode_device_num()
	{
		int devicenum=0;
		for(int i=2;i<6;i++)
		{
			devicenum<<=8;
			devicenum|= (rowkey[i] & 0xff);  
		}
		System.out.println("devicenum:"+devicenum);
	}
	
	public void decode_event_time()
	{
		int eventtime=0;
		for(int i=8;i<12;i++)
		{
			eventtime<<=8;
			eventtime|= (rowkey[i] & 0xff);  
		}
		System.out.println("eventtime:"+eventtime);
	}
	
	public void decode_monitor_device_type()
	{
		int monitordevicetype=0;
		for(int i=12;i<14;i++)
		{
			monitordevicetype<<=8;
			monitordevicetype|= (rowkey[i] & 0xff);  
		}
		System.out.println("monitordevicetype:"+monitordevicetype);
	}
	
	public void decode_monitor_device_num()
	{
		int monitordevicenum=0;
		for(int i=14;i<18;i++)
		{
			monitordevicenum<<=8;
			monitordevicenum|= (rowkey[i] & 0xff);  
		}
		System.out.println("monitordevicenum:"+monitordevicenum);
	}
}
