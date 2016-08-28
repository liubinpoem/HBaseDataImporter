package Tools;

public class qualifier_generator {
	public byte[] qualifier=new byte[5];
	public int event_type,column_name;
	
	public qualifier_generator(int evt_type,int col_name)
	{
		event_type=evt_type;
		column_name=col_name;
		generate_event_type();
		generate_column_name();
	}

	public void generate_event_type()
	{
		qualifier[0]=(byte)((event_type>>8)&0xff);
		qualifier[1]=(byte)(event_type&0xff);
	}
	
	public void generate_column_name()
	{
		qualifier[2]=(byte)((column_name>>16)&0xff);
		qualifier[3]=(byte)((column_name>>8)&0xff);
		qualifier[4]=(byte)(column_name&0xff);
	}
}
