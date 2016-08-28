package HBaseDataImporter;

public class content_detector {
	
	public String oracle_parameters_detect(String url,String db_name,String tb_name,String user,String pwd)
	{
		String result="0";
		if(url.equals(""))
		{
			result="Empty url!";
			System.out.println(result);
			return result;
		}
		if(db_name.equals(""))
		{
			result="Empty database name!";
			return result;
		}
		if(tb_name.equals(""))
		{
			result="Empty table name!";
			return result;
		}
		if(user.equals(""))
		{
			result="Empty user name!";
			return result;
		}
		if(pwd.equals(""))
		{
			result="Empty password!";
			return result;
		}
		return result="1";
	}
}
