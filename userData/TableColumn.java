package userData;

import util.Util;

public class TableColumn{
	
	String column_name,data_type,pri_null;
	int clmn_length;
		
	public TableColumn(String column_name, String data_type, String pri_null)
	{
		this.column_name = column_name;
		this.data_type = data_type;
		this.pri_null = pri_null;
		this.clmn_length = Util.getDataLength(data_type);
	}
	
	public String getClmnNm()
	{
		return column_name;
	}
	
	public String getDtTp()
	{
		return data_type;
	}
	
	public String getPrNll()
	{
		return pri_null;
	}
	
	public int getClmnLngth()
	{			
		return clmn_length;		
	}
			
}