package metaData;

import util.Util;

public class DavisBaseTablesCell{
	
	int rowid_size = Util.INT_REAL_SIZE;
	int table_name_size = Util.TEXT_SIZE;
	int record_count_size = Util.INT_REAL_SIZE;
	int avg_length_size = Util.SMALLINT_SIZE;
	int payload_size = Util.SMALLINT_SIZE;
	int clmn_cnt_size = Util.TINYINT_SIZE;
	int table_name_cnt_size = Util.TINYINT_SIZE;
	int record_count_cnt_size = Util.TINYINT_SIZE;
	int avg_length_cnt_size = Util.TINYINT_SIZE;
	
	byte table_name_byte = Util.TEXT;
	byte record_count_byte = Util.INT;
	byte avg_length_byte = Util.SMALLINT;
	
	int rcd_size = payload_size+rowid_size+
			clmn_cnt_size+table_name_cnt_size+record_count_cnt_size+avg_length_cnt_size+
			table_name_size+record_count_size+avg_length_size;
	
	String table_name;
	int record_count,avg_length;
	
	public int getSize()
	{
		return rcd_size;
	}
	
	public int getPayloadSize()
	{
		return (rcd_size-payload_size-rowid_size);
	}
	
	public String getTableName()
	{
		return table_name;
	}
	
	public int getRcdCnt()
	{
		return record_count;
	}
	
	public int getAvgLngth()
	{
		return avg_length;
	}
	
	public DavisBaseTablesCell(String table_name, int record_count, int avg_length)
	{
		this.table_name = table_name;
		this.record_count = record_count;
		this.avg_length = avg_length;
	}
	
	public DavisBaseTablesCell()
	{

	}
	
	public int getClmnCnt()
	{
		int clmnCnt = 3; // table_name,record_count,avg_length
		return clmnCnt;
	}
	
	public byte getTblNameByte()
	{
		return table_name_byte;
	}
	
	public byte getRcdCntByte()
	{
		return record_count_byte;
	}
	
	public byte getAvgLngthByte()
	{
		return avg_length_byte;
	}
	
}

