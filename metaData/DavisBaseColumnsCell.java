package metaData;

import util.Util;

public class DavisBaseColumnsCell
{
	
	int rowid_size = Util.INT_REAL_SIZE;
	int table_name_size = Util.TEXT_SIZE;
	int clmn_name_size = Util.TEXT_SIZE;
	int data_type_size = Util.TEXT_SIZE;
	int ordinal_pos_size = Util.TINYINT_SIZE;
	int nullable_size = Util.TEXT_SIZE;
	int payload_size = Util.SMALLINT_SIZE;
	int clmn_cnt_size = Util.TINYINT_SIZE;
	int table_name_cnt_size = Util.TINYINT_SIZE;
	int clmn_name_cnt_size = Util.TINYINT_SIZE;
	int data_type_cnt_size = Util.TINYINT_SIZE;
	int ordinal_pos_cnt_size = Util.TINYINT_SIZE;
	int nullable_cnt_size = Util.TEXT_SIZE;
	int column_key_size = Util.TEXT_SIZE;
	
	int rcd_size = payload_size + rowid_size + 
			clmn_cnt_size+table_name_size + clmn_name_cnt_size+data_type_cnt_size+
			ordinal_pos_cnt_size+nullable_cnt_size+clmn_name_size +
			data_type_size + ordinal_pos_size + nullable_size + column_key_size;
	
	byte table_name_byte = Util.TEXT;
	byte clmn_name_byte = Util.TEXT;
	byte data_type_byte = Util.TEXT;
	byte ordinal_pos_byte = Util.TINYINT;
	byte nullable_byte = Util.TEXT;
	byte column_key_byte = Util.TEXT;
	
	int ordinal_pos, rowid;
	String rowid_type,table_name,
	column_name, data_type, 
	is_nullable,column_key;
	
	public int getSize()
	{
		return rcd_size;
	}
	
	public int getPayloadSize()
	{
		return rcd_size-(payload_size + rowid_size);
	}
	
	public DavisBaseColumnsCell()
	{
		
	}
	
	public DavisBaseColumnsCell(int rowid,String table_name,
			String column_name, String data_type,  
			int ordinal_pos, String is_nullable, String column_key)
	{
		this.rowid = rowid;
		this.table_name = table_name;
		this.column_name = column_name;
		this.data_type = data_type;
		this.ordinal_pos = ordinal_pos;
		this.is_nullable = is_nullable;
		this.column_key = column_key;
	}
	
	public int getRowid()
	{
		return rowid;
	}
	
	public String getTableName()
	{
		return table_name;
	}
	
	public String getClmnName()
	{
		return column_name;
	}

	public String getDataType()
	{
		return data_type;
	}
	
	public int getOrdinalPos()
	{
		return ordinal_pos;
	}

	public String getIsNullable()
	{
		return is_nullable;
	}
	
	public String getClmnKey()
	{
		return column_key;
	}
	
	public int getClmnCnt()
	{
		int clumnCnt = 6;
		return clumnCnt;
	}
	
	public byte getTblNameByte()
	{
		return table_name_byte;
	}
	
	public byte getClmnNameByte()
	{
		return clmn_name_byte;
	}
	
	public byte getDataTypeByte()
	{
		return data_type_byte;
	}
	
	public byte getOrdinalPosByte()
	{
		return ordinal_pos_byte;
	}
	
	public byte getNullableByte()
	{
		return nullable_byte;
	}
	
	public byte getClmnKeyByte()
	{
		return column_key_byte;
	}
	
}
