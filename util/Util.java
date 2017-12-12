package util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import db.DavisBasePrompt;
import metaData.DavisBaseColumnsCell;

public class Util {

	public static int TINYINT_NULL_SIZE = 1;
	public static int SMALLINT_NULL_SIZE = 2;
	public static int INT_REAL_NULL_SIZE = 4;
	public static int DOUBLE_DATETIME_DATE_NULL_SIZE = 4;
	public static int TINYINT_SIZE = 1;
	public static int SMALLINT_SIZE = 2;
	public static int INT_REAL_SIZE = 4;
	public static int BIGINT_SIZE = 8;
	public static int DOUBLE_DATETIME_DATE_SIZE = 8;
	public static int TEXT_SIZE = 20;
	
	public static byte TINYINT_NULL = 0x00;
	public static byte SMALLINT_NULL = 0x01;
	public static byte INT_REAL_NULL = 0x02;
	public static byte DOUBLE_DATETIME_DATE_NULL = 0x03;
	public static byte TINYINT = 0x04;
	public static byte SMALLINT = 0x05;
	public static byte INT = 0x06;
	public static byte BIGINT = 0x07;
	public static byte REAL = 0x08;
	public static byte DOUBLE = 0x09;
	public static byte DATETIME = 0x0A;
	public static byte DATE = 0x0B;
	public static byte TEXT = 0x14;
	
	public static int NODE_RCD_LNGTH = 4 + 4; // page number int + integer key
	
	public static String NULL_STR = "NULL";
	public static String TINYINT_STR = "TINYINT";
	public static String SMALLINT_STR = "SMALLINT";
	public static String INT_STR = "INT";
	public static String BIGINT_STR = "BIGINT";
	public static String REAL_STR = "REAL";
	public static String DOUBLE_STR = "DOUBLE";
	public static String DATETIME_STR = "DATETIME";
	public static String DATE_STR = "DATE";
	public static String TEXT_STR = "TEXT";
	
	public static String PRIMARY_KEY_STR = "PRI";
	public static String NOTNULL_STR = "NOT NULL";
	
	public static int PRIMARY_KEY_INT = 0;
	public static int NOTNULL_INT = 1;
			
	public static String davisBaseTblPath = "data/catalog/davisbase_tables.tbl";
	public static String davisBaseClmnPath = "data/catalog/davisbase_columns.tbl";
	public static String davisbase_tables = "davisbase_tables";
	public static String davisbase_columns = "davisbase_columns";
	public static int pageSize = 512;
	
	public static String data = "data/";
	public static String catalog = "catalog/";
	public static String user_data = "user_data/";
	public static String suffix = ".tbl";
	
	public static ArrayList<String> OPERATORS_LIST = new ArrayList<String>(Arrays.asList(new String[]{">=","<=","!=","=",">","<"}));
		
	public static int getDataLength(byte data_type)
	{
		int data_length = 0;
		
		switch(data_type)
		{
		case 0x00: // TINYINT_NULL
			data_length = TINYINT_NULL_SIZE;
			break;
		case 0x01: // SMALLINT_NULL
			data_length = SMALLINT_NULL_SIZE;
			break;
		case 0x02: // INT_REAL_NULL
			data_length = INT_REAL_NULL_SIZE;
			break;
		case 0x03: // DOUBLE_DATETIME_DATE_NULL
			data_length = DOUBLE_DATETIME_DATE_NULL_SIZE;
			break;
		case 0x04: // TINYINT
			data_length = TINYINT_SIZE;
			break;
		case 0x05: // SMALLINT
			data_length = SMALLINT_SIZE;
			break;
		case 0x06: // INT
			data_length = INT_REAL_SIZE;
			break;
		case 0x07: // BIGINT
			data_length = BIGINT_SIZE;
			break;
		case 0x08: // REAL
			data_length = INT_REAL_SIZE;
			break;
		case 0x09: // DOUBLE
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case 0x0A: // DATETIME
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case 0x0B: // DATE
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case 0x14: // TEXT
			data_length = TEXT_SIZE;
			break;
		default:
			break;
		}
		
		return data_length;
	}
	
	public static int getDataLength(String data_type)
	{
		int data_length = -1;
		
		switch(data_type.trim().toUpperCase())
		{

		case "NULL": // NULL
			data_length = DOUBLE_DATETIME_DATE_NULL_SIZE;
			break;
		case "TINYINT": // TINYINT
			data_length = TINYINT_SIZE;
			break;
		case "SMALLINT": // SMALLINT
			data_length = SMALLINT_SIZE;
			break;
		case "INT": // INT
			data_length = INT_REAL_SIZE;
			break;
		case "BIGINT": // BIGINT
			data_length = BIGINT_SIZE;
			break;
		case "REAL": // REAL
			data_length = INT_REAL_SIZE;
			break;
		case "DOUBLE": // DOUBLE
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case "DATETIME": // DATETIME
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case "DATE": // DATE
			data_length = DOUBLE_DATETIME_DATE_SIZE;
			break;
		case "TEXT": // TEXT
			data_length = TEXT_SIZE;
			break;
		default:
			System.out.println("Cannot parse the data type: " + data_type);
			break;
		}
		
		return data_length;
	}
	
	public static String getDataType(byte data_type)
	{
		String data_str = "";
		
		switch(data_type)
		{
		case 0x00: // TINYINT_NULL
			data_str = NULL_STR;
			break;
		case 0x01: // SMALLINT_NULL
			data_str = NULL_STR;
			break;
		case 0x02: // INT_REAL_NULL
			data_str = NULL_STR;
			break;
		case 0x03: // DOUBLE_DATETIME_DATE_NULL
			data_str = NULL_STR;
			break;
		case 0x04: // TINYINT
			data_str = TINYINT_STR;
			break;
		case 0x05: // SMALLINT
			data_str = SMALLINT_STR;
			break;
		case 0x06: // INT
			data_str = INT_STR;
			break;
		case 0x07: // BIGINT
			data_str = BIGINT_STR;
			break;
		case 0x08: // REAL
			data_str = REAL_STR;
			break;
		case 0x09: // DOUBLE
			data_str = DOUBLE_STR;
			break;
		case 0x0A: // DATETIME
			data_str = DATETIME_STR;
			break;
		case 0x0B: // DATE
			data_str = DATE_STR;
			break;
		case 0x14: // TEXT
			data_str = TEXT_STR;
			break;
		default:
			break;
		}
		
		return data_str;
	}

	public static HashMap<Integer,String> printHeaders(LinkedList<DavisBaseColumnsCell> clmns, String[] columns) {
		
		HashMap<Integer,String> clmn_headers = new HashMap<Integer,String>();
		
		for(int i=0;i<clmns.size();i++)
		{
			DavisBaseColumnsCell rcd = clmns.get(i);
			String clmn_name = rcd.getClmnName().trim();
			int ordinal_pos = rcd.getOrdinalPos();
			int data_length = Util.getDataLength(rcd.getDataType());
			
			for(int j=0;j<columns.length;j++)
			{
				if(clmn_name.equalsIgnoreCase(columns[j].trim()))
				{
					System.out.print(String.format("%-"+data_length+"s\t", clmn_name.trim()));
					//System.out.print("\t");
					clmn_headers.put(ordinal_pos,clmn_name);	
				}
			}
			
		}
				
		System.out.println();
		System.out.println(DavisBasePrompt.line("-", 120));
		
		return clmn_headers;
		
	}
	
	public static LinkedList<String> printHeaders(LinkedList<DavisBaseColumnsCell> clmns) {

		LinkedList<String> clmn_headers = new LinkedList<String>();
		
		for(int i=0;i<clmns.size();i++)
		{
			DavisBaseColumnsCell rcd = clmns.get(i);
			String clmn_name = rcd.getClmnName().trim();
			int data_length = Util.getDataLength(rcd.getDataType());
			
			System.out.print(String.format("%-"+data_length+"s\t", clmn_name.trim()));
			//System.out.print("\t");
			clmn_headers.add(clmn_name);				
		}		
		System.out.println();
		System.out.println(DavisBasePrompt.line("-", 120));
		
		return clmn_headers;
	}
	
	public static String findFilePath(String table)
	{
		// print valid records 
		String path = "data/";
		
		if(table.equalsIgnoreCase(Util.davisbase_tables)
				||table.equalsIgnoreCase(Util.davisbase_columns))
		{
			path += Util.catalog+table+Util.suffix;
		}
		else
		{
			path += Util.user_data+table+Util.suffix;
		}
		
		return path;
	}

	public static boolean compare(byte[] rcd, String clmn_data_type, String operator, String value) {
		
		boolean is_true = false;
		
		switch(clmn_data_type.trim().toUpperCase())
		{
			case "NULL":
				String column_null = new String(rcd).trim();
				if(operator.equals("="))
				{
					is_true = column_null.equalsIgnoreCase(value.trim());
				}else if(operator.equals("!="))
				{
					is_true = !column_null.equalsIgnoreCase(value.trim());
				}
				break;
			case "TINYINT":
				int column_tinyint = (int)java.nio.ByteBuffer.wrap(rcd).get();
				is_true = compare(column_tinyint,operator,value);
				break;
			case "SMALLINT":
				int column_smallint = (int)java.nio.ByteBuffer.wrap(rcd).getShort();
				is_true = compare(column_smallint,operator,value);
				break;
			case "INT":
				int column_int = (int)java.nio.ByteBuffer.wrap(rcd).getInt();
				is_true = compare(column_int,operator,value);
				break;
			case "BIGINT":
				int column_bigint = (int)java.nio.ByteBuffer.wrap(rcd).getLong();
				is_true = compare(column_bigint,operator,value);
				break;
			case "REAL":

			case "DOUBLE":
				double column_real = (double)java.nio.ByteBuffer.wrap(rcd).getDouble();
				is_true = compare(column_real,operator,value);
				break;
			case "DATETIME":
				value.replaceAll("-", "").replaceAll("_", "").replaceAll(":", "");
				long column_long_datetime = java.nio.ByteBuffer.wrap(rcd).getLong();
				is_true = compare(column_long_datetime,operator,value);
				break;
			case "DATE":
				value.replaceAll(":", "");
				long column_long_date = java.nio.ByteBuffer.wrap(rcd).getLong();
				is_true = compare(column_long_date,operator,value);
				break;				
			case "TEXT":
				String column_str = new String(rcd).trim();
				if(operator.equals("="))
				{
					is_true = column_str.equalsIgnoreCase(value.trim());
				}else if(operator.equals("!="))
				{
					is_true = !column_str.equalsIgnoreCase(value.trim());
				}
				break;
			default:
				System.out.println("Invalid data type: " + clmn_data_type);
		}
		
		return is_true;
		
	}

	private static boolean compare(double column_double, String operator, String value) {
		boolean is_true = false;
		
		try
		{
			Double.parseDouble(value);
		}catch(NumberFormatException e)
		{
			System.out.println("An integer must compare to an integer: " + value);
			return is_true;
		}
		
		switch(operator.trim())
		{
			case ">=":
				is_true = (column_double>=Double.parseDouble(value));
				break;
			case "<=":
				is_true = (column_double<=Double.parseDouble(value));
				break;
			case "!=":
				is_true = (column_double!=Double.parseDouble(value));
				break;
			case "=":
				is_true = (column_double==Double.parseDouble(value));
				break;
			case ">":
				is_true = (column_double>Double.parseDouble(value));
				break;
			case "<":
				is_true = (column_double<Double.parseDouble(value));
				break;
			default:
				System.out.println("Cannot parse the operator: " + operator);
				break;
		}
				
		return is_true;
	}

	public static boolean compare(int column, String operator, String value) {
		
		boolean is_true = false;
		
		try
		{
			Integer.parseInt(value);
		}catch(NumberFormatException e)
		{
			System.out.println("An integer must compare to an integer: " + value);
			return is_true;
		}
		
		switch(operator.trim())
		{
			case ">=":
				is_true = (column>=Integer.parseInt(value));
				break;
			case "<=":
				is_true = (column<=Integer.parseInt(value));
				break;
			case "!=":
				is_true = (column!=Integer.parseInt(value));
				break;
			case "=":
				is_true = (column==Integer.parseInt(value));
				break;
			case ">":
				is_true = (column>Integer.parseInt(value));
				break;
			case "<":
				is_true = (column<Integer.parseInt(value));
				break;
			default:
				System.out.println("Cannot parse the operator: " + operator);
				break;
		}
				
		return is_true;
	}

	public static void initTblFile(String file_path,int rcdPosition)
	{
		RandomAccessFile table;
		try {
			table = new RandomAccessFile(file_path,"rw");
	
			table.setLength(Util.pageSize*1);
		
			int rcdLocation = 0;
			int currentPg = 0;
			int pgLocation = Util.pageSize*currentPg;
		
			// define the header
			table.seek(pgLocation+rcdLocation);
			// a leaf page
			table.write(0x0D); // 1 byte
			// no. of records = 0
			table.write(0x00); // 1 byte
		
			// define start of content
			table.writeShort(rcdPosition); // 2 bytes
		
			// the right most page
			table.writeInt(-1); // 4 bytes
		
			table.close();	
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public static byte getDataByte(String data_str) {

		byte data_byte = 0x16;
		
		switch(data_str)
		{
		case "TINYINT_NULL": // TINYINT_NULL
			data_byte = TINYINT_NULL;
			break;
		case "SMALLINT_NULL": // SMALLINT_NULL
			data_byte = SMALLINT_NULL;
			break;
		case "INT_REAL_NULL": // INT_REAL_NULL
			data_byte = INT_REAL_NULL;
			break;
		case "DOUBLE_DATETIME_DATE_NULL": // DOUBLE_DATETIME_DATE_NULL
			data_byte = DOUBLE_DATETIME_DATE_NULL;
			break;
		case "TINYINT": // TINYINT
			data_byte = TINYINT;
			break;
		case "SMALLINT": // SMALLINT
			data_byte = SMALLINT;
			break;
		case "INT": // INT
			data_byte = INT;
			break;
		case "BIGINT": // BIGINT
			data_byte = BIGINT;
			break;
		case "REAL": // REAL
			data_byte = REAL;
			break;
		case "DOUBLE": // DOUBLE
			data_byte = DOUBLE;
			break;
		case "DATETIME": // DATETIME
			data_byte = DATETIME;
			break;
		case "DATE": // DATE
			data_byte = DATE;
			break;
		case "TEXT": // TEXT
			data_byte = TEXT;
			break;
		default:
			break;
		}
		
		return data_byte;
	}

	public static boolean chckTblExist(String table_name) {
		
		boolean is_exist = true;
		
		String path = findFilePath(table_name);
		
		File table = new File(path);
		
		if(!table.exists()||!table.isFile())
		{
			System.out.println("the table doesnot exist: \"" + table_name + "\"");
			is_exist = false;
		}
		
		return is_exist;
	}

	public static int splitPg(String table_name, int current_Pg)
	{
		int pages = 0;
		RandomAccessFile table;
		String path = findFilePath(table_name);
		
		try {
			table = new RandomAccessFile(path,"rw");
			
			pages = (int)(table.length()/pageSize);
			
			int new_length = (pages+1)*pageSize;
					
			table.setLength(new_length);
			
			// right most page
			int rgt_pg_offset = current_Pg*pageSize+4;
			table.seek(rgt_pg_offset);			
			table.writeInt(pages);
			
			rgt_pg_offset = pages*pageSize+4;
			table.seek(rgt_pg_offset);
			
			table.writeInt(-1);
						
			table.seek(pages*pageSize);
			
			table.write(0x0D);
			
			table.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		return pages;
	}
	
	public static boolean chckPgIsFull(String table_name, int rcd_size, int currentPg) {
		
		boolean isFull = false;
		int rcd_cnt = -1;
		
		String path = findFilePath(table_name);
		
		RandomAccessFile table;
		try {
			
			table = new RandomAccessFile(path,"r");
			table.seek(currentPg*Util.pageSize + 1);
			
			rcd_cnt = table.read();
			
			table.close();
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		int left_size = Util.pageSize - (8+2*rcd_cnt + rcd_size*rcd_cnt);
		
		if(left_size < rcd_size+2)
		{
			isFull = true;
		}
				
		return isFull;
	}

	public static boolean chckDnplct(String table_name, int value) {
		
		boolean duplicate = true;
		
		String path = findFilePath(table_name);
		
		RandomAccessFile table;
		try {
			
			table = new RandomAccessFile(path,"r");
			int pg_cnt = (int)table.length()/Util.pageSize;
			int key_offset = -1;
			int current_pg = 0;
			int rcd_cnt = -1;
			int index_offset = -1;
			int pri_key_offset = -1;
			int pri_key = -1;
								
			for(int i=0;i<pg_cnt;i++)
			{
				table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = table.read();
				if(rcd_cnt == 0)
				{
					table.close();
					return false;
				}
				
				index_offset = current_pg*Util.pageSize + 8;
				
				for(int j=0;j<rcd_cnt;j++)
				{
					table.seek(index_offset);
					key_offset = table.readShort();
					pri_key_offset = key_offset + 2;
					table.seek(pri_key_offset);
					pri_key = table.readInt();
					if(pri_key == value)
					{
						table.close();
						System.out.println("primary key duplicate: " + value);
						return duplicate;
					}
					index_offset += 2;
				}
				
				current_pg++;
				
			}
			
			table.close();
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		duplicate = false;
		return duplicate;
	}

	public static boolean chckNullable(LinkedList<DavisBaseColumnsCell> tbl_clmns, int start) {
		
		boolean nullable = false;
		int count = tbl_clmns.size();
		
		for(int i=start;i<count;i++)
		{
			if(tbl_clmns.get(i).getIsNullable().equalsIgnoreCase("NO"))
			{
				System.out.println("column cannot be null: " + tbl_clmns.get(i).getClmnName());
				return nullable;
			}
		}
			
		nullable = true;
		return nullable;
	}

}
