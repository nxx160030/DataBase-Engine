package commands;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import metaData.DavisBaseColumns;
import metaData.DavisBaseTables;
import metaData.DavisBaseColumnsCell;
import java.util.ListIterator;

import util.Util;

//parse select,exit commands
public class VDL {
	
	public boolean parseCmd(String userCommand) {
		
		boolean success = false;
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0).trim().toLowerCase()) {
			case "select":
				success = parseSelect(userCommand);
				if(!success)
					System.out.println("format example: select *|column1,column2 from table_name [where column operator value];");
				break;
			case "exit":
				success = parseExit(userCommand);
				break;
		}
		
		return success;
	}
	
	private boolean parseSelect(String userCommand)
	{
		boolean success = false;
		
		userCommand = userCommand.replaceAll(" ,", ",").replaceAll(", ", ",");
		
		if(userCommand.contains(">="))
			userCommand = userCommand.replaceAll(">=", " >= ");
		else if(userCommand.contains("<="))
			userCommand = userCommand.replaceAll("<=", " <= ");
		else if(userCommand.contains("!="))
			userCommand = userCommand.replaceAll("!=", " != ");
		else if(userCommand.contains("="))
			userCommand = userCommand.replaceAll("=", " = ");
		else if(userCommand.contains(">"))
			userCommand = userCommand.replaceAll(">", " > ");
		else if(userCommand.contains("<"))
			userCommand = userCommand.replaceAll("<", " < ");
		
		String[] commandTokens = userCommand.split(" ");
		
		if(commandTokens.length < 4)
		{
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			return success;
		}

		String[] columns = commandTokens[1].split(",");
		String from = commandTokens[2].trim();
		if(!from.equalsIgnoreCase("from"))
		{
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			return success;
		}
		
		String table = commandTokens[3].trim();
		
		if(commandTokens.length > 4 && commandTokens.length < 8)
		{
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			return success;
		}
		
		if(commandTokens.length == 8)
		{
			String where = commandTokens[4];
			if(!where.equalsIgnoreCase("where"))
			{
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				return success;
			}
			
			String column = commandTokens[5].trim();
			String operator = commandTokens[6].trim();
			String value = commandTokens[7].replaceAll("'", "").trim();
			
			selectOperatorRecords(table,columns,column,operator,value);	
		}
			
		if(commandTokens.length == 4)
		{
			if(columns.length==1&&columns[0].trim().equals("*"))
				selectStarRecords(table);
			else
				selectAllRecords(table,columns);
		}

		success = true;
		return success;		
	}
	
	// read all records
	private void selectStarRecords(String table) {
		
		// check if the table exists in davisbase_tables
		DavisBaseTables davisbase_tables = new DavisBaseTables();
		if(!davisbase_tables.checkTblRcd(table))
		{
			System.out.println("The table doesnot exist: " + table);
			return;
		}
		
		// get column_name, data_type, ordinal_position, is_nullable, column_key from davisbase_columns
		DavisBaseColumns davisbase_columns = new DavisBaseColumns();
		LinkedList<DavisBaseColumnsCell> clmns = davisbase_columns.getRcds(table);
		
		if(clmns.isEmpty())
		{
			System.out.println("There are no columns in the table: " + table);
			return;
		}
				
		String path = Util.findFilePath(table);
		
		try {
			RandomAccessFile rd_table = new RandomAccessFile(path,"r");
			int pg_cnt = (int)rd_table.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			for(int i=0;i<pg_cnt;i++)
			{
				// read record count
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt += rd_table.read();
			}
					
			// no records
			if(rcd_cnt==0)
			{
				System.out.println("There are no records in the table: " + table);
				Util.printHeaders(clmns);
				rd_table.close();
				return;				
			}
			
			// print headers
			LinkedList<String> clmn_headers = Util.printHeaders(clmns);
			current_pg = 0;
			for(int j=0;j<pg_cnt;j++)
			{
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = rd_table.read();
				int start_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				// print records			
				for(int i=0;i<rcd_cnt;i++)
				{
					// move to record
					rd_table.seek(start_offset);
					int rcd_offset = rd_table.readShort(); 
					rd_table.seek(rcd_offset);
					
					// read payload
					rd_table.readShort(); // 2 bytes
					// read primary key or rowid
					int rowid = rd_table.readInt(); // 4 bytes
					System.out.print(rowid + "\t");
					
					// read columns count
					int clmns_cnt = rd_table.read(); // 1 byte
					// data_type offset
					int data_type_offset = rcd_offset+7; // payload+rowid+clmns_cnt
									
					rcd_offset += 7+clmns_cnt; // where columns begin
					// read each column
					for(int k=0;k<clmns_cnt;k++)
					{
						// read data type
						rd_table.seek(data_type_offset);
						byte data_type = rd_table.readByte();
						
						// get data_str and data_length
						String data_str = Util.getDataType(data_type);
						int data_length = Util.getDataLength(data_type);
						
						int header_length = clmn_headers.get(k).trim().length();
						int clmn_length = Util.getDataLength(data_str);
						int print_length = Math.max(header_length,clmn_length);
								
						// read and print the column
						readAndPrintClmn(rd_table,rcd_offset,data_str,print_length);
						
						rcd_offset += data_length;
						data_type_offset++;
					}
						
					System.out.println();
					
					// advance offset to next record
					start_offset += 2;
				}
			}
		
			rd_table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	// select columns from table where column operator value
	private void selectOperatorRecords(String table, String[] columns, String column, String operator, String value) {
		
		// check if the table exists in davisbase_tables
		DavisBaseTables davisbase_tables = new DavisBaseTables();
		if(!davisbase_tables.checkTblRcd(table))
		{
			System.out.println("The table doesnot exist: " + table);
			return;
		}
		
		// get column_name, data_type, ordinal_position, is_nullable from davisbase_columns
		DavisBaseColumns davisbase_columns = new DavisBaseColumns();
		LinkedList<DavisBaseColumnsCell> clmns = davisbase_columns.getRcds(table);			
	
		if(clmns.isEmpty())
		{
			System.out.println("There are no columns in the table: " + table);
			return;
		}
		
		String pri_key = null;
		for(int i=0;i<clmns.size();i++)
		{
			// find the primary key column name
			if(clmns.get(i).getClmnKey().trim().equalsIgnoreCase(Util.PRIMARY_KEY_STR))
			{
				pri_key = clmns.get(i).getClmnName().trim();
			}
		}
		
		// select *
		if(columns.length == 1 && columns[0].trim().equals("*"))
		{
			columns = new String[clmns.size()];
			for(int i=0;i<clmns.size();i++)
			{
				columns[i] = clmns.get(i).getClmnName().trim();
			}
		}
		
		// get column ordinal_position
		LinkedList<Integer> ordinal_pos_list = new LinkedList<Integer>();
		
		// check if columns exists
		for(int i=0;i<columns.length;i++)
		{
			boolean clmn_exist = false;
			ListIterator<DavisBaseColumnsCell> itr = clmns.listIterator();
			while(itr.hasNext())
			{
				DavisBaseColumnsCell rcd = itr.next();
				if(rcd.getClmnName().equalsIgnoreCase(columns[i].trim()))
				{
					ordinal_pos_list.add(rcd.getOrdinalPos());
					clmn_exist = true;
				}
			}
			if(!clmn_exist)
			{
				System.out.println("There are no column: "+ columns[i].trim()+" in the table: " + table);
				return;
			}
		}
				
		// check if column exist
		boolean clmn_exist = false;
		int clmn_ordinal_pos = -1;
		int clmn_data_length = -1;
		String clmn_data_type = "";
		ListIterator<DavisBaseColumnsCell> itr1 = clmns.listIterator();
		while(itr1.hasNext())
		{
			DavisBaseColumnsCell rcd = itr1.next();
			if(rcd.getClmnName().equalsIgnoreCase(column.trim()))
			{
				clmn_exist = true;
				clmn_ordinal_pos = rcd.getOrdinalPos();
				clmn_data_length = Util.getDataLength(rcd.getDataType());
				clmn_data_type = rcd.getDataType();
			}
		}
		if(!clmn_exist)
		{
			System.out.println("There are no column: "+ column.trim()+" in the table: " + table);
			return;
		}
		
		
		// check if operator is valid
		if(!Util.OPERATORS_LIST.contains(operator.trim()))
		{
			System.out.println("Invalid operator: "+ operator);
			return;
		}
		
		// check when String/NULL with operator other than = or !=
		ListIterator<DavisBaseColumnsCell> itr2 = clmns.listIterator();
		while(itr2.hasNext())
		{
			DavisBaseColumnsCell clmn = itr2.next();
			if(clmn.getClmnName().equalsIgnoreCase(column.trim())&&
					(clmn.getDataType().equalsIgnoreCase(Util.TEXT_STR)||
							clmn.getDataType().equalsIgnoreCase(Util.NULL_STR)))
			{
				if(!operator.trim().equals("!=")&&
						!operator.trim().equals("="))
				{
					
					System.out.println("Only != or = can be used upon "+ clmn.getDataType() + ": " +column.trim());
					return;
				}
			}
		}
		
		// print headers
		HashMap<Integer,String> clmn_headers = Util.printHeaders(clmns,columns);
		
		// print valid records 
		String path = Util.findFilePath(table);
				
		try {
			RandomAccessFile rd_table = new RandomAccessFile(path,"r");
			int pg_cnt = (int)rd_table.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			// read record count
			for(int i=0;i<pg_cnt;i++)
			{
				// read record count
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt += rd_table.read();
			}
					
			// no records
			if(rcd_cnt==0)
			{
				System.out.println("There are no records in the table: " + table);
				rd_table.close();
				return;				
			}
			
			// columns list
			ArrayList<String> clmn_list = new ArrayList<String>(Arrays.asList(columns));
					
			for(int j=0;j<pg_cnt;j++)
			{
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = rd_table.read();
				int start_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				// print records			
				for(int i=0;i<rcd_cnt;i++)
				{
					// move to record
					rd_table.seek(start_offset);
					int rcd_offset = rd_table.readShort()+2; // rowid location
					rd_table.seek(rcd_offset);	
									
					// check if this record is valid with where condition
					// column is rowid or primary key
					if(column.trim().equalsIgnoreCase("rowid") || 
							column.trim().equalsIgnoreCase(pri_key) )
					{
						int rowid = rd_table.readInt();
						// compare rowid
						if(!Util.compare(rowid,operator,value))
						{
							// condition not satisfied, move to next record
							start_offset += 2;
							continue;
						}
						
					}
					else
					{
						// read columns count
						rd_table.seek(rcd_offset+4); // clmn_cnt
						int clmns_cnt = rd_table.read(); // 1 byte
						int clmn_offset = rcd_offset+5+clmns_cnt;
						
						for(int k=0;k<clmn_ordinal_pos-2;k++)
						{
							// read datatype
							rd_table.seek(rcd_offset+5+k);
							byte data_type = rd_table.readByte();
							int data_length = Util.getDataLength(data_type);
							clmn_offset += data_length;
						}
						// read the column value
						rd_table.seek(clmn_offset);
						byte[] rcd = new byte[clmn_data_length];
						rd_table.readFully(rcd);
						if(!Util.compare(rcd,clmn_data_type,operator,value))
						{
							// condition not satisfied, move to next record
							start_offset += 2;
							continue;
						}
					}
				
					rd_table.seek(rcd_offset);
					// read payload
					//rd_table.readShort(); // 2 bytes
					// read rowid
					int rowid = rd_table.readInt(); // 4 bytes
					if(clmn_list.contains("rowid") || clmn_list.contains(pri_key))
						System.out.print(rowid + "\t");
					
					// read columns count
					int clmns_cnt = rd_table.read(); // 1 byte
					// data_type offset
					int data_type_offset = rcd_offset+5; // rowid+clmns_cnt
									
					rcd_offset += 5+clmns_cnt; // where columns begin
					// read each column
					for(int t=0;t<clmns_cnt;t++)
					{
						int ordinal_pos = t+2;
						
						// read data type
						rd_table.seek(data_type_offset);
						byte data_type = rd_table.readByte();
						
						// get data_str and data_length
						String data_str = Util.getDataType(data_type).trim();
						int data_length = Util.getDataLength(data_type);
						
						if(ordinal_pos_list.contains(ordinal_pos))
						{
							int header_length = clmn_headers.get(ordinal_pos).trim().length();
							int clmn_length = Util.getDataLength(data_str);
							int print_length = Math.max(header_length,clmn_length);
									
							// read and print the column
							readAndPrintClmn(rd_table,rcd_offset,data_str,print_length);
						}
						
						rcd_offset += data_length;
						data_type_offset++;
					}
						
					System.out.println();				
					// advance offset to next record
					start_offset += 2;
				}			
			}
		
			rd_table.close();
						
		} catch (IOException e) {
			e.printStackTrace();
		}
				
	}

	// read all records with specified columns
	private void selectAllRecords(String table, String[] columns) {

		// check if the table exists in davisbase_tables
		DavisBaseTables davisbase_tables = new DavisBaseTables();
		if(!davisbase_tables.checkTblRcd(table))
		{
			System.out.println("The table doesnot exist: " + table);
			return;
		}
		
		// get column_name, data_type, ordinal_position, is_nullable from davisbase_columns
		DavisBaseColumns davisbase_columns = new DavisBaseColumns();
		LinkedList<DavisBaseColumnsCell> clmns = davisbase_columns.getRcds(table);
		
		if(clmns.isEmpty())
		{
			System.out.println("There are no columns in the table: " + table);
			return;
		}
		
		// get column ordinal_position
		LinkedList<Integer> ordinal_pos_list = new LinkedList<Integer>();
		
		// check if columns exists
		for(int i=0;i<columns.length;i++)
		{
			boolean clmn_exist = false;
			ListIterator<DavisBaseColumnsCell> itr = clmns.listIterator();
			while(itr.hasNext())
			{
				DavisBaseColumnsCell rcd = itr.next();
				if(rcd.getClmnName().equalsIgnoreCase(columns[i].trim()))
				{
					ordinal_pos_list.add(rcd.getOrdinalPos());
					clmn_exist = true;
				}
			}
			if(!clmn_exist)
			{
				System.out.println("There are no column: "+ columns[i].trim()+" in the table: " + table);
				return;
			}
		}
						
		String path = Util.findFilePath(table);
				
		try {
			RandomAccessFile rd_table = new RandomAccessFile(path,"r");
			int pg_cnt = (int)rd_table.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			// read record count
			for(int i=0;i<pg_cnt;i++)
			{
				// read record count
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt += rd_table.read();
			}
						
			// no records
			if(rcd_cnt==0)
			{
				System.out.println("There are no records in the table: " + table);
				Util.printHeaders(clmns,columns);
				rd_table.close();
				return;				
			}
			
			// print headers
			HashMap<Integer,String> clmn_headers = Util.printHeaders(clmns,columns);
			
			String pri_key = null;
			for(int i=0;i<clmns.size();i++)
			{
				// find the primary key column name
				if(clmns.get(i).getClmnKey().trim().equalsIgnoreCase(Util.PRIMARY_KEY_STR))
				{
					pri_key = clmns.get(i).getClmnName().trim();
				}
			}
			
			// columns list
			ArrayList<String> clmn_list = new ArrayList<String>(Arrays.asList(columns));
			
			for(int j=0;j<pg_cnt;j++)
			{
				rd_table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = rd_table.read();
				int start_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				// print records			
				for(int i=0;i<rcd_cnt;i++)
				{
					// move to record
					rd_table.seek(start_offset);
					int rcd_offset = rd_table.readShort(); 
					rd_table.seek(rcd_offset);
					
					// read payload
					rd_table.readShort(); // 2 bytes
					// read rowid
					int rowid = rd_table.readInt(); // 4 bytes
					if(clmn_list.contains("rowid") || clmn_list.contains(pri_key))
						System.out.print(rowid + "\t");
					
					// read columns count
					int clmns_cnt = rd_table.read(); // 1 byte
					// data_type offset
					int data_type_offset = rcd_offset+7; // payload+rowid+clmns_cnt
									
					rcd_offset += 7+clmns_cnt; // where columns begin
					// read each column
					for(int k=0;k<clmns_cnt;k++)
					{
						int ordinal_pos = k+2;
						
						// read data type
						rd_table.seek(data_type_offset);
						byte data_type = rd_table.readByte();
						
						// get data_str and data_length
						String data_str = Util.getDataType(data_type).trim();
						int data_length = Util.getDataLength(data_type);
						
						if(ordinal_pos_list.contains(ordinal_pos))
						{
							int header_length = clmn_headers.get(ordinal_pos).trim().length();
							int clmn_length = Util.getDataLength(data_str);
							int print_length = Math.max(header_length,clmn_length);
							
							// read and print the column
							readAndPrintClmn(rd_table,rcd_offset,data_str,print_length);
						}
						
						rcd_offset += data_length;
						data_type_offset++;
					}
						
					System.out.println();				
					// advance offset to next record
					start_offset += 2;
				}
				
			}
		
			rd_table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private boolean parseExit(String userCommand)
	{
		return true;
	}
	
	// read current location and print
	private void readAndPrintClmn(RandomAccessFile rd_table,int rcd_offset,String data_str,int print_length)
	{
	try{
		switch(data_str.trim().toUpperCase())
		{
			case "NULL":
				System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				//System.out.print("\t");
				break;
			case "TINYINT":
				rd_table.seek(rcd_offset);
				int tinyint = rd_table.readByte();
				if(tinyint == 0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", tinyint));
				System.out.print("\t");
				break;
			case "SMALLINT":
				rd_table.seek(rcd_offset);
				int smallint = rd_table.readShort();
				if(smallint == 0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", smallint));
				//System.out.print("\t");
				break;
			case "INT":
				rd_table.seek(rcd_offset);
				int int_rd = rd_table.readInt();
				if(int_rd == 0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", int_rd));
				//System.out.print("\t");
				break;
			case "BIGINT":
				rd_table.seek(rcd_offset);
				long long_rd = rd_table.readLong();
				if(long_rd == 0.0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", long_rd));
				//System.out.print("\t");
				break;
			case "REAL":
				rd_table.seek(rcd_offset);
				float float_rd = rd_table.readFloat();
				if(float_rd == 0.0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", float_rd));
				//System.out.print("\t");
				break;
			case "DOUBLE":
				rd_table.seek(rcd_offset);
				double double_rd = rd_table.readDouble();
				if(double_rd == 0.0)
					System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
				else
					System.out.print(String.format("%-"+print_length+"s\t", double_rd));
				//System.out.print("\t");
				break;
			case "DATETIME":
				rd_table.seek(rcd_offset);
				long datetime = rd_table.readLong();
	   			ZoneId zoneId = ZoneId.of ( "America/Chicago" );
	   			Instant x = Instant.ofEpochSecond ( datetime ); 
	   			ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( x, zoneId ); 
	   			zdt2.toLocalTime();
	   			String datetime_str = zdt2.toLocalDateTime().toString();
	   			if(datetime_str.equals("1969-12-31"))
	   				System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
	   			else
	   				System.out.print(String.format("%-"+print_length+"s\t", datetime_str));
				//System.out.print("\t");
				break;
			case "DATE":
					rd_table.seek(rcd_offset);
					long date = rd_table.readLong();
					ZoneId zoneId1 = ZoneId.of ( "America/Chicago" );
					Instant x1 = Instant.ofEpochSecond (date); 
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant ( x1, zoneId1 ); 
					String date_str =zdt3.toLocalDate().toString();
		   			if(date_str.equals("1969-12-31"))
		   				System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
		   			else
		   				System.out.print(String.format("%-"+print_length+"s\t", date_str));
				//System.out.print("\t");
				break;
			case "TEXT":
				rd_table.seek(rcd_offset);
				byte[] buffer3 = new byte[Util.TEXT_SIZE];
				rd_table.read(buffer3);
				String text = new String(buffer3).trim();
	   			if(text.equals(""))
	   				System.out.print(String.format("%-"+print_length+"s\t", "NULL"));
	   			else
	   				System.out.print(String.format("%-"+print_length+"s\t", text));
				//System.out.print("\t");
				break;
		}
	} catch (IOException e) {
			e.printStackTrace();
	}
	}
}
