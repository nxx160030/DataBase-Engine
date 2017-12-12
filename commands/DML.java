package commands;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import metaData.DavisBaseColumns;
import metaData.DavisBaseColumnsCell;
import metaData.DavisBaseTables;
import userData.TableColumn;
import userData.TableRcd;
import util.Util;

//parse insert,delete,update commands
public class DML {

	public boolean parseCmd(String userCommand) {

		boolean success = false;
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0).trim().toLowerCase()) {
		
			case "insert":
				success = insertTable(userCommand);
				if(!success)
					System.out.println("format example: insert into table_name [column_list] values (value1,value2);");
				break;
			case "update":
				success = updateTable(userCommand);
				if(!success)
					System.out.println("format example: update table_name set column_name = value [where column_name operator value];");
				break;
			case "delete":
				success = deleteTable(userCommand);
				if(!success)
					System.out.println("format example: delete from table_name where primary_key=value;");
				break;
		}
		
		return success;
		
	}

	private boolean deleteTable(String userCommand) {
		
		boolean success = false;
		
		userCommand = userCommand.replaceAll("=", " = ");
		
		if(!chckDltFrmt(userCommand))
			return success;
	
		String[] commandTokens = userCommand.split(" ");
		
		String table_name = commandTokens[2].trim();
		
		String dlt_key = commandTokens[6].replaceAll("'", "").trim();
		
		String path = Util.findFilePath(table_name);
		
		try{
		RandomAccessFile table = new RandomAccessFile(path,"rw");
		
		int pg_cnt = (int)table.length()/Util.pageSize;
		int current_pg = 0;
		int rcd_cnt = 0;
		int rcd_offset = -1;
		boolean found = false;

		for(int j=0;j<pg_cnt;j++)
		{
			boolean pg_found = false;
			table.seek(current_pg*Util.pageSize + 1);
			rcd_cnt = table.read();
			int index_offset = current_pg*Util.pageSize + 8;
			int dlt_cnt = 0;
			
			for(int i=0;i<rcd_cnt;i++)
			{
				table.seek(index_offset);
				rcd_offset = table.readShort();
				table.seek(rcd_offset); // payload + primary key
				int pay_load = table.readShort();
				int pri_key = table.readInt();
				if(!Util.compare(pri_key, "=", dlt_key))
				{
					index_offset += 2;
					continue;
				}
				
				// found the record,re-write the data to 0
				table.seek(rcd_offset);
				for(int k=0;k<pay_load+6;k++)
					table.write(0);
				
				// move all the indexs ahead by 2
				int index_start = index_offset + 2;
				int index_write = index_offset;
				
				for(int k=0;k<rcd_cnt-i;k++)
				{
					table.seek(index_start);
					int address = table.readShort();
					table.seek(index_write);
					table.writeShort(address);
					index_start += 2;
					index_write += 2;
				}
				
				dlt_cnt++;
							
				pg_found = true;
				found = true;
			}
			
			// update the record count
			if(pg_found)
			{
				table.seek(current_pg*Util.pageSize + 1);
				table.write(rcd_cnt-dlt_cnt);
			}
			
			// update record start
			table.seek(current_pg*Util.pageSize + 1);
			rcd_cnt = table.read();
			table.seek(current_pg*Util.pageSize + 8*rcd_cnt);
			int rcd_start = table.readShort();
			table.seek(2);
			table.writeShort(rcd_start);
			
			
			// update davisbase_tables
			DavisBaseTables davisTbl = new DavisBaseTables();
			davisTbl.updateTblCnt(table_name, -dlt_cnt);
			
			current_pg++;
		}
						
		table.close();
		
		if(!found)
		{
			System.out.println("no record found: " + dlt_key);
			return success;
		}
		
		}catch(IOException e)
		{
			e.printStackTrace();
		}
		
		success = true;
		return success;
	}

	private boolean updateTable(String userCommand) {
		
		boolean success = false;
		
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
		
		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
		
		if(!chckUpdtFrmt(userCommand))
			return success;
		
		String[] commandTokens = userCommand.split(" ");
		
		String table_name = commandTokens[1].trim();
		
		String column_name = commandTokens[3].trim();
		
		String value = commandTokens[5].replaceAll("'", "").trim();
		
		if(commandTokens.length == 6)
		{
			if(!updateAll(table_name,column_name,value))
				return success;
		}else
		{
			String cond_clmn = commandTokens[7].trim();
			String cond_opr = commandTokens[8].trim();
			String cond_val = commandTokens[9].replaceAll("'", "").trim();
			if(!updateCond(table_name,column_name,value,
					cond_clmn,cond_opr,cond_val))
				return success;
		}
		
		success = true;
		return success;
	}

	private boolean updateCond(String table_name, String column_name, 
			String value, String cond_clmn, String cond_opr,
			String cond_val) {
		
		boolean success = false;
		
		String path = Util.findFilePath(table_name);

		int rcd_cnt = -1;
		int rcd_offset = -1;
		int ordinal_pos = -1;
		int cond_ordinal_pos = -1;
		int index_offset = 8;
		String data_type = null;
		String cond_data_type = null;
		int cond_data_length = -1;
		byte clmn_type_byte;
		boolean is_pri_key = false;
				
		RandomAccessFile table;
		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		
		// find ordinal_pos and data_type
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell columns = tbl_clmns.get(i);
			if(columns.getClmnName().trim().equalsIgnoreCase(column_name.trim()))
			{
				ordinal_pos = columns.getOrdinalPos();
				
				if(columns.getClmnKey().trim().equalsIgnoreCase(Util.PRIMARY_KEY_STR))
					is_pri_key = true;
			}
			
			if(columns.getClmnName().trim().equalsIgnoreCase(cond_clmn.trim()))
			{
				cond_ordinal_pos = columns.getOrdinalPos();
				cond_data_type = columns.getDataType().trim();
				cond_data_length = Util.getDataLength(cond_data_type);
			}

		}
		
		try {
			table = new RandomAccessFile(path,"rw");
			int pg_cnt = (int)table.length()/Util.pageSize;
			int current_pg = 0;
			
			for(int j=0;j<pg_cnt;j++)
			{
				table.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = table.read();
				index_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				for(int i=0;i<rcd_cnt;i++)
				{
					table.seek(index_offset);
					rcd_offset = (int)table.readShort();
					table.seek(rcd_offset+6); // payload + primary key
					int clmn_cnt = table.read();
					int clmn_offset = rcd_offset+7+clmn_cnt;
					int cond_clmn_offset = rcd_offset+7+clmn_cnt;
					
					// compare condition				
					// if condition is primary key
					if(is_pri_key)
					{
						table.seek(rcd_offset+2);
						int pri_key = table.readInt();
						// compare primary key
						if(!Util.compare(pri_key,cond_opr,cond_val))
						{
							// condition not satisfied, move to next record
							index_offset += 2;
							continue;
						}
						
					}else
					{
						table.seek(rcd_offset+7);
						// condition is not primary key
						for(int k=2;k<cond_ordinal_pos;k++)
						{
							byte data_type_byte = table.readByte();
							int data_length = Util.getDataLength(data_type_byte);
							cond_clmn_offset += data_length;
						}
						
						table.seek(cond_clmn_offset);
						byte[] cond_value = new byte[cond_data_length];
						table.readFully(cond_value);
						if(!Util.compare(cond_value,cond_data_type,cond_opr,cond_val))
						{
							// condition not satisfied, move to next record
							index_offset += 2;
							continue;
						}
					}
		
						// condition satisifed, update the record
						// get data_type_byte
						table.seek(rcd_offset+6+ordinal_pos-1);
						clmn_type_byte = table.readByte();
						data_type = Util.getDataType(clmn_type_byte).trim();
						// move to column offset 
						table.seek(rcd_offset+7);						
						for(int k=2;k<ordinal_pos;k++)
						{
							byte data_type_byte = table.readByte();
							int data_length = Util.getDataLength(data_type_byte);
							clmn_offset += data_length;
						}
						
						// move to column offset 
						if(!writeToClmn(table,clmn_offset,data_type,value))
							return success;
										
						index_offset += 2;		
					}						
				
			}			
			
			table.close();
			
		}catch (IOException e) {
			e.printStackTrace();
		}
				
		success = true;
		return success;		
	}

	private boolean updateAll(String table_name, String column_name, String value) {
		
		boolean success = false;
		
		String path = Util.findFilePath(table_name);

		int rcd_cnt = -1;
		int rcd_offset = -1;
		int ordinal_pos = -1;
		int index_offset = 8;
		String data_type = null;
		byte clmn_type_byte;
				
		RandomAccessFile table;
		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		
		// find ordinal_pos and data_type
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell columns = tbl_clmns.get(i);
			if(columns.getClmnName().trim().equalsIgnoreCase(column_name))
			{
				ordinal_pos = columns.getOrdinalPos();
			}

		}
		
		try {
			table = new RandomAccessFile(path,"rw");
			
			int pg_cnt = (int)table.length()/Util.pageSize;
			int current_pg = 0;
			
			for(int j=0;j<pg_cnt;j++)
			{
				int rcd_cnt_index = current_pg*Util.pageSize + 1;
				table.seek(rcd_cnt_index);
				rcd_cnt = (int)table.readByte();
				index_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				for(int i=0;i<rcd_cnt;i++)
				{
					table.seek(index_offset);
					rcd_offset = table.readShort();
					table.seek(rcd_offset+6); // payload + primary key
					int clmn_cnt = table.read();
					int clmn_offset = rcd_offset+7+clmn_cnt;
					
					for(int k=2;k<ordinal_pos;k++)
					{
						byte data_type_byte = table.readByte();
						int data_length = Util.getDataLength(data_type_byte);
						clmn_offset += data_length;
					}
					
					// get data_type_byte
					clmn_type_byte = table.readByte();
					data_type = Util.getDataType(clmn_type_byte).trim();
					
					// move to column offset 
					table.seek(clmn_offset);
					if(!writeToClmn(table,clmn_offset,data_type,value))
						return success;
									
					index_offset += 2;				
				}
				
			}			
			
			table.close();
			
		}catch (IOException e) {
			e.printStackTrace();
		}
				
		success = true;
		return success;			
	}

	private boolean writeToClmn(RandomAccessFile table, int clmn_offset, String data_type, String value) {

		boolean success = false;
		try {
				table.seek(clmn_offset);
			
				switch(data_type.trim().toUpperCase())
				{
				case "TINYINT_NULL": // TINYINT_NULL
					table.write(0);
					break;
				case "SMALLINT_NULL": // SMALLINT_NULL
					table.writeShort(0);
					break;
				case "INT_REAL_NULL": // INT_REAL_NULL
					table.writeInt(0);
					break;
				case "DOUBLE_DATETIME_DATE_NULL": // DOUBLE_DATETIME_DATE_NULL
					table.writeDouble(0);
					break;
				case "TINYINT": // TINYINT
					table.write(Integer.parseInt(value));
					break;
				case "SMALLINT": // SMALLINT
					table.writeShort(Integer.parseInt(value));
					break;
				case "INT": // INT
					table.writeInt(Integer.parseInt(value));
					break;
				case "BIGINT": // BIGINT
					table.writeLong(Long.parseLong(value));
					break;
				case "REAL": // REAL
					float real_value = Float.parseFloat(value);
					table.writeFloat(real_value);
					break;
				case "DOUBLE": // DOUBLE
					table.writeDouble(Double.parseDouble(value));
					break;
				case "DATETIME": // DATETIME
					String[] dateTime = value.split("_");
					String[] date1 = dateTime[0].split("-");
					String[] time1 = dateTime[1].split(":");
					ZoneId zoneId1 = ZoneId.of ( "America/Chicago" );
					ZonedDateTime zdt1 = ZonedDateTime.of(Integer.parseInt(date1[0]),Integer.parseInt(date1[1]),
							Integer.parseInt(date1[2]),Integer.parseInt(time1[0]), Integer.parseInt(time1[1]), 
							Integer.parseInt(time1[2]),1234,zoneId1);					 
					long epochSeconds1 = zdt1.toInstant().toEpochMilli() / 1000;
					table.writeLong (epochSeconds1);
					break;
				case "DATE": // DATE
					String[] date = value.split("-");
					
					ZoneId zoneId = ZoneId.of ( "America/Chicago" );
					ZonedDateTime zdt = ZonedDateTime.of(Integer.parseInt(date[0]),
							Integer.parseInt(date[1]),Integer.parseInt(date[2]),0, 0, 0,1234,zoneId);						 
					long epochSeconds = zdt.toInstant().toEpochMilli()/1000;
					table.writeLong (epochSeconds);
					break;
				case "TEXT": // TEXT
					table.writeBytes(value);
					break;
				default:
					System.out.println("Cannot parse the data type: " + data_type);
					return success;
				}			
		} catch (IOException e) {
			e.printStackTrace();
		}		
		success = true;	
		return success;
	}

	private boolean insertTable(String userCommand) {
		
		boolean success = false;

		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
		
		if(!chckInsrtFrmt(userCommand))
			return success;
		
		String[] commandTokens = userCommand.split(" ");
		
		String table_name = commandTokens[2].trim();
		
		if(!Util.chckTblExist(table_name))
			return success;
		
		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		
		ArrayList<String> clmn_list;
		ArrayList<String> vl_list;
		int pri_key = -1;

		if(commandTokens.length==6)
		{
			clmn_list = new ArrayList<String>(Arrays.asList(
					commandTokens[3].toLowerCase().replace("(", "").replace(")", "").replaceAll(" ", "").split(",")));
			
			vl_list = new ArrayList<String>(Arrays.asList(
					commandTokens[5].toLowerCase().replace("(", "").replace(")", "").replaceAll("'", "").replaceAll(" ", "").split(",")));
			
		}else
		{			
			clmn_list = new ArrayList<String>();
			
			vl_list = new ArrayList<String>(Arrays.asList(
					commandTokens[4].toLowerCase().replace("(", "").replace(")", "").replaceAll("'", "").replaceAll(" ", "").split(",")));
			
/*			if(tbl_clmns.size() > vl_list.size())
			{
				if(!Util.chckNullable(tbl_clmns,vl_list.size()))
					return success;
			}*/		
			
			for(int i=0;i<tbl_clmns.size();i++)
			{
				DavisBaseColumnsCell clmn =  tbl_clmns.get(i);
				String clmn_name = clmn.getClmnName().trim().toLowerCase();
				clmn_list.add(clmn_name);
			}
				
		}
		
		if(!Util.chckNullable(tbl_clmns,vl_list.size()))
			return success;
		
		pri_key = Integer.parseInt(vl_list.get(0));
		
		if(Util.chckDnplct(table_name, pri_key))
			return success;

		String path = Util.findFilePath(table_name);

		TableRcd tblRcd = new TableRcd(tbl_clmns,clmn_list,vl_list);
		int rcd_size = tblRcd.getRcdSize();
		int rcd_cnt = -1;
		int new_offset = -1;
				
		RandomAccessFile table;
		try {
			table = new RandomAccessFile(path,"rw");
			int current_pg = (int)table.length()/Util.pageSize - 1;
			
			if(Util.chckPgIsFull(table_name, rcd_size, current_pg))
			{
				current_pg = Util.splitPg(table_name, current_pg);
				new_offset = (current_pg+1)*Util.pageSize - rcd_size;
			}else
			{
				table.seek(current_pg*Util.pageSize + 2);
				int rcd_offset = table.readShort();
				if(rcd_offset == 0)
					new_offset = (current_pg+1)*Util.pageSize - rcd_size;
				else
					new_offset = rcd_offset - rcd_size;
			}
			
			// write to the record
			if(!writeToRcd(table,tblRcd,new_offset,tbl_clmns))
			{
				table.close();
				return success;
			}
			
			// update record count
			table.seek(current_pg*Util.pageSize + 1);
			rcd_cnt = table.read();
			table.seek(current_pg*Util.pageSize + 1);
			table.writeByte(rcd_cnt+1);
			// update record index
			table.seek(current_pg*Util.pageSize + 8+rcd_cnt*2);
			table.writeShort(new_offset);
			// update record start
			table.seek(current_pg*Util.pageSize + 2);
			table.writeShort(new_offset);
								
			table.close();
			
			// update davisbase_tables
			DavisBaseTables davisTbl = new DavisBaseTables();
			davisTbl.updateTblCnt(table_name, +1);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		success = true;
		return success;
	}

	private boolean writeToRcd(RandomAccessFile table, TableRcd tblRcd,
			int new_offset,LinkedList<DavisBaseColumnsCell> tbl_clmns) {

		boolean success = false;
		
		try {
			table.seek(new_offset);
			table.writeShort(tblRcd.getPayload()); // payload
			table.writeInt(tblRcd.getPriKey()); // primary key
			
			ArrayList<TableColumn> clmn_list = tblRcd.getClmnList();
			ArrayList<String> vl_list = tblRcd.getValueList();
			int clmn_cnt = Math.max(clmn_list.size(), tbl_clmns.size())-1;
			table.write(clmn_cnt); // column count
			
			int clmn_byte_offset = new_offset + 6 + 1; // write the data_type_byte
			
			int rcd_offset = new_offset + 6 + 1 + clmn_cnt; // where record column begins
			
			for(int i=1;i<=clmn_cnt;i++)
			{
				TableColumn clmn = clmn_list.get(i);
				String data_type = clmn.getDtTp();
				byte data_byte = Util.getDataByte(data_type.trim().toUpperCase());
				
				// column byte
				table.seek(clmn_byte_offset++);
				table.writeByte(data_byte);
				// record 
				table.seek(rcd_offset);
				String value=vl_list.get(i).trim();
				
				if(value.equalsIgnoreCase(Util.NULL_STR))
				{
					data_type = clmn.getDtTp();
					data_byte = Util.getDataByte(data_type.trim().toUpperCase());
					rcd_offset += Util.getDataLength(data_byte);

				}else
				{
					switch(data_type.trim().toUpperCase())
					{
					case "TINYINT": // TINYINT
						int tinyint_value = Integer.parseInt(value);
						table.write(tinyint_value);
						break;
					case "SMALLINT": // SMALLINT
						int smallint_value = Integer.parseInt(value);
						table.writeShort(smallint_value);
						break;
					case "INT": // INT
						int int_value = Integer.parseInt(value);
						table.writeInt(int_value);
						break;
					case "BIGINT": // BIGINT
						long long_value = Long.parseLong(value);
						table.writeLong(long_value);
						break;
					case "REAL": // REAL
						float real_value = Float.parseFloat(value);
						table.writeFloat(real_value);
						break;
					case "DOUBLE": // DOUBLE
						double rcd_value = Double.parseDouble(value);
						table.writeDouble(rcd_value);
						break;
					case "DATETIME": // DATETIME
						String[] dateTime = value.split("_");
						String[] date1 = dateTime[0].split("-");
						String[] time1 = dateTime[1].split(":");
						ZoneId zoneId1 = ZoneId.of ( "America/Chicago" );
						ZonedDateTime zdt1 = ZonedDateTime.of(Integer.parseInt(date1[0]),Integer.parseInt(date1[1]),
								Integer.parseInt(date1[2]),Integer.parseInt(time1[0]), Integer.parseInt(time1[1]), 
								Integer.parseInt(time1[2]),1234,zoneId1);					 
						long epochSeconds1 = zdt1.toInstant().toEpochMilli() / 1000;
						table.writeLong (epochSeconds1);
						break;
					case "DATE": // DATE
						String[] date = value.split("-");
						
						ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						ZonedDateTime zdt = ZonedDateTime.of(Integer.parseInt(date[0]),
								Integer.parseInt(date[1]),Integer.parseInt(date[2]),0, 0, 0,1234,zoneId);						 
						long epochSeconds = zdt.toInstant().toEpochMilli()/1000;
						table.writeLong (epochSeconds);
						break;
					case "TEXT": // TEXT
						table.writeBytes(value);
						break;
					default:
						System.out.println("Cannot parse the data type: " + data_type);
						return success;
					}
					
					rcd_offset += clmn.getClmnLngth();
				}
					
			}
						
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		success = true;
		return success;
	}

	private boolean chckInsrtFrmt(String userCommand) {
		
		boolean success = false;
		
		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
		
		String[] commandTokens = userCommand.split(" ");
		
		if(commandTokens.length != 6 && commandTokens.length != 5)
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		String table_name = commandTokens[2].trim();
		if(!Util.chckTblExist(table_name))
		{
			System.out.println("table doesnot exist: \"" + table_name + "\"");
			return success;
		}
			
		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		
		ArrayList<String> clmn_list = null;
		ArrayList<String> vl_list = null;
		
		if(commandTokens.length == 6)
		{
			if(!commandTokens[1].trim().equalsIgnoreCase("into")
					|| !commandTokens[4].trim().equalsIgnoreCase("values"))
			{
				System.out.println("wrong command format: \"" + userCommand + "\"");
				return success;
			}
			
			if(!commandTokens[3].startsWith("(")
				|| !commandTokens[3].endsWith(""))
			{
				System.out.println("wrong command format: \"" + commandTokens[3] + "\"");
				return success;
			}
			
			if(!commandTokens[5].startsWith("(")
					|| !commandTokens[5].endsWith(""))
				{
					System.out.println("wrong command format: \"" + commandTokens[5] + "\"");
					return success;
				}
					
			clmn_list = new ArrayList<String>(Arrays.asList(
					commandTokens[3].toLowerCase().replace("(", "").replace(")", "").split(",")));
			
			vl_list = new ArrayList<String>(Arrays.asList(
					commandTokens[5].toLowerCase().replace("(", "").replace(")", "").split(",")));
			
			if(clmn_list.size() != vl_list.size())
			{
				System.out.println("columns missing"+ "\"");
				return success;
			}
			
		}else
		{
			if(!commandTokens[1].trim().equalsIgnoreCase("into")
					|| !commandTokens[3].trim().equalsIgnoreCase("values"))
			{
				System.out.println("wrong command format: \"" + userCommand + "\"");
				return success;
			}
			
			if(!commandTokens[4].startsWith("(")
					|| !commandTokens[4].endsWith(""))
				{
					System.out.println("wrong command format: \"" + commandTokens[4] + "\"");
					return success;
				}
				
			clmn_list = new ArrayList<String>();

			for(int i=0;i<tbl_clmns.size();i++)
			{
				DavisBaseColumnsCell clmn =  tbl_clmns.get(i);
				String clmn_name = clmn.getClmnName().trim().toLowerCase();
				clmn_list.add(clmn_name);
			}
			
			vl_list = new ArrayList<String>(Arrays.asList(
					commandTokens[4].toLowerCase().replace("(", "").replace(")", "").split(",")));
		}
		
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell clmn =  tbl_clmns.get(i);
			String clmn_name = clmn.getClmnName().trim().toLowerCase();
			String clmn_pri = clmn.getClmnKey().trim().toLowerCase();
			
			// check primary key 
			if(clmn_pri.equalsIgnoreCase(Util.PRIMARY_KEY_STR))
			{
				if(!clmn_list.contains(clmn_name.trim()))
				{
					System.out.println("primary key is mandatory: \"" + clmn_name + "\"");
					return success;
				}else
				{
					if(vl_list.get(i).trim().equalsIgnoreCase(Util.NULL_STR))
					{
						System.out.println("primary key cannot be null: \"" + clmn_name + "\"");
						return success;
					}
				}
			}			
		}	
		
		success = true;
		return success;
	}

	private boolean chckUpdtFrmt(String userCommand) {
		
		boolean success = false;
		
		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
		
		String[] commandTokens = userCommand.split(" ");
		
		if(commandTokens.length != 6 && commandTokens.length != 10)
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		if(!commandTokens[2].trim().equalsIgnoreCase("SET")
				|| !commandTokens[4].trim().equalsIgnoreCase("="))
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		String table_name = commandTokens[1].trim();
		if(!Util.chckTblExist(table_name))
		{
			System.out.println("table doesnot exist: \"" + table_name + "\"");
			return success;
		}

		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		List<String> clmn_list = new ArrayList<String>();
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell clmns = tbl_clmns.get(i);
			String clmns_name = clmns.getClmnName().trim();
			clmn_list.add(clmns_name);
		}
		
		if(commandTokens.length == 10)
		{
			if(!commandTokens[6].trim().equalsIgnoreCase("WHERE"))
			{
				System.out.println("wrong command format: \"" + commandTokens[6] + "\"");
				return success;
			}
			
			String clmn_name = commandTokens[7].trim();
			String clmn_opr = commandTokens[8].trim();
			
			if(!clmn_list.contains(clmn_name))
			{
				System.out.println("no such column: \"" + commandTokens[7] + "\"");
				return success;
			}
					
			if(!Util.OPERATORS_LIST.contains(clmn_opr))
			{
				System.out.println("wrong command format: \"" + commandTokens[8] + "\"");
				return success;
			}
			
			for(int i=0;i<tbl_clmns.size();i++)
			{
				DavisBaseColumnsCell clmns = tbl_clmns.get(i);
				String data_type = clmns.getDataType().trim();
				String clmns_name = clmns.getClmnName().trim();
				
				if(clmn_name.equalsIgnoreCase(clmns_name))
				{
					if(data_type.equalsIgnoreCase(Util.TEXT_STR) &&
							(!clmn_opr.equalsIgnoreCase("=") && 
									!clmn_opr.equalsIgnoreCase("!=") ))
					{
						System.out.println("String column operator can only be '=' or '!=' \"" + commandTokens[8] + "\"");
						return success;
					}
				}					
			}
		}
		
		String column_name = commandTokens[3].trim();
		if(!clmn_list.contains(column_name))
		{
			System.out.println("no such column: \"" + commandTokens[3] + "\"");
			return success;
		}
		
		// primary key column cannot be updated		
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell clmns = tbl_clmns.get(i);
			String pri_key = clmns.getClmnKey().trim();
			String clmns_name = clmns.getClmnName().trim();
			
			if(clmns_name.equalsIgnoreCase(column_name)
					&& pri_key.equalsIgnoreCase(Util.PRIMARY_KEY_STR))
			{
				System.out.println("primary key cannot be set \"" + commandTokens[3] + "\"");
				return success;
			}
		}
		
		success = true;
		return success;
	}
	
	private boolean chckDltFrmt(String userCommand) {
		
		boolean success = false;
		
		String[] commandTokens = userCommand.split(" ");
		
		if(commandTokens.length != 7)
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		if(!commandTokens[1].trim().equalsIgnoreCase("from")
				||!commandTokens[3].trim().equalsIgnoreCase("where")
				||!commandTokens[5].trim().equalsIgnoreCase("="))
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		String table_name = commandTokens[2].trim();
		if(!Util.chckTblExist(table_name))
		{
			System.out.println("table doesnot exist: \"" + table_name + "\"");
			return success;
		}
		
		// check primary key valid
		String cnd_clmn = commandTokens[4].trim();
		String pri_key = null;
		LinkedList<DavisBaseColumnsCell> tbl_clmns = new DavisBaseColumns().getRcds(table_name);
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell clmns = tbl_clmns.get(i);
			if(clmns.getClmnName().trim().equalsIgnoreCase(cnd_clmn))
			{
				pri_key = clmns.getClmnKey().trim();
				break;
			}
		}
		if(!pri_key.equalsIgnoreCase(Util.PRIMARY_KEY_STR))
		{
			System.out.println("condition must be primary key: \"" + cnd_clmn + "\"");
			return success;
		}
		
		success = true;
		return success;
	}
}
