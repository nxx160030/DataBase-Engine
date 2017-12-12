package metaData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import util.Util;

public class DavisBaseColumns {

	private RandomAccessFile davisBaseClmnFile;
	
	public void init() {
		
		File data = new File("data");			
		if(!data.exists()||!data.isDirectory())
		{
			data.mkdir();
		}
		
		File catalog = new File("data/catalog");
		if(!catalog.exists()||!catalog.isDirectory())
		{
			catalog.mkdir();
		}
		
		File table = new File(Util.davisBaseClmnPath);
		// if davisbase_tables.tbl already exists, return 
		if(table.exists()&&!table.isDirectory())
			return;
		
		// davisbase_columns.tbl doesnot exist, create it
		createDavisBaseClmn();
	
	}
	
	// init the table davisbase_columns.tbl
	private void createDavisBaseClmn()
	{
		try {
			davisBaseClmnFile = new RandomAccessFile(Util.davisBaseClmnPath,"rw");
			
			int rcdLocation = 0;
			int currentPg = 0;
			int pgLocation = Util.pageSize*currentPg;	
			
			// update davisbase_tables.tbl
			DavisBaseTables davisBaseTblFile = new DavisBaseTables();
			DavisBaseTablesCell record = new DavisBaseTablesCell(Util.davisbase_columns,10,new DavisBaseColumnsCell().getSize());
			if(!davisBaseTblFile.writeIntoDavisBaseTables(record,currentPg))
			{
				throw new Exception("failed to update " + Util.davisbase_tables + " " + Util.davisbase_columns);
			}
			
			davisBaseClmnFile.setLength(Util.pageSize);		
			
			// 1st leaf page
			// define the header
			davisBaseClmnFile.seek(pgLocation+rcdLocation);
			// a leaf page
			davisBaseClmnFile.write(0x0D); // 1 byte
			// no. of records
			int rcds_cnt = 0;
			davisBaseClmnFile.writeByte(rcds_cnt); // 1 byte
			
			// define start of content
			int rcd_size = new DavisBaseColumnsCell().getSize();
			int rcdPosition = Util.pageSize-rcd_size; 
			davisBaseClmnFile.writeShort(rcdPosition); // 2 bytes
			
			// the right most page
			davisBaseClmnFile.writeInt(-1); // 4 bytes
			
			// add the record 1-4, which is davisbase_tables.tbl, to the end of page
			int payload_size = new DavisBaseColumnsCell().getPayloadSize();	
			int rowid = 1;
			int ordinal_pos = 1;
			String is_nullable = "NO";
						
			LinkedHashMap<String,String> davisBaseTableClumns = new DavisBaseTables().getClmns();
			for(Map.Entry<String, String> entry: davisBaseTableClumns.entrySet())
			{
				String column_name = entry.getKey();
				String data_type = entry.getValue();
				DavisBaseColumnsCell clmn;
				
				if(column_name.equalsIgnoreCase("rowid"))
				{
					 clmn = new DavisBaseColumnsCell(rowid, Util.davisbase_tables,
							 column_name,  data_type,  
							 ordinal_pos,  is_nullable, Util.PRIMARY_KEY_STR);
				}else
				{
					 clmn = new DavisBaseColumnsCell(rowid, Util.davisbase_tables,
							 column_name,  data_type,  
							 ordinal_pos,  is_nullable, Util.NULL_STR);
				}

				if(!Util.chckPgIsFull(Util.davisbase_columns,rcd_size,currentPg))
				{
					if(!writeIntoDavisBaseColumns(currentPg,clmn,rcdPosition,ordinal_pos,payload_size))
					{
						throw new Exception("failed to update " + Util.davisbase_columns + " " + column_name);
					}										
				}else
				{
					currentPg = Util.splitPg(Util.davisbase_columns,currentPg);
					
					rcdPosition = (currentPg+1)*Util.pageSize - rcd_size;
							
					if(!writeIntoDavisBaseColumns(currentPg,clmn,rcdPosition,ordinal_pos,payload_size))
					{
						throw new Exception("failed to update " + Util.davisbase_columns + " " + column_name);
					}
				}
				
				rowid++;
				ordinal_pos++;
				rcdPosition -= rcd_size;
			}
			
			// add the records 5-11, which is davisbase_columns.tbl itself
			ordinal_pos = 1;
			HashMap<String,String> davisBaseClmnClumns = getClmns();
			for(Map.Entry<String, String> entry: davisBaseClmnClumns.entrySet())
			{
				String column_name = entry.getKey();
				String data_type = entry.getValue();
				DavisBaseColumnsCell clmn;
				
				if(column_name.equalsIgnoreCase("rowid"))
				{
					 clmn = new DavisBaseColumnsCell(rowid, Util.davisbase_columns,
							 column_name,  data_type,  
							 ordinal_pos,  is_nullable, Util.PRIMARY_KEY_STR);
				}else
				{
					 clmn = new DavisBaseColumnsCell(rowid, Util.davisbase_columns,
							 column_name,  data_type,  
							 ordinal_pos,  is_nullable, Util.NULL_STR);
				}
				
				if(!Util.chckPgIsFull(Util.davisbase_columns,rcd_size,currentPg))
				{
					if(!writeIntoDavisBaseColumns(currentPg,clmn,rcdPosition,ordinal_pos,payload_size))
					{
						throw new Exception("failed to update " + Util.davisbase_columns + " " + column_name);
					}										
				}else
				{
					currentPg = Util.splitPg(Util.davisbase_columns,currentPg);
					
					rcdPosition = (currentPg+1)*Util.pageSize - rcd_size;					
							
					if(!writeIntoDavisBaseColumns(currentPg,clmn,rcdPosition,ordinal_pos,payload_size))
					{
						throw new Exception("failed to update " + Util.davisbase_columns + " " + column_name);
					}
				}
				
				rowid++;
				ordinal_pos++;
				rcdPosition -= rcd_size;

			}
			
			davisBaseClmnFile.close();
		} catch (Exception e) {			
			e.printStackTrace();
		}
	}

	// get the davisbase_columns.tbl
	public RandomAccessFile getDavisBaseClmns()
	{
		try {
			davisBaseClmnFile = new RandomAccessFile(Util.davisBaseClmnPath,"rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return davisBaseClmnFile;
	}
	
	// write into table davisbase_tables.tbl
	public boolean writeIntoDavisBaseColumns(int currentPg,
			DavisBaseColumnsCell record, 
			int rcdPosition, int ordinal_pos,int payload)
	{
		boolean success = false;
		
		int rcd_cnt = getRcdCnt(currentPg);
		int start_position = currentPg*Util.pageSize + 8+rcd_cnt*2; // the very beginning position of start positions
		
		try {
			davisBaseClmnFile = getDavisBaseClmns();
			davisBaseClmnFile.seek(start_position);

			// write the start position
			davisBaseClmnFile.writeShort(rcdPosition); // 2 bytes
		
			// write the record
			davisBaseClmnFile.seek(rcdPosition);
			
			// payload
			davisBaseClmnFile.writeShort(payload); // 2 bytes
			// row id
			davisBaseClmnFile.writeInt(record.getRowid()); // 4 bytes
			
			// columns count = 6, table_name,column_name,data_type,ordinal_position,is_nullable,column_key
			davisBaseClmnFile.writeByte(record.getClmnCnt()); // 1 byte
			
			// table_name_byte
			davisBaseClmnFile.write(record.getTblNameByte()); // 1 byte
			
			// column_name_byte
			davisBaseClmnFile.write(record.getClmnNameByte()); // 1 byte
			
			// data_type_byte
			davisBaseClmnFile.write(record.getDataTypeByte()); // 1 byte
			
			// ordinal_position_byte
			davisBaseClmnFile.write(record.getOrdinalPosByte()); // 1 byte
			
			// is_nullable_byte
			davisBaseClmnFile.write(record.getNullableByte()); // 1 byte
			
			// column_key_byte
			davisBaseClmnFile.write(record.getClmnKeyByte()); // 1 byte
			
			// table name
			davisBaseClmnFile.writeBytes(record.getTableName()); // 20 bytes
			// column name
			int next = 2+4+7*Util.TINYINT_SIZE+Util.TEXT_SIZE;
			davisBaseClmnFile.seek(rcdPosition+next);
			davisBaseClmnFile.writeBytes(record.getClmnName()); // 20 bytes
			
			// data_type
			next += Util.TEXT_SIZE;
			davisBaseClmnFile.seek(rcdPosition+next);
			davisBaseClmnFile.writeBytes(record.getDataType()); // 20 bytes
			
			// ordinal_position
			next += Util.TEXT_SIZE;
			davisBaseClmnFile.seek(rcdPosition+next);
			davisBaseClmnFile.writeByte(ordinal_pos); // 1 byte
			
			// is_nullable
			next += Util.TINYINT_SIZE;
			davisBaseClmnFile.seek(rcdPosition+next);
			davisBaseClmnFile.writeBytes(record.getIsNullable()); // 20 bytes
			
			// column_key
			next += Util.TEXT_SIZE;
			davisBaseClmnFile.seek(rcdPosition+next);
			davisBaseClmnFile.writeBytes(record.getClmnKey()); // 20 bytes
						
			// update record count
			int rcd_cnt_position = currentPg*Util.pageSize + 1;
			davisBaseClmnFile.seek(rcd_cnt_position);
			int new_rcd_cnt = rcd_cnt+1;
			davisBaseClmnFile.writeByte(new_rcd_cnt);
			
			// update content start
			int content_start_position = currentPg*Util.pageSize + 2;
			davisBaseClmnFile.seek(content_start_position);
			davisBaseClmnFile.writeShort(rcdPosition);
			
			davisBaseClmnFile.close();
			
		} catch (IOException e) {

			e.printStackTrace();
		}

		
		success = true;
		
		return success;
	}
	
	// get current page records count
	public int getRcdCnt(int current_pg)
	{
		int rcd_cnt_position = current_pg*Util.pageSize + 1; 
		int rcd_cnt = -1;
		try {
			davisBaseClmnFile = getDavisBaseClmns();
			davisBaseClmnFile.seek(rcd_cnt_position);
			rcd_cnt = davisBaseClmnFile.read();
			davisBaseClmnFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rcd_cnt;
	}

	public LinkedHashMap<String,String> getClmns()
	{
		// define column_name and data_type
		LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
		map.put("rowid", "INT");
		map.put("table_name", "TEXT");
		map.put("column_name", "TEXT");
		map.put("data_type", "TEXT");
		map.put("ordinal_position", "TINYINT");
		map.put("is_nullable", "TEXT");
		map.put("column_key", "TEXT");
		
		return map;
	}

	public LinkedList<DavisBaseColumnsCell> getRcds(String table) {
		
		LinkedList<DavisBaseColumnsCell> rcds = new LinkedList<DavisBaseColumnsCell>();
		
		davisBaseClmnFile = getDavisBaseClmns();
		
		try {		
			// read record count
			int pg_cnt = (int)davisBaseClmnFile.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			for(int j=0;j<pg_cnt;j++)
			{
				davisBaseClmnFile.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = davisBaseClmnFile.read();
				int start_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				// read records		
				for(int i=0;i<rcd_cnt;i++)
				{
					// move to record
					davisBaseClmnFile.seek(start_offset);
					int rcd_offset = davisBaseClmnFile.readShort();
					davisBaseClmnFile.seek(rcd_offset);
					int current_set = rcd_offset;
					
					// read payload
					current_set += 2; // 2 bytes
					davisBaseClmnFile.seek(current_set);
					// read rowid
					int rowid = davisBaseClmnFile.readInt(); // 4 bytes

					// move to table_name
					current_set += 11; // rowid,columns count, 6 data_types
					davisBaseClmnFile.seek(current_set);
					byte[] buffer = new byte[Util.TEXT_SIZE];
					// read table_name
					davisBaseClmnFile.read(buffer);
					String table_name = new String(buffer).trim();
					
					// if table_name is the table, create a DavisBaseColumnsCell,else continue
					if(table_name.equalsIgnoreCase(table.trim()))
					{
						// read column_name
						buffer = new byte[Util.TEXT_SIZE];
						davisBaseClmnFile.read(buffer);
						String column_name = new String(buffer);
						
						// read data_type
						buffer = new byte[Util.TEXT_SIZE];
						davisBaseClmnFile.read(buffer);
						String data_type = new String(buffer);
						
						// read ordinal_position
						int ordinal_position = davisBaseClmnFile.read();
						
						// read is_nullable
						buffer = new byte[Util.TEXT_SIZE];
						davisBaseClmnFile.read(buffer);
						String is_nullable = new String(buffer);
						
						// read column_key
						buffer = new byte[Util.TEXT_SIZE];
						davisBaseClmnFile.read(buffer);
						String column_key = new String(buffer);
						
						// new a DavisBaseColumnsCell and put into rcds list
						rcds.add(new DavisBaseColumnsCell(rowid,table_name.trim().toLowerCase(),
								column_name.trim().toLowerCase(),data_type.trim().toLowerCase(),
								ordinal_position,is_nullable.trim().toLowerCase(),column_key.trim().toLowerCase()));
					}
									
					// advance offset to next record
					start_offset += 2;
				}
			}

			davisBaseClmnFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return rcds;
	}
	
	public boolean deleteTblRcds(String table_name)
	{
		boolean success = false;
		davisBaseClmnFile = getDavisBaseClmns();
		
		try {
			// read record count
			int pg_cnt = (int)davisBaseClmnFile.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			// find table_name and delete index header
			int rcd_offset = -1;
			int index_offset = 8;
			int front_cnt = 0;
			int after_cnt = 0;
			int clmn_cnt = 0;
			int index_start = 0;
			int index_write = 0;
						
			for(int j=0;j<pg_cnt;j++)
			{
				index_offset = current_pg*Util.pageSize + 8;
				boolean pg_found = false;
				front_cnt = 0;
				after_cnt = 0;
				clmn_cnt = 0;
				index_start = current_pg*Util.pageSize + 0;
				index_write = current_pg*Util.pageSize + 0;
				
				davisBaseClmnFile.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = davisBaseClmnFile.read();
				
				for(int i=0;i<rcd_cnt;i++)
				{
					davisBaseClmnFile.seek(index_offset);
					rcd_offset = davisBaseClmnFile.readShort();
					davisBaseClmnFile.seek(rcd_offset+2+4+1+rcd_cnt); // payload + rowid key + rcd_cnt_byte + rcd_cnt
					byte[] name_buffer = new byte[Util.TEXT_SIZE];
					davisBaseClmnFile.readFully(name_buffer);
					String tbl_name = new String(name_buffer).trim();
					
					if(!tbl_name.equalsIgnoreCase(table_name.trim()))
					{
						index_offset += 2;
						continue;
					}
					
					clmn_cnt++;
					
					if(clmn_cnt == 1)
					{
						index_write = index_offset;
						front_cnt = i;
					}
					
					pg_found = true;
					
					// found the record,re-write the data to 0
					davisBaseClmnFile.seek(rcd_offset);
					int rcd_size = new DavisBaseColumnsCell().getSize();
					for(int t=0;t<rcd_size;t++)
						davisBaseClmnFile.write(0);		
					
					index_offset += 2;
				}
				
				index_start = index_write + 2*clmn_cnt;
				after_cnt = rcd_cnt - front_cnt - clmn_cnt;
				// found the record,
				// move all the indexs ahead by 2			
				for(int k=0;k<after_cnt;k++)
				{
					davisBaseClmnFile.seek(index_start);
					int address = davisBaseClmnFile.readShort();
					davisBaseClmnFile.seek(index_write);
					davisBaseClmnFile.writeShort(address);
					index_start += 2;
					index_write += 2;
				}
				
				// if not in the middle
				if(after_cnt == 0)
				{
					for(int k=0;k<clmn_cnt;k++)
					{
						davisBaseClmnFile.seek(index_write);
						davisBaseClmnFile.writeShort(0);
						index_write += 2;
					}
				}
				
				// update the record count
				if(pg_found)
				{
					davisBaseClmnFile.seek(current_pg*Util.pageSize + 1);
					davisBaseClmnFile.write(rcd_cnt-clmn_cnt);
				}
				
				// update record start
				davisBaseClmnFile.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = davisBaseClmnFile.read();
				davisBaseClmnFile.seek(current_pg*Util.pageSize + 8*rcd_cnt);
				int rcd_start = davisBaseClmnFile.readShort();
				davisBaseClmnFile.seek(2);
				davisBaseClmnFile.writeShort(rcd_start);
				
				current_pg++;
										
			}
			
			davisBaseClmnFile.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		success =true;
		return success;				
	}
	
}
