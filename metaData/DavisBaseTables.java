package metaData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;

import util.Util;

public class DavisBaseTables {
	
	private RandomAccessFile davisBaseTblFile;

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
		
		File table = new File(Util.davisBaseTblPath);
		// if davisbase_tables.tbl already exists, return 
		if(table.exists()&&!table.isDirectory())
			return;
		
		// davisbase_tables.tbl doesnot exist, create it
		createDavisBaseTbl();
	
	}
	
	// init the table
	private void createDavisBaseTbl()
	{
		try {
			davisBaseTblFile = new RandomAccessFile(Util.davisBaseTblPath,"rw");	
			davisBaseTblFile.setLength(Util.pageSize);
			
			int rcdLocation = 0;
			int currentPg = 0;
			int pgLocation = Util.pageSize*currentPg;
			
			// define the header
			davisBaseTblFile.seek(pgLocation+rcdLocation);
			// a leaf page
			davisBaseTblFile.write(0x0D); // 1 byte
			// no. of records = 0
			davisBaseTblFile.write(0x00); // 1 byte
			
			// define start of content
			DavisBaseTablesCell cell = new DavisBaseTablesCell();
			int rcdPosition = Util.pageSize-cell.getSize();
			davisBaseTblFile.writeShort(rcdPosition); // 2 bytes
			
			// the right most page
			davisBaseTblFile.writeInt(-1); // 4 bytes
			
			// add record 1, itself
			writeIntoDavisBaseTables(new DavisBaseTablesCell(Util.davisbase_tables,
					new DavisBaseTablesCell().getRcdCnt(),
					new DavisBaseTablesCell().getSize()),currentPg);
			
			davisBaseTblFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
	// write into table davisbase_tables.tbl
	public boolean writeIntoDavisBaseTables(DavisBaseTablesCell record, int currentPg)
	{
		boolean success = false;
				
		int rcd_cnt = getRcdCnt(currentPg);
		int start_position = currentPg*Util.pageSize + 8+rcd_cnt*2; // the very beginning position of start positions
		DavisBaseTablesCell cell = new DavisBaseTablesCell();
		int rcdPosition = (currentPg+1)*Util.pageSize-cell.getSize()*(rcd_cnt+1); 
		
		// write the start position
		try {
				davisBaseTblFile = this.getDavisBaseTables();
				davisBaseTblFile.seek(start_position);

				davisBaseTblFile.writeShort(rcdPosition); // 2 bytes
			
				// write the record
				davisBaseTblFile.seek(rcdPosition);
				
				// payload
				davisBaseTblFile.writeShort(record.getPayloadSize()); // 2 bytes
				// row id
				davisBaseTblFile.writeInt(rcd_cnt+1); // 4 bytes
				
				// columns count = 3, table_name,record_count,avg_length
				davisBaseTblFile.writeByte(record.getClmnCnt()); // 1 byte
				
				// table_name_byte
				davisBaseTblFile.write(record.getTblNameByte()); // 1 byte
				
				// record_count_byte
				davisBaseTblFile.write(record.getRcdCntByte()); // 1 byte
				
				// avg_length_byte
				davisBaseTblFile.write(record.getAvgLngthByte()); // 1 byte
				
				// table name
				davisBaseTblFile.writeBytes(record.getTableName()); // 20 bytes
				
				int next = 2+4+4*Util.TINYINT_SIZE+Util.TEXT_SIZE;
				// record_count
				davisBaseTblFile.seek(rcdPosition+next);
				davisBaseTblFile.writeInt(record.getRcdCnt()); // 4 bytes
				
				next += Util.INT_REAL_SIZE;
				// avg_length
				davisBaseTblFile.seek(rcdPosition+next);
				davisBaseTblFile.writeShort(record.getAvgLngth());
								
				// update record count
				int rcd_cnt_position = currentPg*Util.pageSize + 1;
				davisBaseTblFile.seek(rcd_cnt_position);
				davisBaseTblFile.writeByte(rcd_cnt+1);
				
				// update content start
				int content_start_position = currentPg*Util.pageSize + 2;
				davisBaseTblFile.seek(content_start_position);
				davisBaseTblFile.writeShort(rcdPosition);
				
				// update davisbase_tables record count
				updateTblsRcdCnt();
				
				davisBaseTblFile.close();
				success = true;
				
		} catch (IOException e) {

				e.printStackTrace();
		}
			
		return success;
	}
		
	private void updateTblsRcdCnt() {
		
		davisBaseTblFile = getDavisBaseTables();
		
		try {			
			// read record count
			int pg_cnt = (int)davisBaseTblFile.length()/Util.pageSize;
			int rcd_cnt = 0;
			for(int i=0;i<pg_cnt;i++)
			{			
				davisBaseTblFile.seek(i*Util.pageSize + 1);
				rcd_cnt += davisBaseTblFile.read();
			}
					
			int start_offset = 8;
			davisBaseTblFile.seek(1);
			int pg1_rcd_cnt = davisBaseTblFile.read();
			// read records		
			for(int i=0;i<pg1_rcd_cnt;i++)
			{
				// move to record
				davisBaseTblFile.seek(start_offset);
				int rcd_offset = davisBaseTblFile.readShort();
				davisBaseTblFile.seek(rcd_offset);
				int current_set = rcd_offset;
				
				// move to table_name
				current_set += 10; // payload,rowid,column count, 3 columns
				davisBaseTblFile.seek(current_set);
				
				byte[] buffer = new byte[Util.TEXT_SIZE];
				// read table_name
				davisBaseTblFile.read(buffer);
				String table_name = new String(buffer).trim();
				
				if(table_name.equalsIgnoreCase(Util.davisbase_tables))
				{
					davisBaseTblFile.seek(current_set+Util.TEXT_SIZE);
					// update the record count to be same as rcd_cnt
					davisBaseTblFile.writeInt(rcd_cnt);
					break;
				}
				
				// to next start offset
				start_offset += 2;
			}
			
			davisBaseTblFile.close();
		} catch (IOException e) {

			e.printStackTrace();
		}
		
	}

	public LinkedHashMap<String,String> getClmns()
	{
		// define column_name and data_type
		LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();

		map.put("rowid", "INT");
		map.put("table_name", "TEXT");
		map.put("record_count", "INT");
		map.put("avg_length", "SMALLINT");		
	
		return map;
	}
	
	// get the davisbase_tables.tbl
	public RandomAccessFile getDavisBaseTables()
	{
		try {
			davisBaseTblFile = new RandomAccessFile(Util.davisBaseTblPath,"rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}	
		return davisBaseTblFile;
	}
	
	// get current records count
	public int getRcdCnt(int currentPg)
	{
		int rcd_cnt_position = currentPg*Util.pageSize + 1; 
		int rcd_cnt = -1;
		try {
			davisBaseTblFile = getDavisBaseTables();
			davisBaseTblFile.seek(rcd_cnt_position);
			rcd_cnt = davisBaseTblFile.read();
			davisBaseTblFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rcd_cnt;
	}

	public boolean checkTblRcd(String table) {
		
		boolean exist = false;
		davisBaseTblFile = getDavisBaseTables();
				
		try {
			// read record count
			int pg_cnt = (int)davisBaseTblFile.length()/Util.pageSize;
			int current_pg = 0;
			int rcd_cnt = 0;
			
			for(int j=0;j<pg_cnt;j++)
			{
				davisBaseTblFile.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = davisBaseTblFile.read();
				int start_offset = current_pg*Util.pageSize + 8;
				current_pg++;
				
				// read records		
				for(int i=0;i<rcd_cnt;i++)
				{
					// move to record
					davisBaseTblFile.seek(start_offset);
					int rcd_offset = davisBaseTblFile.readShort();
					davisBaseTblFile.seek(rcd_offset);
					int current_set = rcd_offset;
					
					// move to table_name
					current_set += 10; // payload,rowid,column count, 3 columns
					davisBaseTblFile.seek(current_set);
					
					byte[] buffer = new byte[Util.TEXT_SIZE];
					// read table_name
					davisBaseTblFile.read(buffer);
					String table_name = new String(buffer);
					
					// if table_name is the table, exist is true
					if(table_name.trim().equalsIgnoreCase(table.trim()))
					{
						exist = true;
						break;
					}
					
					// to next start offset
					start_offset += 2;
				}
				
			}
			
			davisBaseTblFile.close();
		} catch (IOException e) {

			e.printStackTrace();
		}
		return exist;
	}

	public boolean deleteTblRcd(String table_name)
	{
		boolean success = false;
		davisBaseTblFile = getDavisBaseTables();
			
		try {		
			// read record count
			int pg_cnt = (int)davisBaseTblFile.length()/Util.pageSize;
			int current_pg = 0;
		
			for(int j=0;j<pg_cnt;j++)
			{
				davisBaseTblFile.seek(current_pg*Util.pageSize + 1);
				int rcd_cnt = davisBaseTblFile.read();
				int rcd_offset = -1;
				int index_offset = current_pg*Util.pageSize + 8;			
				boolean found = false;
				
				for(int i=0;i<rcd_cnt;i++)
				{
					davisBaseTblFile.seek(index_offset);
					rcd_offset = davisBaseTblFile.readShort();
					davisBaseTblFile.seek(rcd_offset+2+4+1+rcd_cnt); // payload + rowid key + rcd_cnt_byte + rcd_cnt
					byte[] name_buffer = new byte[Util.TEXT_SIZE];
					davisBaseTblFile.readFully(name_buffer);
					String tbl_name = new String(name_buffer).trim();
					
					if(!tbl_name.equalsIgnoreCase(table_name.trim()))
					{
						index_offset += 2;
						continue;
					}
					
					// found the record,
					davisBaseTblFile.seek(rcd_offset);
					int pay_load = davisBaseTblFile.readShort();
					for(int k=0;k<pay_load+6;k++)
						davisBaseTblFile.write(0);
					
					// move all the indexs ahead by 2
					int index_start = index_offset + 2;
					int index_write = index_offset;
					
					for(int k=0;k<rcd_cnt-i;k++)
					{
						davisBaseTblFile.seek(index_start);
						int address = davisBaseTblFile.readShort();
						davisBaseTblFile.seek(index_write);
						davisBaseTblFile.writeShort(address);
						index_start += 2;
						index_write += 2;
					}
								
					found = true;
					break;
				}
				
				// update the record count
				if(found)
				{
					davisBaseTblFile.seek(current_pg*Util.pageSize + 1);
					davisBaseTblFile.write(rcd_cnt-1);
				}
				
				// update record start
				davisBaseTblFile.seek(current_pg*Util.pageSize + 1);
				rcd_cnt = davisBaseTblFile.read();
				davisBaseTblFile.seek(current_pg*Util.pageSize + 8*rcd_cnt);
				int rcd_start = davisBaseTblFile.readShort();
				davisBaseTblFile.seek(2);
				davisBaseTblFile.writeShort(rcd_start);
				
				current_pg++;
			}
			
			davisBaseTblFile.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		success = true;
		return success;		
	}
	
	public boolean updateTblCnt(String tbl,int delta)
	{
		boolean success = false;
		davisBaseTblFile = getDavisBaseTables();
		
		try{
		// read record count
		int pg_cnt = (int)davisBaseTblFile.length()/Util.pageSize;
		int current_pg = 0;
		int rcd_cnt = 0;
		
		for(int j=0;j<pg_cnt;j++)
		{
			davisBaseTblFile.seek(current_pg*Util.pageSize + 1);
			rcd_cnt = davisBaseTblFile.read();
			int start_offset = current_pg*Util.pageSize + 8;
			current_pg++;
			
			// read records		
			for(int i=0;i<rcd_cnt;i++)
			{
				// move to record
				davisBaseTblFile.seek(start_offset);
				int rcd_offset = davisBaseTblFile.readShort();
				davisBaseTblFile.seek(rcd_offset);
				int current_set = rcd_offset;
				
				// move to table_name
				current_set += 10; // payload,rowid,column count, 3 columns
				davisBaseTblFile.seek(current_set);
				
				byte[] buffer = new byte[Util.TEXT_SIZE];
				// read table_name
				davisBaseTblFile.read(buffer);
				String table_name = new String(buffer);
				
				davisBaseTblFile.seek(current_set+Util.TEXT_SIZE);
				int cnt = davisBaseTblFile.readInt();
				
				// if table_name is the table, exist is true
				if(table_name.trim().equalsIgnoreCase(tbl.trim()))
				{
					davisBaseTblFile.seek(current_set+Util.TEXT_SIZE);
					davisBaseTblFile.writeInt(cnt+delta);				
					break;
				}
				
				// to next start offset
				start_offset += 2;
			}
			
		}
		
		davisBaseTblFile.close();
		
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		
		success = true;
		return success;
	}

}
