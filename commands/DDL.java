package commands;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ListIterator;


import metaData.DavisBaseColumns;
import metaData.DavisBaseColumnsCell;
import metaData.DavisBaseTables;
import metaData.DavisBaseTablesCell;
import userData.TableColumn;
import userData.TableRcd;
import util.Util;

//parse show tables, create table, drop table commands
public class DDL {

	public boolean parseCmd(String userCommand) {
		
		boolean success = false;
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0).trim().toLowerCase()) {
		
			case "create":
				success = createTable(userCommand);
				if(!success)
					System.out.println("format example: create table table_name (column1 int primary key,column2 data_type [not null]);");
				break;
			case "drop":
				success = dropTable(userCommand);
				if(!success)
					System.out.println("format example: drop table table_name;");
				break;
		}
		
		return success;

	}

	// new a table file and initialize
	private boolean createTable(String userCommand) {
		
		boolean success = false;
		
		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
		
		String[] commandTokens = userCommand.split(" ");
		
		if(!createTblFrmtChck(userCommand))
			return success;

		String table_name = commandTokens[2].trim();
		
		ArrayList<TableColumn> clmn_list = createTblClmns(userCommand);
		
		// create new table file and write into headers
		String dir_path = Util.data + Util.user_data;
		File dir = new File(dir_path);
		if(!dir.exists() || !dir.isDirectory())
			dir.mkdir();
		
		String path = Util.findFilePath(table_name);
		File is_exist = new File(path);
		if(is_exist.exists())
		{
			System.out.println("table " + table_name + "already exist!");
			return success;
		}
		
		// calculate record size
		TableRcd tblRcd = new TableRcd(clmn_list);
		int rcd_size = tblRcd.getRcdSize();  
		
		int rcdPosition = 0;
		// init the table file
		Util.initTblFile(path, rcdPosition);
					
		// update meta data davisbase_tables and davisbase_columns
		// update davisbase_tables.tbl
		long davis_tbl_fl_length = new File(Util.davisBaseTblPath).length();
		int currentPg = (int)davis_tbl_fl_length/Util.pageSize;
		DavisBaseTables davisBaseTbl = new DavisBaseTables();
		DavisBaseTablesCell record = new DavisBaseTablesCell(table_name,0,rcd_size);
		RandomAccessFile  davisBaseTblFile = davisBaseTbl.getDavisBaseTables();
		if(Util.chckPgIsFull(Util.davisbase_tables,rcd_size,currentPg-1))
		{
			currentPg = Util.splitPg(Util.davisbase_tables,currentPg);	
		}
				
		if(!davisBaseTbl.writeIntoDavisBaseTables(record,currentPg-1))
		{
			try {
					throw new Exception("failed to update " + Util.davisbase_tables + " with new table " + table_name);
			} catch (Exception e) {
					e.printStackTrace();
			}
		}
		
		try {
			davisBaseTblFile.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		// upate davisbase_columns
		long davis_clmn_fl_length = new File(Util.davisBaseClmnPath).length();
		currentPg = (int)davis_clmn_fl_length/Util.pageSize;
		DavisBaseColumns davisbase_columns = new DavisBaseColumns();
		RandomAccessFile davisBaseClmnFile = davisbase_columns.getDavisBaseClmns();
		int payload_size = rcd_size-2-4; // minus payload and primary key
		int rcd_cnt = 0;
		int current_pg_rcd_cnt = davisbase_columns.getRcdCnt(currentPg-1);;
		for(int i=0;i<currentPg;i++)
		{
			rcd_cnt += davisbase_columns.getRcdCnt(currentPg-i-1);
		}

		int rowid = rcd_cnt+1;
		int ordinal_pos = 1;
		rcd_size = new DavisBaseColumnsCell().getSize();
		rcdPosition = currentPg*Util.pageSize-rcd_size*(current_pg_rcd_cnt+1); 
				
		ListIterator<TableColumn> itr = clmn_list.listIterator();
		while(itr.hasNext())
		{
			TableColumn clmn = itr.next();
			String clmn_name = clmn.getClmnNm();
			String data_type = clmn.getDtTp();
			String pri_null = clmn.getPrNll();
			String is_nullable = Util.NULL_STR;
			String pri_key = Util.NULL_STR;
			if(pri_null.equalsIgnoreCase(Util.PRIMARY_KEY_STR) || 
					pri_null.equalsIgnoreCase(Util.NOTNULL_STR))
			{
				is_nullable = "NO";
			}
			else
			{
				is_nullable = "YES";
			}
			
			if(pri_null.equalsIgnoreCase(Util.PRIMARY_KEY_STR))
			{
				pri_key = Util.PRIMARY_KEY_STR;
			}
			
			DavisBaseColumnsCell clmn_cell = new DavisBaseColumnsCell(rowid,table_name,
					clmn_name, data_type,  
					 ordinal_pos, is_nullable,pri_key);
			try{
			
			if(!Util.chckPgIsFull(Util.davisbase_columns,rcd_size,currentPg-1))
			{
				if(!davisbase_columns.writeIntoDavisBaseColumns(currentPg-1,clmn_cell,rcdPosition,ordinal_pos,payload_size))
				{
					throw new Exception("failed to update " + Util.davisbase_columns + " " + clmn_name);
				}										
			}else
			{
				currentPg = Util.splitPg(Util.davisbase_columns,currentPg-1)+1;
				
				rcdPosition = currentPg*Util.pageSize - rcd_size;					
						
				if(!davisbase_columns.writeIntoDavisBaseColumns(currentPg-1,clmn_cell,rcdPosition,ordinal_pos,payload_size))
				{
					throw new Exception("failed to update " + Util.davisbase_columns + " " + clmn_name);
				}
			}
			
			}catch(Exception e)
			{
				e.printStackTrace();
			}
			rowid++;
			ordinal_pos++;
			rcdPosition -= rcd_size;
		}
		
		try {
			davisBaseClmnFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		success = true;
		
		return success;		
	}
	
	// drop a table
	private boolean dropTable(String userCommand) {
		 
		boolean success = false;
		
		if(!dropTblFrmtChck(userCommand))
			return success;
		
		String[] commandTokens = userCommand.split(" ");
		
		String table_name = commandTokens[2].trim();
		
		String path = Util.findFilePath(table_name);
		
		File table = new File(path);
		
		if(!table.delete())
		{
			System.out.println("Drop table failed: " + table_name);
			return success;
		}
		
		// drop succeeded, update meta data
		DavisBaseColumns davisBaseClmnFile = new DavisBaseColumns();
		DavisBaseTables davisBaseTblFile = new DavisBaseTables();
		if(!davisBaseClmnFile.deleteTblRcds(table_name)
				|| !davisBaseTblFile.deleteTblRcd(table_name))
		{
			System.out.println("Update meta data failed: " + table_name);
			return success;
		}
				
		success = true;
		return success;	
	}
	
	private boolean dropTblFrmtChck(String userCommand) {
	
		boolean success = false;

		String[] commandTokens = userCommand.split(" ");
		
		if(commandTokens.length != 3)
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success;
		}
		
		if(!commandTokens[1].trim().equalsIgnoreCase("table"))
		{
			System.out.println("wrong command format: \"" + commandTokens[1] + "\"");
			return success;
		}
		
		String table_name = commandTokens[2].trim();
		
		if(!Util.chckTblExist(table_name))
		{
			System.out.println("table doesnot exist: \"" + table_name + "\"");
			return success;
		}
		
		success = true;
		return success;
	}

	// check create command format validity
	private boolean createTblFrmtChck(String userCommand)
	{
		boolean success = true;
		
		userCommand = userCommand.replaceAll(", ", ",").replaceAll(" ,", ",");
				
		String[] commandTokens = userCommand.split(",");
		
		String[] cmd0 = commandTokens[0].split(" ");
		
		if(cmd0.length < 7)
		{
			System.out.println("wrong command format: \"" + userCommand + "\"");
			return success = false;
		}
	
		if(!cmd0[1].trim().equalsIgnoreCase("table"))
		{
			System.out.println("wrong command format: \"" + cmd0[1] + "\"");
			return success = false;
		}
		
		if(!cmd0[3].trim().startsWith("(") || !cmd0[3].trim().endsWith(""))
		{
			System.out.println("wrong command format: \"" + cmd0[3] + "\"");
			return success = false;
		}
			
		//String[] columns = commandTokens[3].split(",");
		
		int column_length = commandTokens.length;
		
		if(!commandTokens[column_length-1].trim().endsWith(")"))
		{
			System.out.println("wrong command format: \"," + commandTokens[column_length-1] + "\"");
			return success = false;
		}
		
		if(!cmd0[3].trim().startsWith("(") ||
				!cmd0[4].trim().equalsIgnoreCase("int") ||
				!cmd0[5].trim().equalsIgnoreCase("primary") ||
				!cmd0[6].trim().equalsIgnoreCase("key"))
		{
			System.out.println("wrong command format: \"" + commandTokens[0] + "\"");
			return success = false;
		}	
		
		for(int i=1; i<commandTokens.length-1;i++)
		{
			String[] column_middle = commandTokens[i].split(" ");
			if(!(column_middle.length == 4 || 
					column_middle.length == 2))
			{
				System.out.println("wrong command format: \"" + commandTokens[i] + "\"");
				return success = false;
			}
			
			if(column_middle.length == 4)
			{
				if(!(column_middle[2].equalsIgnoreCase("not") && 
						column_middle[3].equalsIgnoreCase("null")))
				{
					System.out.println("wrong command format: \"" + commandTokens[i] + "\"");
					return success = false;
				}
			}
					
		}
		
		String[] column_last = commandTokens[commandTokens.length-1].split(" ");
		if(!(column_last.length == 4 || 
				column_last.length == 2))
		{
			System.out.println("wrong command format: \"" + commandTokens[commandTokens.length-1] + "\"");
			return success = false;
		}
		
		if(column_last.length == 4)
		{
			if(!(column_last[2].equalsIgnoreCase("not") && 
					column_last[3].equalsIgnoreCase("null)")))
			{
				System.out.println("wrong command format: \"" + commandTokens[commandTokens.length-1] + "\"");
				return success = false;
			}
		}
		
		return success;
	}
	
	// return the new columns, with the 1st column the primary key column
	private ArrayList<TableColumn> createTblClmns(String userCommand) {

		ArrayList<TableColumn> clmn_list = new ArrayList<TableColumn>();
		
		String[] columns = userCommand.split(",");
		
		// add primary key column
		String[] clmn_pri = columns[0].split(" ");
		String pri_clmn_name = clmn_pri[3].trim().replace("(", "");
		TableColumn pri_clmn = new TableColumn(pri_clmn_name,Util.INT_STR,Util.PRIMARY_KEY_STR);
		clmn_list.add(pri_clmn);
		
		// add other columns 
		for(int i=1; i<columns.length;i++)
		{
			String[] clmn_middle = columns[i].replace("(", "").replace(")", "").split(" ");
			String clmn_name = clmn_middle[0].trim();
			String data_type = clmn_middle[1].trim();
			String null_or_not = null;
			if(clmn_middle.length == 4 && 
					clmn_middle[2].trim().equalsIgnoreCase("not"))
			{
				null_or_not = Util.NOTNULL_STR;
			}
			else
			{
				null_or_not = Util.NULL_STR;
			}
			
			TableColumn middle_clmn = new TableColumn(clmn_name,data_type,null_or_not);
			clmn_list.add(middle_clmn);
		}
			
		return clmn_list;
	}
		
}
