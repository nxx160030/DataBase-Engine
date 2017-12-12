package userData;

import java.util.ArrayList;
import java.util.LinkedList;

import metaData.DavisBaseColumnsCell;
import util.Util;

public class TableRcd{
	int rowid_size = Util.INT_REAL_SIZE;
	int payload_size = Util.SMALLINT_SIZE;
	ArrayList<TableColumn> clmn_list = new ArrayList<TableColumn>() ;
	ArrayList<String> vl_list = new ArrayList<String>(); 
	int rcd_size = 2+1; // payload,column count
	int pri_key = -1;
	
	public TableRcd(ArrayList<TableColumn> clmn_list)
	{
		this.clmn_list = clmn_list;
		
		for(int i=0; i<clmn_list.size();i++)
		{
			rcd_size += clmn_list.get(i).getClmnLngth();
		}
		
		rcd_size += clmn_list.size();

	}
	
	public TableRcd(LinkedList<DavisBaseColumnsCell> tbl_clmns, ArrayList<String> cmd_clmns,
			ArrayList<String> vl_list) {
		
		for(int i=0;i<tbl_clmns.size();i++)
		{
			DavisBaseColumnsCell clmn =  tbl_clmns.get(i);
			String clmn_name = clmn.getClmnName().trim().toLowerCase();
			String data_type = clmn.getDataType().trim().toLowerCase();
			String pri_null = clmn.getClmnKey().trim().toLowerCase();
			
			if(pri_null.equalsIgnoreCase(Util.PRIMARY_KEY_STR))
				pri_key = Integer.parseInt(vl_list.get(i).trim());
				
			this.clmn_list.add(new TableColumn(clmn_name,data_type,pri_null));
			
			if(cmd_clmns.contains(clmn_name))
			{
				this.vl_list.add(vl_list.get(i).trim());
			}else
			{
				this.vl_list.add(Util.NULL_STR);
			}
		}
		
		for(int i=0; i<clmn_list.size();i++)
		{
			rcd_size += clmn_list.get(i).getClmnLngth();
		}
		
		rcd_size += clmn_list.size();
	}

	public int getRcdSize()
	{
		return rcd_size;
	}
	
	public ArrayList<TableColumn> getClmnList()
	{
		return clmn_list;
	}
	
	public ArrayList<String> getValueList()
	{
		return vl_list;
	}
	
	public int getPayload()
	{
		return rcd_size - rowid_size - payload_size;
	}
	
	public int getPriKey()
	{
		return pri_key;
	}
	
	
	
}

