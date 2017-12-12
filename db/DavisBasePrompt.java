package db;

import java.util.Scanner;

import commands.DDL;
import commands.DML;
import commands.VDL;
import metaData.DavisBaseColumns;
import metaData.DavisBaseTables;

import java.util.ArrayList;
import java.util.Arrays;

public class DavisBasePrompt {

		/* This can be changed to whatever you like */
		static String prompt = "davisql> ";
		static String version = "v1.0b";
		static String copyright = "©2017 Nana Xu";
		static boolean isExit = false;
		static DavisBaseTables davisBaseTbl;
		static DavisBaseColumns davisBaseColumns;

		/* 
		 *  The Scanner class is used to collect user commands from the prompt
		 *  There are many ways to do this. This is just one.
		 *
		 *  Each time the semicolon (;) delimiter is entered, the userCommand 
		 *  String is re-populated.
		 */
		static Scanner scanner = new Scanner(System.in).useDelimiter(";");
		
		/** ***********************************************************************
		 *  Main method
		 */
	    public static void main(String[] args) {

	    	// check if the datase exists and complete, if not, complete the initialization
	    	initMetaData();
	    	    	
			/* Display the welcome screen */
			splashScreen();

			/* Variable to collect user input from the prompt */
			String userCommand = ""; 

			while(!isExit) {
				System.out.print(prompt);
				/* toLowerCase() renders command case insensitive */
				userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
				
				userCommand.replace(";", "");
				// userCommand = userCommand.replace("\n", "").replace("\r", "");
				parseUserCommand(userCommand);
			}
			//System.out.println("Exiting...");

		}

		/** ***********************************************************************
		 *  Method definitions
		 */

		/**
		 *  Display the splash screen
		 */
		public static void splashScreen() {
			System.out.println(line("-",80));
	        System.out.println("Welcome to DavisBaseLite"); // Display the string.
			System.out.println("DavisBaseLite Version " + getVersion());
			System.out.println(getCopyright());
			System.out.println("\nType \"help;\" to display supported commands.");
			System.out.println(line("-",80));
		}
		
		/**
		 * @param s The String to be repeated
		 * @param num The number of time to repeat String s.
		 * @return String A String object, which is the String s appended to itself num times.
		 */
		public static String line(String s,int num) {
			String a = "";
			for(int i=0;i<num;i++) {
				a += s;
			}
			return a;
		}
		
		/**
		*  Help: Display supported commands
		*/
		public static boolean help() {
				boolean success = false;
				
				System.out.println(line("*",80));
				System.out.println("SUPPORTED COMMANDS");
				System.out.println("All commands below are case insensitive");
				System.out.println();
				System.out.println("\tSHOW TABLES;                        			   Displays a list of all tables in DavisBase");
				System.out.println("\tCREATE TABLE table_name;                         Create a new table table_name in DavisBase");
				System.out.println("\tCREATE TABLE table_name (column_name1 INT PRIMARY KEY, "
														+ "column_name2 data_type2 [NOT NULL], column_name3 data_type3 [NOT NULL]);"
														+ "\tCreate a new table table_name in DavisBase with schema");
				System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
				System.out.println("\tINSERT INTO table_name [column_list] VALUES (value_list);        "
														+ "Inserts a single record into a table.");
				System.out.println("\tDELETE FROM table_name [column_list] VALUES (value_list);        "
														+ "Delete a single record from a table.");
				System.out.println("\tUPDATE table_name SET column_name = value [WHERE condition];        "
														+ "Modifies one or more records in a table.");
				System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
				System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");	
				System.out.println("\tEXIT;                                            Exit the program");
				System.out.println("\tVERSION;                                         Show the program version.");
				System.out.println("\tHELP;                                            Show this help information");
				System.out.println();
				System.out.println();
				System.out.println(line("*",80));
				
				success = true;
				return success;
		}

		/** return the DavisBase version */
		public static String getVersion() {
			return version;
		}
		
		public static String getCopyright() {
			return copyright;
		}
		
		public static boolean displayVersion() {
			boolean success = false;
			System.out.println("DavisBaseLite Version " + getVersion());
			System.out.println(getCopyright());
			success = true;
			return success;
		}
		
		// parse all commands
		public static void parseUserCommand (String userCommand) {
			
			/* commandTokens is an array of Strings that contains one token per array element 
			 * The first token can be used to determine the type of command 
			 * The other tokens can be used to pass relevant parameters to each command-specific
			 * method inside each case statement */
			ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
			
			boolean success = false;
			
			switch (commandTokens.get(0).trim().toLowerCase()) {
			
				case "show":
					System.out.println("Show all the tables in DavisBase:");
					success = ddl(userCommand);
					break;
				case "create":
					System.out.println("Create a new table in DavisBase:");
					success = ddl(userCommand);
					break;
				case "drop":
					System.out.println("Drop a table in DavisBase:");
					success = ddl(userCommand);
					break;
				case "insert":
					System.out.println("Insert into a table in DavisBase:");
					success = dml(userCommand);
					break;
				case "delete":
					System.out.println("Delete from a table in DavisBase:");
					success = dml(userCommand);
					break;
				case "update":
					System.out.println("Update a table in DavisBase:");
					success = dml(userCommand);
					break;
				case "help":
					System.out.println("Display help info in DavisBase:");
					success = help();
					break;
				case "version":
					System.out.println("Display version of DavisBase:");
					success = displayVersion();
					break;
				case "select":
					System.out.println("Query from a table in DavisBase:");
					success = vdl(userCommand);
					break;
				case "exit":
					System.out.println("Exit from DavisBase:");
					success = vdl(userCommand);
					isExit = true;
					break;
				default:
					System.out.println("I didn't understand the command: \"" + userCommand + "\"");
					break;
			}
			
			if(success)
				System.out.println(userCommand + " succeeded!");
			else
				System.out.println(userCommand + " failed!");
		}
		
		// parse select,exit commands
		private static boolean vdl(String userCommand) {
			
			VDL vdl = new VDL();
			return vdl.parseCmd(userCommand);
			
		}

		// parse insert,delete,update commands
		private static boolean dml(String userCommand) {
			
			DML dml = new DML();
			return dml.parseCmd(userCommand);			
		}

		// parse show tables, create table, drop table commands
		private static boolean ddl(String userCommand) {
			
			ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
			if(commandTokens.get(0).trim().equalsIgnoreCase("show")&&
					commandTokens.get(1).trim().equalsIgnoreCase("tables"))
			{
				return vdl("SELECT * FROM davisbase_tables");
				
			}
			else
			{
				DDL ddl = new DDL();
				return ddl.parseCmd(userCommand);
			}	
			
		}

		// check if the meta data exists and complete, if not, complete the initialization
		private static void initMetaData()
		{
			davisBaseTbl = new DavisBaseTables();
			davisBaseTbl.init();
			
			davisBaseColumns = new DavisBaseColumns();
			davisBaseColumns.init();
		}	

}
