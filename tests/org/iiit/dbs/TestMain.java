package org.iiit.dbs;

public class TestMain {

	public static void main(String[] args)
	{   
		String configPath = "resources/config.txt";
		DBConfigReader.getInstance().readConfig(configPath);
		DBSystem dbSystem = new DBSystem();
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		 QueryProcessor queryObj = new QueryProcessor(dbSystem);
	     queryObj.queryType("SELECT  * from countries");
		// queryObj.queryType("SELECT NAME,ID from countries");
		 //queryObj.queryType("SELECT NAME,ID from countries where ID=302614");
	}

}
