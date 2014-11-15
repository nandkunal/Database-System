package org.iiit.dbs;

import java.io.IOException;

public class TestMain {

	public static void main(String[] args)
	{   
		String configPath = "resources/config.txt";
		DBConfigReader.getInstance().readConfig(configPath);
		DBSystem dbSystem = new DBSystem();
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		 QueryProcessor queryObj = new QueryProcessor(dbSystem);
	     try {
			queryObj.queryType("SELECT  * FROM countries;");
			queryObj.queryType("SELECT * FROM countries where ID=302610;");
			queryObj.queryType("SELECT NAME FROM countries;");
			queryObj.queryType("SELECT * from persons INNER JOIN countries ON persons.ID=countries.ID");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
