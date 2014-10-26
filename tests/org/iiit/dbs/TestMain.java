package org.iiit.dbs;

public class TestMain {

	public static void main(String[] args) {
		DBSystem dbSystem = new DBSystem();
		String configPath = "resources/config.txt";
		dbSystem.readConfig(configPath);
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		//dbSystem.insertRecord("countries","302614,India,NA");
		//dbSystem.flushPages();
		//get record
		System.out.println(dbSystem.getRecord("countries", 0));
	}

}
