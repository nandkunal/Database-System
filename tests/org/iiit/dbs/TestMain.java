package org.iiit.dbs;

public class TestMain {

	public static void main(String[] args) {
		DBSystem dbSystem = new DBSystem();
		String configPath = "resources/config.txt";
		dbSystem.readConfig(configPath);
		dbSystem.populateDBInfo();
		dbSystem.displayDataBaseMetaData();
		dbSystem.initializeLRUTable();
		dbSystem.getRecord("student", 1);
		dbSystem.insertRecord("student", "6,Pops");
		dbSystem.flushPages();
		
	}

}
