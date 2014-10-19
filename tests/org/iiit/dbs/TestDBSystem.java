package org.iiit.dbs;



import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDBSystem {
	private DBSystem dbSystem = null;
	
	
	@Before
	public void setUp(){
		dbSystem = new DBSystem();
		String configPath = "resources/config.txt";
		dbSystem.readConfig(configPath);
	}
	
	//@Test
	public void testpopulateDBInfo(){
		dbSystem.populateDBInfo();
		dbSystem.displayDataBaseMetaData();
	}
	
	@Test
	public void testinsertRecord()
	{  
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		dbSystem.insertRecord("countries","302614,India,NA");
		dbSystem.flushPages();
	}
	
	//@Test
	public void testgetRecord(){
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		System.out.println(dbSystem.getRecord("countries",0));
		System.out.println(dbSystem.getRecord("countries",1));
	}
	@After
	public void tearDown(){
		dbSystem = null;
	}
	

}
