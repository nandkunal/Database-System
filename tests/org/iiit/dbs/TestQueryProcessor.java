package org.iiit.dbs;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestQueryProcessor {
	private QueryProcessor queryObj;
	private DBSystem dbSystem = null;

	@Before
	public void setUp() throws Exception {
		String configPath = "resources/config.txt";
		DBConfigReader.getInstance().readConfig(configPath);
		dbSystem = new DBSystem();
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
		queryObj = new QueryProcessor(dbSystem);
	
	}

	@After
	public void tearDown() throws Exception {
		queryObj = null;
	}
	//@Test
    public void testAllSelect() {
	
		//CreateIndex index = new CreateIndex("countries", "id");
		//index.readDataFileAndWriteToIndex();
	    try {
			queryObj.queryType("SELECT  * from countries");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
}
	//@Test
	    public void testSelect() {
		try {
			queryObj.queryType("SELECT NAME,VAL from countries");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	//@Test
    public void testSelectDistinct() {
	try {
		queryObj.queryType("SELECT DISTINCT City FROM Customers;");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
	//@Test
    public void testSelectWhere() {
	try {
		queryObj.queryType("SELECT * FROM countries where ID=302622;");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
	//@Test
    public void testSelectOrderBy() {
	try {
		queryObj.queryType("SELECT name FROM countries where id=1 ORDER BY id;");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
	//@Test
    public void testSelectGroupBy() {
	try {
		queryObj.queryType("SELECT name FROM countries where id=1 GROUP BY title;");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
	//@Test
    public void testSelectHaving() {
	try {
		queryObj.queryType("SELECT name FROM countries where id=1 GROUP BY title HAVING id>10;");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
	//@Test
    public void testCreate() {
    	try {
    		queryObj.queryType("CREATE TABLE Persons(PersonID int,LastName varchar(255),FirstName varchar(255),Address varchar(255),City varchar(255));");
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
}
    
    @Test
	public void testSimpleInnerJoin()
	{
		try {
			queryObj.queryType("SELECT * from persons INNER JOIN countries ON persons.ID=countries.ID");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
