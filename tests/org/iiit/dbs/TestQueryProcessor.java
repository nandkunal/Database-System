package org.iiit.dbs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestQueryProcessor {
	private QueryProcessor queryObj;
	private DBSystem dbSystem = null;

	@Before
	public void setUp() throws Exception {
		queryObj = new QueryProcessor();
		dbSystem = new DBSystem();
		String configPath = "resources/config.txt";
		dbSystem.readConfig(configPath);
		dbSystem.populateDBInfo();
		dbSystem.initializeLRUTable();
	}

	@After
	public void tearDown() throws Exception {
		queryObj = null;
	}
	@Test
    public void testAllSelect() {
	queryObj.queryType("SELECT  * from countries");
	
}
	//@Test
	    public void testSelect() {
		queryObj.queryType("SELECT  name ,ID from countries");
		
	}
	//@Test
    public void testSelectDistinct() {
	queryObj.queryType("SELECT DISTINCT City FROM Customers;");
	
}
	//@Test
    public void testSelectWhere() {
	queryObj.queryType("SELECT name FROM countries where id=1 and value='Abc';");
	
}
	//@Test
    public void testSelectOrderBy() {
	queryObj.queryType("SELECT name FROM countries where id=1 ORDER BY id;");
	
}
	//@Test
    public void testSelectGroupBy() {
	queryObj.queryType("SELECT name FROM countries where id=1 GROUP BY title;");
	
}
	//@Test
    public void testSelectHaving() {
	queryObj.queryType("SELECT name FROM countries where id=1 GROUP BY title HAVING id>10;");
	
}
	//@Test
    public void testCreate() {
	queryObj.queryType("CREATE TABLE Persons(PersonID int,LastName varchar(255),FirstName varchar(255),Address varchar(255),City varchar(255));");
	
}
}
