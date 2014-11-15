package org.iiit.dbs;

import org.iiit.dbs.execptions.TableNotFoundExecption;
import org.iiit.dbs.execptions.UnknownColumnException;
import org.junit.Before;
import org.junit.Test;

public class TestCreateIndex {

	@Before
	public void setUp() throws Exception {
		String configPath = "resources/config.txt";
		DBConfigReader.getInstance().readConfig(configPath);
	}
	
	@Test
	public void testCreateIndex() throws TableNotFoundExecption, UnknownColumnException
	{
		CreateIndex index = new CreateIndex("persons", "ID");
		index.readDataFileAndWriteToIndex();
	}

}
