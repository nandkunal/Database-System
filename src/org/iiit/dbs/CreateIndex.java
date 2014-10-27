package org.iiit.dbs;

import java.io.File;
import java.io.IOException;

public class CreateIndex {
	
	private static final String CONFIGPATH="resources/config.txt";
	private static final String TABLE_INDEX_EXTENSION="index";
	private static final String FILE_EXTENSION="csv";
	private String tableName;
	
	
	public CreateIndex(String tableName)
	{
		this.tableName=tableName;
	}
	
	public void readDataFileAndWriteToIndex()
	{
		
	}
	
	

	public void createIndexFile()
	{
		String indexFileName=tableName+"."+TABLE_INDEX_EXTENSION;
		File f = new File(DBConfigReader.getInstance().getPathTables()+File.separator+indexFileName);
		if(!f.exists()){
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
}
