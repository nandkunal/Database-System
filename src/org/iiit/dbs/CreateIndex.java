package org.iiit.dbs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.iiit.dbs.execptions.TableNotFoundExecption;
import org.iiit.dbs.execptions.UnknownColumnException;

public class CreateIndex {
	
	private static final String TABLE_INDEX_EXTENSION="index";
	private static final String FILE_EXTENSION="csv";
	private String tableName;
	private String index;
	
	
	public CreateIndex(String tableName,String index)
	{
		this.tableName=tableName;
		this.index=index;
		
	}
	
	public void readDataFileAndWriteToIndex() throws TableNotFoundExecption, UnknownColumnException
	{   
		//Check first Table Exists
		if(!Validator.isTableExists(tableName)){
			throw new TableNotFoundExecption(tableName);
		}else
		{
			long offset = 0;
			if(!Validator.isColumnExistsInTable(tableName, index))
			{
				throw new UnknownColumnException(index, tableName);
			}
			String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName+"."+FILE_EXTENSION;
			Map<Long,Long> indexMap = new TreeMap<Long,Long>();
			FileWriter fr = null;
			BufferedWriter br = null;
			String indexFileName=tableName+"."+TABLE_INDEX_EXTENSION;
			String indexFilePath=DBConfigReader.getInstance().getPathTables()+File.separator+indexFileName;
			Scanner tableReader = null;
			try {
				createIndexFile(indexFilePath);
				fr = new FileWriter(indexFilePath);
				br = new BufferedWriter(fr);
				tableReader = new Scanner(new File(tableNameFile));
				tableReader.useDelimiter("\n");
				while(tableReader.hasNext())
				{
					String lineReader = tableReader.next();
					//TODO:Assuming the Index will be Long  Need to convert to String :)
					//TODO:Assuming the first column is Index :)
					indexMap.put(Long.parseLong(lineReader.split(",")[0]),offset);
					offset+=lineReader.length()+1;

				}



				Iterator<Map.Entry<Long,Long>> entry = indexMap.entrySet().iterator();
				while(entry.hasNext())
				{
					Map.Entry<Long, Long> ent = entry.next();
					Long key = ent.getKey();
					Long value = ent.getValue();
					br.write(key.toString()+","+value.toString());
					br.write(System.getProperty("line.separator"));
				}

			} 
			catch (IOException e) {

				e.printStackTrace();
			}
			finally
			{
				try {
					br.close();
					fr.close();
					tableReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		}
	
	
	
	

	private void createIndexFile(String indexFilePath)
	{
		File f = new File(indexFilePath);
		if(!f.exists()){
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
}
