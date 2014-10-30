package org.iiit.dbs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
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
		if(!Validator.isColumnExistsInTable(tableName, index))
		{
			throw new UnknownColumnException(index, tableName);
		}
		 String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName+"."+FILE_EXTENSION;
		 long recordId=0;
		 Map<Long,Long> indexMap = new TreeMap<Long,Long>();
		 String line=null;
		 RandomAccessFile randomAccessFile=null;
		 RandomAccessFile indexFile=null;
		 String indexFileName=tableName+"."+TABLE_INDEX_EXTENSION;
		 String indexFilePath=DBConfigReader.getInstance().getPathTables()+File.separator+indexFileName;
		try {
			createIndexFile(indexFilePath);
			 randomAccessFile=new RandomAccessFile(tableNameFile,"r");
			 indexFile = new RandomAccessFile(indexFilePath,"rw");
			 try {
				
				while((line=randomAccessFile.readLine())!=null)
				{
					//System.out.println(line);
					indexMap.put(Long.parseLong(line.split(",")[0]),recordId);
					recordId++;
				}
				Iterator<Map.Entry<Long,Long>> entry = indexMap.entrySet().iterator();
				while(entry.hasNext())
				{
					Map.Entry<Long, Long> ent = entry.next();
					Long key = ent.getKey();
					Long value = ent.getValue();
					indexFile.writeChars(Long.toString(key)+","+Long.toString(value));
					indexFile.writeChars("\n");
				}
				
			} 
			 catch (IOException e) {
					
					e.printStackTrace();
				}
				
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		finally
		{
			try {
				indexFile.close();
				randomAccessFile.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
