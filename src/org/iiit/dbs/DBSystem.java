package org.iiit.dbs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class DBSystem {
	private static HashMap<String,List<Page>> dbMetaData = new HashMap<String,List<Page>>();
	public static HashMap<String, HashMap<Integer,Page> > localPageTable;
	public static LinkedHashMap<Integer,HashMap<Integer,String>> page;
	public static HashMap<Integer,String> globalPageMap;
	public static HashMap<Integer,List<Integer>> flushInfoMap;
	public static List<Integer> pagesToFlush = new ArrayList<Integer>();
	
	
	public void initializeLRUTable()
	
	{
		globalPageMap=new HashMap<Integer,String>();
        page=new LinkedHashMap<Integer, HashMap<Integer, String>>();
        flushInfoMap=new HashMap<Integer,List<Integer>>();
        int i;
        for(i=0;i<DBConfigReader.getInstance().getNumPages();i++)
        {
            globalPageMap.put(i,null);
            page.put(i,null);
            List<Integer> vec=new ArrayList<Integer>();
            flushInfoMap.put(i,vec);
        }
        

        localPageTable=new HashMap<String, HashMap<Integer, Page>>();
        try
        {
            for(i=0;i<DBConfigReader.getInstance().getTableNamesList().size();i++)
            {
            	localPageTable.put(DBConfigReader.getInstance().getTableNamesList().get(i),null);
            }
        }
        catch(Exception e)
        {

            e.printStackTrace();
        }
     }
	
	
	
	public void populateDBInfo()
	{
		
		try
		{
			Iterator<String> it=DBConfigReader.getInstance().getTableNamesList().iterator();
			String table_name;
			String table_Fname;
			String record;
			
			int currentFree=0;
			int currentOffset,currentRecord,recordLength=0;
			int nextRecordOffset=0,nextRecordSize=0;
			int newLine = 0;
			
			boolean newPage=true,firstPage = true,pageAdded = false,lastRecord=false;
			
			Page pageData = new Page();
			
			// Reading all the tables Data into pages
			
			
			while(it.hasNext())
			{
				table_name=it.next();
				table_Fname=DBConfigReader.getInstance().getPathTables()+File.separator+table_name+".index";
				Scanner tableReader = new Scanner(new File(table_Fname));
				tableReader.useDelimiter("\n");
				
				currentOffset = 0;
				currentRecord = 0;
				
				firstPage = true;
				newPage=true;
				
				nextRecordOffset=0;
				nextRecordSize=0;
				
				lastRecord = false ;
				newLine = 0;
				
				// Read the rows of the table
				
				while(tableReader.hasNext())
				{
					if(newPage)
					{
						pageData = new Page();
						pageData.startRecord=currentRecord;
						pageData.offSet=currentOffset;
						
						currentOffset+=nextRecordOffset;
						currentFree = DBConfigReader.getInstance().getPageSize()-nextRecordSize;
						
						newPage= false;
						newLine=1;
						pageAdded=false;
						record=null;
						
					}else
					{
						record=tableReader.next();
						recordLength=record.length();
						if(!tableReader.hasNext() && record!=null)
							lastRecord=true;
						currentOffset+=recordLength+1;
						if((currentFree-recordLength-1)<0 && (currentFree - recordLength)==0)
						{
							newLine = 0;						
						}
						currentFree-= (recordLength+newLine);
						currentRecord++;
						if(currentFree<0)
						{
							if(firstPage)
							{	
									currentRecord--;
									firstPage = false;
							}
							pageData.endRecord = currentRecord -1;
							currentOffset -= (recordLength+1); 
							
							nextRecordOffset = recordLength+newLine; 
							nextRecordSize = recordLength+0;	
							
							addRecordtoDB(table_name,pageData);
							
							newPage = true;
							pageAdded = true;
						}
					}
				}
				
				if(lastRecord && pageAdded)
				{
					pageData = new Page(); 
					pageData.startRecord = currentRecord;
					pageData.offSet = currentOffset ;
					pageData.endRecord = currentRecord;
					addRecordtoDB(table_name,pageData);
				}else if(lastRecord && firstPage)
				{
					pageData.endRecord=currentRecord-1;
					addRecordtoDB(table_name, pageData);
				}
				else if(lastRecord && !pageAdded) 
				{
					pageData.endRecord = currentRecord;
					addRecordtoDB(table_name, pageData);
				}
				
				tableReader.close();
			}
			
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		
	}

	


	
	
	
	
	public void insertRecord(String tableName, String record)
	{
	
		String table_Fname=DBConfigReader.getInstance().getPathTables()+File.separator+tableName+".csv";
		List <Page> currentTable=dbMetaData.get(tableName);
		
		int lastPageNo=currentTable.size()-1;
        HashMap <Integer,String> thisPage=null;
        
        long freeSpaceInLastPage;
        long fileLength=0;
        String replaceTableName="";
        RandomAccessFile randomAccessFile;

        Page lastPage=currentTable.get(lastPageNo);
        try
        {
            randomAccessFile=new RandomAccessFile(table_Fname,"r");
            fileLength=randomAccessFile.length();
            randomAccessFile.close();
        }
        catch(Exception e)
        {
        	System.out.println("Error while opening file in insertRecord "+e);
        	e.printStackTrace();
        }
        
        freeSpaceInLastPage=DBConfigReader.getInstance().getPageSize()-(fileLength-lastPage.offSet);
        if(freeSpaceInLastPage>=(record.length()+1) || freeSpaceInLastPage==(record.length()) )
        {
        	 int k;
             boolean pageFound=false;
             if(localPageTable.get(tableName)==null)
             {
            	 int availablePage=freePageAvailable();
            	 /* no free frame available to fetch required page into memory*/
                 if(availablePage==-1)
                 {
                	 	availablePage=replacePageAlgo();
                	 	/* get table name from which the old entry is to be invalidated */
                	 	replaceTableName=globalPageMap.get(availablePage);
                	 	localPageTable.get(replaceTableName).remove(availablePage);
                 }
                 
                 HashMap<Integer,String> buffer=new HashMap<Integer, String>();
            	 try
            	 {
            		 randomAccessFile=new RandomAccessFile(table_Fname,"r");
            			
                     /* goto first record of that page using offset*/
                    randomAccessFile.seek(lastPage.offSet);

                    String line="";
                    int i;

                     /*read next n records*/
                    for(i=0;;i++)
                    {
                        line=randomAccessFile.readLine();

                        if(line==null)
                            break;

                        buffer.put(lastPage.startRecord+i,line);
                    }
                    buffer.put(lastPage.startRecord+i,record);
                    flushInfoMap.get(availablePage).add(lastPage.startRecord+i);
                    page.put(availablePage, buffer);
                    if(!pagesToFlush.contains(availablePage))
                    	pagesToFlush.add(availablePage);
                    
                    randomAccessFile.close();
                    lastPage.endRecord++;
                    
                    HashMap<Integer,Page> vector=localPageTable.get(tableName);
               	 	if(vector==null)
               	 		vector=new HashMap<Integer, Page>();

                    /* make entry in global page table*/
                    globalPageMap.put(availablePage,tableName);
                    vector.put(availablePage,lastPage);
                    localPageTable.put(tableName,vector);
            	 }catch(Exception e)
            	 {
            		 e.printStackTrace();
            	 }
             }
             else
             {
             
	             Object [] pageKey=localPageTable.get(tableName).keySet().toArray();
	
	
	             /* check whether that page is available in memory */
	             for(k=0;k<localPageTable.get(tableName).size();k++)
	             {
	                 if(localPageTable.get(tableName).get(pageKey[k]).offSet==lastPage.offSet)
	                 {
	                     pageFound=true;
	                     lastPageNo=Integer.parseInt(pageKey[k].toString());
	                     break;
	                 }
	             }
	             if(pageFound)
	             {
	            	 thisPage=page.get(lastPageNo);
	                 thisPage.put(++lastPage.endRecord,record);
	                 flushInfoMap.get(lastPageNo).add(lastPage.endRecord);
	                 if(!pagesToFlush.contains(lastPageNo))
	                    	pagesToFlush.add(lastPageNo);
	                 page.put(lastPageNo,thisPage);
	                 localPageTable.get(tableName).put(lastPageNo,lastPage);
	             }
	             else
	             {
	            	 int availablePage=freePageAvailable();
	            	 /* no free frame available to fetch required page into memory*/
	                 if(availablePage==-1)
	                 {
	                	 	availablePage=replacePageAlgo();
	                	 	/* get table name from which the old entry is to be invalidated */
	                	 	replaceTableName=globalPageMap.get(availablePage);
	                	 	localPageTable.get(replaceTableName).remove(availablePage);
	                	 	
	                	 	
	                	 	
	                 }
	                 HashMap<Integer,String> buffer=new HashMap<Integer, String>();
	                 try
	                 {
	                    /* open table file to read page */
	                    randomAccessFile=new RandomAccessFile(table_Fname,"r");
	
	                     /* goto first record of that page using offset*/
	                    randomAccessFile.seek(lastPage.offSet);
	
	                    String line="";
	                    int i;
	
	                     /*read next n records*/
	                    for(i=0;;i++)
	                    {
	                        line=randomAccessFile.readLine();
	
	                        if(line==null)
	                            break;
	
	                        buffer.put(lastPage.startRecord+i,line);
	                    }
	                    buffer.put(lastPage.startRecord+i,record);
	                    flushInfoMap.get(availablePage).add(lastPage.startRecord+i);
	                    page.put(availablePage, buffer);
	                    if(!pagesToFlush.contains(availablePage))
	                    	pagesToFlush.add(availablePage);
	                    randomAccessFile.close();
	                    lastPage.endRecord++;
	
	                    /* make entry in global page table*/
	                    globalPageMap.put(availablePage,tableName);
	                    localPageTable.get(tableName).put(availablePage,lastPage);
	                }
	
	                 catch (Exception e)
	                 {
	                	 e.printStackTrace();
	                  //   System.out.println("Error="+e);
	                 }
	             }
	             
	             
	             
             }     
        }else
        {
        	int tempOffset,availablePage;
            int tempStart,tempEnd;
            try
            {
                  randomAccessFile=new RandomAccessFile(table_Fname,"r");
                  fileLength=randomAccessFile.length();
                  randomAccessFile.close();

            }
            catch(Exception e)
            {
              	e.printStackTrace();
                 // System.out.println("Error while opening file in insertRecord "+e);
              }

              tempOffset=(int)fileLength+1;
              tempStart=lastPage.endRecord+1;
              tempEnd=tempStart;

              Page tempObj=new Page();
              tempObj.offSet=tempOffset;
              tempObj.startRecord=tempStart;
              tempObj.endRecord=tempEnd;

              dbMetaData.get(tableName).add(tempObj);

              availablePage=freePageAvailable();

              if(availablePage==-1)
              {
                 // System.out.println("free page not found. going for replacement");

                  availablePage=replacePageAlgo();

                  /* get table name from which the old entry is to be invalidated */
                  replaceTableName=globalPageMap.get(availablePage);


                  /* remove old page entry*/

                  localPageTable.get(replaceTableName).remove(availablePage);
              //    System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);


              }


              HashMap<Integer,String> buffer=new HashMap<Integer, String>();

              buffer.put(tempEnd,record);

              page.put(availablePage, buffer);

                  /* make entry in global page table and local page table*/
              globalPageMap.put(availablePage,tableName);
              if(localPageTable.get(tableName)!=null){
              localPageTable.get(tableName).put(availablePage,tempObj);
              }

              try
              {
                //  record=record.replaceAll(" ",",");
            	  FileWriter fw=new FileWriter(new File(table_Fname),true);
            	  fw.write(record+"\n");
            	  fw.close();
              }
              catch (Exception e)
              {
                  e.printStackTrace();
              }
        }
   }
	
	
	
	
	


	public void flushPages()
	{
		
		// get the tableName corresponding to the page and the records from the LRU.
		FileWriter fw;
		try
		{
			for(int i=0;i<pagesToFlush.size();i++)
			{
				int page_num=pagesToFlush.get(i);
				List<Integer> vc=flushInfoMap.get(page_num);
				Map<Integer,String> records=page.get(page_num);
				String tableName=globalPageMap.get(page_num);
				String filename=DBConfigReader.getInstance().getPathTables()+File.separator+tableName+".csv";
				fw=new FileWriter(new File(filename),true);
				for(int l=0;l<vc.size();l++)
				{
					 if(records.containsKey(vc.get(l)))
					 {
						 String rec=records.get(vc.get(l));
						 fw.write(rec+"\n");
						 
				     }
				}
				fw.close();
				dbMetaData.get(tableName).get(dbMetaData.get(tableName).size()-1).endRecord=localPageTable.get(tableName).get(page_num).endRecord;
				flushInfoMap.get(page_num).clear();
			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		pagesToFlush.clear();
	}
	
	
	private int freePageAvailable() {
		// TODO Auto-generated method stub
		
		for(int i=0;i<DBConfigReader.getInstance().getNumPages();i++)
        {
            if(globalPageMap.get(i)==null)
                return i;
        }
        return -1;
	}
	
	
	
	private int replacePageAlgo()
    {

        FileWriter fw;
		Iterator<Map.Entry<Integer,HashMap<Integer,String>>> it= page.entrySet().iterator();
        Map.Entry<Integer,HashMap<Integer,String>> last=null;
        last=it.next();
        int page_num=last.getKey();
        if(pagesToFlush.contains(page_num))
        {
        	List<Integer> vc=flushInfoMap.get(page_num);
        	HashMap<Integer,String> records=page.get(page_num);
        	String tableName=globalPageMap.get(page_num);
        	String filename=DBConfigReader.getInstance().getPathTables()+tableName+".csv";
        	try
        	{
	        	fw=new FileWriter(new File(filename),true);
	        	for(int l=0;l<vc.size();l++)
	        	{
	        		if(records.containsKey(vc.get(l)))
	        		{	
	        			String rec=records.get(vc.get(l));
	        			fw.write(rec+"\n");
	        		}
	        	}
	        	fw.close();
	        	dbMetaData.get(tableName).get(dbMetaData.get(tableName).size()-1).endRecord=localPageTable.get(tableName).get(page_num).endRecord;
	        	flushInfoMap.get(page_num).clear();
        	}catch(Exception e)
        	{
        			e.printStackTrace();
        	}
        	pagesToFlush.remove(page_num);
        }
        return last.getKey();

    }
	
	
	private void addRecordtoDB(String table_name, Page pageData) {
		// TODO Auto-generated method stub
		
			if(dbMetaData.containsKey(table_name))
			{
				dbMetaData.get(table_name).add(pageData);
			}else
			{
				List<Page> pageOffsets=new ArrayList<Page>();
				pageOffsets.add(pageData);
				dbMetaData.put(table_name, pageOffsets);
			}
		
		
	}
	
	public void getAllRecords(String tableName) throws IOException
	{
		String path=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        String tableIndexFile=path.concat(".index");
        String tableFilePath=path.concat(".csv");
        //TODO :Removed all the LRU Logic as of now :(
        //TODO: In case all Pages corresponding to that Table will be loaded in memory
        //System.out.println(dbMetaData);
        //TODO: In this case load all Pages in Memory and directly call seekAllRecords from Main File
        seekAllRecordsFromMainTableFile(tableIndexFile,tableFilePath);
	}

	public void getAllRecordsByColName(String tableName, List<String> cols) throws IOException
	{  
		String path=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        String tableIndexFile=path.concat(".index");
        String tableFilePath=path.concat(".csv");
        //TODO :Removed all the LRU Logic as of now :(
        //TODO: In case all Pages corresponding to that Table will be loaded in memory
        //System.out.println(dbMetaData);
        //TODO: In this case load all Pages in Memory and directly call seekAllRecords from Main File
        seekRecordsFromMainTableFile(tableName,tableIndexFile,tableFilePath,cols);
		
	}
	public void getAllRecordsByWhereCondition(String tableName,
			String leftWhereColumnName, String rightWhereExpValue,
			String operator) throws IOException
	{
		String path=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        String tableIndexFile=path.concat(".index");
        String tableFilePath=path.concat(".csv");
        //Assuming search is based on only on ID ie index file
        seekAllRecordsFromMainTableBasedonCondition(tableIndexFile,tableFilePath,leftWhereColumnName,
        		rightWhereExpValue,operator);
		
	}
	




	private void seekAllRecordsFromMainTableFile(String tableIndexFilePath,String tableFilePath) throws IOException
	{
		 Scanner indexFileReader = null;
		 RandomAccessFile mainTableFile=null;
		 try{
			 
			 indexFileReader = new Scanner(new File(tableIndexFilePath));
			 mainTableFile = new RandomAccessFile(tableFilePath,"r");
			 indexFileReader.useDelimiter("\n");
			 while(indexFileReader.hasNext()){
				 String line = indexFileReader.next();
				 if(line!=null && !line.isEmpty())
				 {
					 String[]pairs=line.split(",");
					 if(pairs !=null && pairs.length==2)
					 {   
						 long offset = Long.parseLong(removeSpaces(pairs[1]));
						 mainTableFile.seek(offset);
						 System.out.println(mainTableFile.readLine());
					 }
				 }
			 }
			 
		 }catch(IOException e)
		 {
			 e.printStackTrace();
		 }
         finally
         {
        	 mainTableFile.close();
        	 indexFileReader.close();
         }
		
	}





	private void seekRecordsFromMainTableFile(String tableName,String tableIndexFilePath,String tableFilePath, List<String> cols) throws IOException
	{  
		List<String> tableCols = DBConfigReader.getInstance().getTableColsListMap().get(tableName);
		List<Integer> indexlist=new ArrayList<Integer>();
		for(int c=0;c<cols.size();c++)
		{
		indexlist.add(tableCols.indexOf(cols.get(c)));
		}
		Scanner indexFileReader = null;
		 RandomAccessFile mainTableFile=null;
		 try{
			 
			 indexFileReader = new Scanner(new File(tableIndexFilePath));
			 mainTableFile = new RandomAccessFile(tableFilePath,"r");
			 indexFileReader.useDelimiter("\n");
			 while(indexFileReader.hasNext()){
				 String line = indexFileReader.next();
				 if(line!=null && !line.isEmpty())
				 {
					 String[]pairs=line.split(",");
					 if(pairs !=null && pairs.length==2)
					 {   
						 long offset = Long.parseLong(removeSpaces(pairs[1]));
						 mainTableFile.seek(offset);
						 String tuple = mainTableFile.readLine();
						 String[] val = tuple.split(",");
						 for(int p : indexlist)
						 {
						 System.out.print(val[p] + " ");
						 }
						 System.out.println(" ");
					 }
				 }
			 }
			 
		 }catch(IOException e)
		 {
			 e.printStackTrace();
		 }
        finally
        {
       	 mainTableFile.close();
       	 indexFileReader.close();
        }
			
       
	}
	
	
	private void seekAllRecordsFromMainTableBasedonCondition(
			String tableIndexFilePath, String tableFilePath,
			String leftWhereColumnName, String rightWhereExpValue,
			String operator) throws IOException
	{     
		boolean isFound=false;
		 Scanner indexFileReader = null;
		 RandomAccessFile mainTableFile=null;
		 try{
			 
			 indexFileReader = new Scanner(new File(tableIndexFilePath));
			 mainTableFile = new RandomAccessFile(tableFilePath,"r");
			 indexFileReader.useDelimiter("\n");
			 while(indexFileReader.hasNext()){
				 String line = indexFileReader.next();
				 if(line!=null && !line.isEmpty())
				 {
					 String[]pairs=line.split(",");
					 if(pairs !=null && pairs.length==2)
					 {   
						 long offset = Long.parseLong(removeSpaces(pairs[1]));
						 String idstr = removeSpaces(pairs[0]);
						 if(idstr.equalsIgnoreCase(rightWhereExpValue))
						 {
						   mainTableFile.seek(offset);
						    System.out.println(mainTableFile.readLine());
						    isFound=true;
						    break;
						 }
					 }
				 }
			 }
			 if(!isFound)
			 {
				 System.out.println("Record doesnot Exists");
			 }
		 }catch(IOException e)
		 {
			 e.printStackTrace();
		 }
        finally
        {
       	 mainTableFile.close();
       	 indexFileReader.close();
        }
		
	}
	
	private String removeSpaces(String str)
	{
		return str.replaceAll("\\s+","");
	}

 private int getRecordsCount(String filePath) throws IOException
 {
    int linesCount =0;
	Reader fileReader=null;
	BufferedReader br =null;
	 try{
		 
		 fileReader = new FileReader(filePath);
		 br = new BufferedReader(fileReader);
		 while(br.readLine()!=null)
		 {
			 linesCount++;
		 }
		 
	 }catch(IOException e)
	 {
		 e.printStackTrace();
	 }
     finally
     {   br.close();
    	 fileReader.close();
     }
	return linesCount;
	 
 }



	
	
}
