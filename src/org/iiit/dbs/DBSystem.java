package org.iiit.dbs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Scanner;


public class DBSystem {
	
	
	private static int pageSize;
	private static int numPages;
	private static int tableCount;
	public String pathTables;
	
	public static ArrayList<String> tableNamesList = new ArrayList<String>();
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
        for(i=0;i<numPages;i++)
        {
            globalPageMap.put(i,null);
            page.put(i,null);
            List<Integer> vec=new ArrayList<Integer>();
            flushInfoMap.put(i,vec);
        }
        

        localPageTable=new HashMap<String, HashMap<Integer, Page>>();
        try
        {
            for(i=0;i<tableNamesList.size();i++)
            {
            	localPageTable.put(tableNamesList.get(i),null);
            }
        }
        catch(Exception e)
        {

            e.printStackTrace();
        }
     }
	
	
	/* 
	 * 
	 * 
	 * Description :
	 * You need to read the configuration file and extract the page size and number 
	 * of pages (these two parameter together define the maximum main memory you can use). 
	 * Values are in number of bytes.You should read the names of the tables from the configuration file.
	 * You can assume that the table data exists in a file named
	 * <table_name>.csv at the path pointed by the config parameter PATH_FOR_DATA.
	 * You will need other metadata information given in config file for future deliverables. 
	 *
	 */
	
	public void readConfig(String configFilePath)
	{
		Map<String,String> configParameters = new HashMap<String,String>();

	      BufferedReader reader = null;
	      InputStream in = null;
			try {
				 in = new FileInputStream(new File(configFilePath));
		         reader = new BufferedReader(new InputStreamReader(in));
		        String line;
		        int start = 0;
		        while ((line = reader.readLine()) != null) 
		        {
		        	Matcher m = Pattern.compile("([A-Z_]+)(\\s+)(.*)").matcher(line);
		        	while(m.find()){
		        		configParameters.put(m.group(1), m.group(3));
		        	}
		        	
		        	if(line.equalsIgnoreCase("BEGIN")){
		        		start=1;
		        	}
		        	if(line.equalsIgnoreCase("END")){
	    				start = 0;
	    			}
		        	if(start !=0){
		        		
		        	if(!line.contains(",")&&(!line.contains("_")) && !line.equalsIgnoreCase("BEGIN")){
		        		tableNamesList.add(line);
		        	}
		        		
		        	}
		        	
		       }
		        pageSize = 	Integer.parseInt(configParameters.get("PAGE_SIZE"));
		        numPages = Integer.parseInt(configParameters.get("NUM_PAGES"));
		        pathTables = configParameters.get("PATH_FOR_DATA");
		        
			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			finally
			{
				try{
					reader.close();
					in.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		
	
	}
	
	/* Description:
	 * 
	 * The data present in each table needs to be represented in pages.
	 * Read the file corresponding to each table line by line (for now assume 1 line = 1 record).
	 * Maintain a mapping from PageNumber to (StartingRecordId, EndingRecordId) in memory.
	 * You can assume unspanned file organisation and record length will
	 * not be greater than page size. 
	*/
	
	public void populateDBInfo()
	{
		
		try
		{
			Iterator<String> it=tableNamesList.iterator();
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
				table_Fname=pathTables+File.separator+table_name+".csv";
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
						currentFree = pageSize-nextRecordSize;
						
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
	
	
	/* Description:
	 * 
	 * Get the corresponding record of the specified table.DO NOT perform I/O every time. 
	 * Each time a request is received, if the page containing the record is already in memory, 
	 * return that record else bring corresponding page in memory. You are supposed 
	 * to implement LRU page replacement algorithm for the same. Print HIT if the page is in memory, 
	 * else print MISS <pageNumber> where <pageNumber> is the page number of memory page which is to be replaced. (You can assume page
	 * numbers starting from 0. So, you have total 0 to <NUM_PAGES 1>pages.) 
	 * 
	 */
	public List<String> getRecord(String tableName)
	{
		List<String>output = new ArrayList<>();
		 String tableNameFile=pathTables+File.separator+tableName;
         tableNameFile=tableNameFile.concat(".csv");
         
		return output;
	}
	

	public String getRecord(String tableName, int recordId)
	{
		
		  String tableNameFile=pathTables+File.separator+tableName;
          tableNameFile=tableNameFile.concat(".csv");
          int i,pageIndex;
          String line="",replaceTableName="";
          int startOffset=0,endOffset=0,availablePage,startLine=0,endLine=0;
          
          // search if that record ID is already available in page
          String found=searchRecordID(tableName,recordId);
          if(found!=null)
          {
             System.out.println("HIT");
             return found;
          }
          
          /*check if any free page available in global page table*/
          availablePage=freePageAvailable();
          // if free page not available then go for page replacement*/
          if(availablePage==-1)
          {
        	  availablePage=replacePageAlgo();
        	  /* get table name from which the old entry is to be invalidated */
              replaceTableName=globalPageMap.get(availablePage);
              /* remove old page entry*/
              localPageTable.get(replaceTableName).remove(availablePage);
             //System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);
           }
          
          System.out.println("MISS "+availablePage);
          List <Page> table=dbMetaData.get(tableName);
          /*
           * Logic to find if RecordId exists in Last Page
           */
          
          Page lastPage = table.get(table.size()-1);
          if(lastPage.endRecord<recordId){
        	  return "No Record Found";
          }
          
          
          /* get start record ,end record and offset from dbInfo for reading the only that page from file*/
          for(i=0;i<table.size();i++)
          {
            if(table.get(i).startRecord <= recordId && table.get(i).endRecord>=recordId)
            {
                startOffset=table.get(i).offSet;
                startLine=table.get(i).startRecord;
                endLine=table.get(i).endRecord;

                if(i<table.size()-1)
                endOffset=table.get(i+1).offSet;

                break;
            }

         }
         pageIndex=i;
         try
         {
        	 HashMap<Integer,String> buffer=new HashMap<Integer, String>();

        	 /* open table file to read page */
        	 RandomAccessFile randomAccessFile=new RandomAccessFile(tableNameFile,"r");

        	 /* goto first record of that page using offset*/
        	 randomAccessFile.seek((long)startOffset);


        	 /*read next n records*/
        	 for(i=0;i< (endLine-startLine+1);i++)
        	 {
        		 line=randomAccessFile.readLine();
        		 if(line==null)
        			 break;
        		 if(startLine+i==recordId)
        			 found=line;
        		 buffer.put(startLine+i,line);

        	 }
        	 page.put(availablePage,buffer);

        	 /* Add newly aquired page number to localpagetable of that table*/

        	 HashMap<Integer,Page> vector=localPageTable.get(tableName);
        	 if(vector==null)
        		 vector=new HashMap<Integer, Page>();


        	 /* for available pag number store its starting recordID endRecordID and offset*/
        	 vector.put(availablePage,table.get(pageIndex));
        	 localPageTable.put(tableName,vector);


        	 /*put newly loaded page and its related table name in global page table*/
        	 globalPageMap.put(availablePage,tableName);

            randomAccessFile.close();

         }
         catch (Exception e)
         {
        	 e.printStackTrace();
         //System.out.println("Error opening table file"+e);
         }
         return found;
}
	
	
	/* Description:
	 * 
	 *  Get the last page for the corresponding Table in main memory, if not already present.
	 *  If the page has enough free space, 
	 *  then append the record to the page else, 
	 *  get new free page and add record to the new page.Do not flush modified page immediately.
	 *  
	 */
	
	
	public void insertRecord(String tableName, String record)
	{
	
		String table_Fname=pathTables+File.separator+tableName+".csv";
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
        
        freeSpaceInLastPage=pageSize-(fileLength-lastPage.offSet);
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
	
	
	
	
	/* Description:
	 * 
	 * Since primary and secondary memory are independent, no need to
	 * flush modified pages immediately, instead this function will be called
	 * to write modified pages to disk.
	 * Write modified pages in memory to disk.
	 * 
	 */
	
	


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
				String filename=pathTables+File.separator+tableName+".csv";
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
		
		for(int i=0;i<numPages;i++)
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
        	String filename=pathTables+tableName+".csv";
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
	/*
	 * 
	 * Tester Funtion 
	 */
	
	public void tester()
	{
		System.out.println("Page Size: "+pageSize);
		System.out.println("Num Pages: "+numPages);
		System.out.println("Num Tables: "+tableCount);
		System.out.println("File Data Path: "+pathTables);
		System.out.println("Printing Table Names");
		
		Iterator it = tableNamesList.iterator();
		while(it.hasNext())
		{
			System.out.println("Table Name: "+it.next().toString());
		}
	}
	
	
	public String searchRecordID(String tableName,int recordID)
    {
        String result=null;
        int i,pageIndex;

        /* get local page table for current table */
        HashMap<Integer,Page> localPages=localPageTable.get(tableName);

        if(localPages==null)
            return null;

        /* store all its page numbers already present*/
        Object [] pageNos=localPages.keySet().toArray();

        /* for each pagenumber check if required recordID lies in that page*/
        for(i=0;i<pageNos.length;i++)
        {
            if(localPages.get(pageNos[i]).startRecord <= recordID && recordID <= localPages.get(pageNos[i]).endRecord)
            {

                /* if required recordID present in current page then put that page again to increase its access order in linkedHashMap*/
                page.put(Integer.parseInt(pageNos[i].toString()),page.get(Integer.parseInt(pageNos[i].toString())));

                      break;
            }
        }
          if(i==pageNos.length)
          {
                return null;
          }

        HashMap <Integer,String> recordLine=null;
        recordLine=page.get(pageNos[i]);

        result=recordLine.get(recordID);

        return result;
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
	
	public void displayDataBaseMetaData(){
		System.out.println(dbMetaData);
		/*Iterator<Entry<String, List<Page>>> entries = dbMetaData.entrySet().iterator();
		while(entries.hasNext())
		{
			 Map.Entry<String, List<Page>> entry = entries.next();
			 System.out.println("Table Name -"+entry.getKey());
			 System.out.println("Pages List -"+entry.getValue());
		}*/
	}
	
	
}
