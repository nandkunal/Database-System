package org.iiit.dbs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
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
	public void getAllRecords(String tableName)
	{
		String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        tableNameFile=tableNameFile.concat(".index");
        int i,pageIndex;
        String line="",replaceTableName="";
        int startOffset=0,endOffset=0,availablePage,startLine=0,endLine=0;
        
    
        
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
        
       
        
        
       
              startOffset=table.get(0).offSet;
              startLine=table.get(0).startRecord;
              endLine=table.get(table.size()-1).endRecord;
              endOffset=table.get(table.size()-1).offSet;

          

       
       
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
      		 
      		 buffer.put(startLine+i,line);

      	 }
      	 page.put(availablePage,buffer);

      	 /* Add newly aquired page number to localpagetable of that table*/

      	 

      	 /*put newly loaded page and its related table name in global page table*/
      	 globalPageMap.put(availablePage,tableName);

          randomAccessFile.close();
          seekRecordsFromMainTableFile(tableName,buffer);

       }
       catch (Exception e)
       {
      	 e.printStackTrace();
       //System.out.println("Error opening table file"+e);
       }
       

	}
	

	public String getRecord(String tableName, int recordId)
	{
		
		  String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
          tableNameFile=tableNameFile.concat(".index");
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
	/*
	 * 
	 * Tester Funtion 
	 */
	
	
	
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
	
	private void seekRecordsFromMainTableFile(String tableName,Map<Integer,String>recordIndexMap) throws IOException{
		 String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
         tableNameFile=tableNameFile.concat(".csv");
         RandomAccessFile randomAccessFile=new RandomAccessFile(tableNameFile,"r");
		for (Map.Entry<Integer, String> entry : recordIndexMap.entrySet()) {
			
			
			System.out.println(randomAccessFile.readLine());
		}
		
	}



	public void getAllRecordsByColName(String tableName, List<String> cols) {

		String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        tableNameFile=tableNameFile.concat(".index");
        int i,pageIndex;
        String line="",replaceTableName="";
        int startOffset=0,endOffset=0,availablePage,startLine=0,endLine=0;
        
    
        
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
        
              startOffset=table.get(0).offSet;
              startLine=table.get(0).startRecord;
              endLine=table.get(table.size()-1).endRecord;
              endOffset=table.get(table.size()-1).offSet;

          

       
       
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
      		 
      		 buffer.put(startLine+i,line);

      	 }
      	 page.put(availablePage,buffer);

      	 /* Add newly aquired page number to localpagetable of that table*/

      	 

      	 /*put newly loaded page and its related table name in global page table*/
      	 globalPageMap.put(availablePage,tableName);

          randomAccessFile.close();
          seekRecordsFromMainTableFile(tableName,buffer,cols);

       }
       catch (Exception e)
       {
      	 e.printStackTrace();
       //System.out.println("Error opening table file"+e);
       }
       

	
		
	}



	private void seekRecordsFromMainTableFile(String tableName,
			HashMap<Integer, String> buffer, List<String> cols) throws IOException {
		String tableNameFile=DBConfigReader.getInstance().getPathTables()+File.separator+tableName;
        tableNameFile=tableNameFile.concat(".csv");
        RandomAccessFile randomAccessFile=new RandomAccessFile(tableNameFile,"r");
		for (Map.Entry<Integer, String> entry : buffer.entrySet()) {
			
			
			String line = randomAccessFile.readLine();
			String[] col = line.split(",");
			List<String> tableCols = DBConfigReader.getInstance().getTableColsListMap().get(tableName);
			List<Integer> indexlist=new ArrayList<Integer>();
			for(int c=0;c<cols.size();c++){
				
					indexlist.add(tableCols.indexOf(cols.get(c)));
				
			}
			for(int p : indexlist)
			{
			System.out.print(col[p] + " ");
			}
			System.out.println(" ");
		}
		
	}
	

	
	
}
