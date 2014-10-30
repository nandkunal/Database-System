package org.iiit.dbs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBConfigReader {
	
	
	private static DBConfigReader reader;
	private List<String>tableNamesList=new ArrayList<String>();
	private Map<String,Map<String,String>> tablesMetaData = new HashMap<String,Map<String,String>>();
	private Map<String,List<String>> tableColsListMap = new HashMap<String,List<String>>();
	private  int pageSize;
	private  int numPages;
	private  String pathTables;
	public static DBConfigReader getInstance()
	{
		if(reader==null){
			reader=new DBConfigReader();
		}
		return reader;
	}
	
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
		        	if(start !=0)
		        	{

		        		if(!line.contains(",")&&(!line.contains("_")) && !line.equalsIgnoreCase("BEGIN"))
		        		{  
		        			Map<String,String> colMap=new HashMap<String,String>();
		        			List<String>colsList = new ArrayList<String>();
		        			tableNamesList.add(line);
		        			while(true)
		        			{  
		        				String val=reader.readLine();
		        				if(!val.contains(","))
		        					break;
		        				String[] cols=val.split(",");
		        				colsList.add(removeSpaces(cols[0]));
			        			colMap.put(removeSpaces(cols[0]), removeSpaces(cols[1]));
		        			}
		        			tableColsListMap.put(line, colsList);
		        			tablesMetaData.put(line, colMap);
		        		}
		        	}
		        	
		       }
		        pageSize = 	Integer.parseInt(configParameters.get("PAGE_SIZE"));
		        numPages = Integer.parseInt(configParameters.get("NUM_PAGES"));
		        pathTables = configParameters.get("PATH_FOR_DATA");
		       // System.out.println(tableNamesList);
		        //System.out.println(tablesMetaData);
		        
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


	private String removeSpaces(String str)
	{
		return str.replaceAll("\\s+","");
	}

	public List<String> getTableNamesList() {
		return tableNamesList;
	}

	public int getPageSize() {
		return pageSize;
	}

	public int getNumPages() {
		return numPages;
	}

	public String getPathTables() {
		return pathTables;
	}

	public Map<String, Map<String, String>> getTablesMetaData() {
		return tablesMetaData;
	}

	public Map<String, List<String>> getTableColsListMap() {
		return tableColsListMap;
	}

}
