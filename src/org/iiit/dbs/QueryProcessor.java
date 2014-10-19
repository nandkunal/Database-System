package org.iiit.dbs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.iiit.dbs.util.TablesNamesFinder;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class QueryProcessor {
	
	private List<String> tableNamesList;
	private int pageSize;
	private int numPages;
	private String pathTables;
	private static final String CONFIGPATH="resources/config.txt";
	private static final String TABLE_METADATA_EXTENSION="data";
	private static final String FILE_EXTENSION="csv";
	
	
	public QueryProcessor(){
		readConfig(CONFIGPATH);
	}
	
	public void queryType(String query){
		if(query !=null){
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			try {
				Statement stmt= parserManager.parse(new StringReader(query));
				if(stmt instanceof CreateTable){
					createCommand(query);
				}else if(stmt instanceof Select){
					selectCommand(query);
				}
			} catch (JSQLParserException e) {
				e.printStackTrace();
			}
		}else{
			System.err.print("Invalid Query");
		}
	}
	
	private void selectCommand(String query) {
		
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			Select selectStmt = (Select) parserManager.parse(new StringReader(query));
			TablesNamesFinder tableFinder = new TablesNamesFinder();
			List<String> tableList = tableFinder.getTableList(selectStmt);
			displaySelectCommand(selectStmt);
		} catch (JSQLParserException e) {
			System.err.print("Invalid Query");
			e.printStackTrace();
		}
		
	}

	private void displaySelectCommand(Select selectStmt) {
		StringBuilder str = new StringBuilder();
		str.append("Querytype:select");
		str.append("\n");
		str.append("Tablename:");
		TablesNamesFinder tableFinder = new TablesNamesFinder();
		List<String> tableList = tableFinder.getTableList(selectStmt);
		for(String tableName : tableList)
		{
			str.append(tableName);
			str.append(",");
		}
		str.deleteCharAt(str.length()-1);
		str.append("\n");
		str.append("Columns:");
		PlainSelect plainSelect = (PlainSelect)selectStmt.getSelectBody();
		List selectElements = plainSelect.getSelectItems();
		for(int i=0;i<selectElements.size();i++){
			SelectExpressionItem expressionItem = (SelectExpressionItem)selectElements.get(i);
			if(expressionItem.getExpression() instanceof Column){
			Column col = (Column)expressionItem.getExpression();
			str.append(col.getColumnName());
			str.append(",");
			}else if(expressionItem.getExpression() instanceof Parenthesis){
				Parenthesis par = (Parenthesis)expressionItem.getExpression();
				Column col = (Column)par.getExpression();
				str.append(col.getColumnName());
				str.append(",");
			}
		}
		str.deleteCharAt(str.length()-1);
		//get Distinct Columns
		str.append("\n");
		str.append("Distinct:");
		if(plainSelect.getDistinct()!=null){
			for(int i=0;i<selectElements.size();i++){
				SelectExpressionItem expressionItem = (SelectExpressionItem)selectElements.get(i);
				if(expressionItem.getExpression() instanceof Column){
				Column col = (Column)expressionItem.getExpression();
				str.append(col.getColumnName());
				str.append(",");
				}else if(expressionItem.getExpression() instanceof Parenthesis){
					Parenthesis par = (Parenthesis)expressionItem.getExpression();
					Column col = (Column)par.getExpression();
					str.append(col.getColumnName());
					str.append(",");
				}
			}
			str.deleteCharAt(str.length()-1);
		}else{
			str.append("NA");
		}
		//get Where Condition
		str.append("\n");
		str.append("Condition:");
		str.append((plainSelect.getWhere()==null)?"NA":plainSelect.getWhere());
		//get By Order By
		str.append("\n");
		str.append("Orderby:");
		List orderByElements = plainSelect.getOrderByElements();
		if(orderByElements!=null){
		for(int j=0;j<orderByElements.size();j++){
		OrderByElement orderElem = (OrderByElement)orderByElements.get(j);	
		if(orderElem.getExpression() instanceof Column){
		Column col = (Column)orderElem.getExpression();
		str.append(col.getColumnName());
		str.append(",");
		}
		}
		str.deleteCharAt(str.length()-1);
		}else{
			str.append("NA");
		}
		str.append("\n");
		str.append("Groupby:");
		
		//Groupby
		List groupBy = plainSelect.getGroupByColumnReferences();
		if(groupBy!=null){
		for(int i=0;i<groupBy.size();i++){
			Column col = (Column) groupBy.get(i);
			str.append(col.getColumnName());
			str.append(",");
		}
		str.deleteCharAt(str.length()-1);
		}else{
			str.append("NA");	
		}
		str.append("\n");
		str.append("Having:");
		Expression havingExpr = plainSelect.getHaving();
		if(havingExpr!=null){
			str.append(havingExpr);	
		}else{
			str.append("NA");		
		}
	   System.out.println(str);
		
		
		
	}

	public void createCommand(String query){
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			CreateTable createTbl = (CreateTable) parserManager.parse(new StringReader(query));
			String tableName = createTbl.getTable().getName();
			if(!isTableExists(tableName)){
				List<ColumnDefinition> tblDefinations = createTbl.getColumnDefinitions();
				createTableMetaData(tableName,tblDefinations);
				displayCreateTblOutput(tableName,tblDefinations);
				createCSVFile(tableName);
				updateDBConfig(tableName,tblDefinations);
			}else{
				System.err.print("Table Already Exist");
			}
		}catch (JSQLParserException e) {
			e.printStackTrace();
		}
		
		
	}
	private void updateDBConfig(String tableName,
			List<ColumnDefinition> tblDefinations) {
		FileWriter filewriter = null;
		BufferedWriter bufferedwriter = null;
		try {
			 filewriter = new FileWriter(CONFIGPATH,true);
			  bufferedwriter = new BufferedWriter(filewriter);
				StringBuilder str = new StringBuilder();
				str.append("BEGIN");
				str.append("\n");
				str.append(tableName);
				str.append("\n");
				for(ColumnDefinition defn:tblDefinations){
					str.append(defn.getColumnName()+","+defn.getColDataType());
					str.append("\n");
				}
				str.append("END");
				bufferedwriter.write(str.toString());
				bufferedwriter.flush();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		finally{
			try {
				bufferedwriter.close();
				filewriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}

	private void createCSVFile(String tableName) {
		String fileName = tableName+"."+FILE_EXTENSION;
		String csvFilePath = "resources"+File.separator+"db"+File.separator;
		File f = new File(csvFilePath+File.separator+fileName);
		try {
			f.createNewFile();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}

	private void displayCreateTblOutput(String tableName,
			List<ColumnDefinition> tblDefinations) {
		StringBuilder str = new StringBuilder();
		for(ColumnDefinition defn:tblDefinations){
			str.append(defn.getColumnName()+" "+defn.getColDataType());
			str.append(",");
		}
		str.deleteCharAt(str.length()-1);
		System.out.println("Querytype:create");
		System.out.println("Tablename:"+tableName);
		System.out.println("Attributes:"+str);
		
	}

	private boolean isTableExists(String tableName) {
		return tableNamesList.contains(tableName);
	}
	
	private void createTableMetaData(String tableName,List<ColumnDefinition> tableDefns){
		String fileName = tableName+"."+TABLE_METADATA_EXTENSION;
		String metadataPath="resources"+File.separator+"db"+File.separator;
		File f = new File(metadataPath+fileName);
		StringBuilder str = new StringBuilder();
		for(ColumnDefinition defn:tableDefns){
			str.append(defn.getColumnName()+":"+defn.getColDataType());
			str.append(",");
		}
		str.deleteCharAt(str.length()-1);
		FileOutputStream stream = null;
		try {
			f.createNewFile();
			stream = new FileOutputStream(f);
			stream.write(str.toString().getBytes());
		} catch (IOException e) {
			
			e.printStackTrace();
		}finally{
			try {
				stream.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
	}
	

	public void readConfig(String configFilePath)
	{
		tableNamesList = new ArrayList<String>();
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

}
