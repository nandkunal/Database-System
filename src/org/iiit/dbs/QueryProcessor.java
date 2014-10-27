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

import org.iiit.dbs.execptions.TableNotFoundExecption;
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
	
	private static final String CONFIGPATH="resources/config.txt";
	private static final String TABLE_METADATA_EXTENSION="data";
	private static final String FILE_EXTENSION="csv";
	
	
	public QueryProcessor(){
		
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
			displaySelectCommand(selectStmt);
		} catch (JSQLParserException e) {
			System.err.print("Invalid Query");
			e.printStackTrace();
		}
		
	}

	private void displaySelectAllCommand(Select selectStmt) {
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
		System.out.println(str.toString());
		
	}

	private void displaySelectCommand(Select selectStmt) {
		QueryAttributes attr=new QueryAttributes();
		TablesNamesFinder tableFinder = new TablesNamesFinder();
		List<String> tableList = tableFinder.getTableList(selectStmt);
		attr.setTableNames(tableList);
		PlainSelect plainSelect = (PlainSelect)selectStmt.getSelectBody();
		List selectElements = plainSelect.getSelectItems();
		List<String>columnNames=new ArrayList<String>();
		if(selectElements.get(0).toString().equalsIgnoreCase("*")){
			
			columnNames.add("*");
		}else{
		for(int i=0;i<selectElements.size();i++){
			SelectExpressionItem expressionItem = (SelectExpressionItem)selectElements.get(i);
			if(expressionItem.getExpression() instanceof Column){
			Column col = (Column)expressionItem.getExpression();
			columnNames.add(col.getColumnName());
			}else if(expressionItem.getExpression() instanceof Parenthesis){
				Parenthesis par = (Parenthesis)expressionItem.getExpression();
				Column col = (Column)par.getExpression();
				columnNames.add(col.getColumnName());
			}
		}
		
		//get Distinct Columns
		}
		attr.setColumnNames(columnNames);
		List<String> distinctColNames= new ArrayList<String>();
		if(plainSelect.getDistinct()!=null){
			for(int i=0;i<selectElements.size();i++){
				SelectExpressionItem expressionItem = (SelectExpressionItem)selectElements.get(i);
				if(expressionItem.getExpression() instanceof Column){
				Column col = (Column)expressionItem.getExpression();
				distinctColNames.add(col.getColumnName());
				}else if(expressionItem.getExpression() instanceof Parenthesis){
					Parenthesis par = (Parenthesis)expressionItem.getExpression();
					Column col = (Column)par.getExpression();
					distinctColNames.add(col.getColumnName());
				}
			}
		}else{
			distinctColNames.add("NA");
		}
		attr.setDistinctColumnName(distinctColNames);
		//get Where Condition
		
		attr.setConditionStatement((plainSelect.getWhere()==null)?"NA":plainSelect.getWhere().toString());
		//get By Order By
		List<String>orderByColNames=new ArrayList<String>();
		List orderByElements = plainSelect.getOrderByElements();
		if(orderByElements!=null){
		for(int j=0;j<orderByElements.size();j++){
		OrderByElement orderElem = (OrderByElement)orderByElements.get(j);	
		if(orderElem.getExpression() instanceof Column){
		Column col = (Column)orderElem.getExpression();
		orderByColNames.add(col.getColumnName());
		}
		}
		
		}else{
			orderByColNames.add("NA");
		}
		attr.setOrderByColumnName(orderByColNames);
		
		//Groupby
		List<String>groupByCols= new ArrayList<>();
		List groupBy = plainSelect.getGroupByColumnReferences();
		if(groupBy!=null){
		for(int i=0;i<groupBy.size();i++){
			Column col = (Column) groupBy.get(i);
			groupByCols.add(col.getColumnName());
		}
		}else{
			groupByCols.add("NA");	
		}
		
		Expression havingExpr = plainSelect.getHaving();
		if(havingExpr!=null){
			attr.setHavingStatement(havingExpr.toString());	
		}else{
			attr.setHavingStatement("NA");		
		}
	   
		SelectQueryExecutor queryExecutor=new SelectQueryExecutor(attr);
		try {
			queryExecutor.executeQuery();
		} catch (TableNotFoundExecption e) {
			System.out.println(e.getMessage());
		}
		
		
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
				//Update the values of DBConfigReader
				DBConfigReader.getInstance().readConfig(CONFIGPATH);
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
		return DBConfigReader.getInstance().getTableNamesList().contains(tableName);
	}
	
	private void createTableMetaData(String tableName,List<ColumnDefinition> tableDefns){
		String fileName = tableName+"."+TABLE_METADATA_EXTENSION;
		String metadataPath=DBConfigReader.getInstance().getPathTables()+File.separator;
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
	



}
