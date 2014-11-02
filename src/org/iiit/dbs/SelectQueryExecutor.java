package org.iiit.dbs;

import java.io.IOException;
import java.util.List;

import org.iiit.dbs.execptions.TableNotFoundExecption;
import org.iiit.dbs.execptions.UnknownColumnException;

public class SelectQueryExecutor {
	
	
	private QueryAttributes attributes;
	private DBSystem db;
	
	
	public SelectQueryExecutor(QueryAttributes attributes,DBSystem db){
		this.attributes=attributes;
		this.db=db;
	}
	
	public void executeQuery() throws TableNotFoundExecption, UnknownColumnException, IOException{
		for(String tableName :attributes.getTableNames()){
			if(!Validator.isTableExists(tableName)){
				throw new TableNotFoundExecption(tableName);
			}
			if(attributes.getColumnNames().size()==1 && attributes.getColumnNames().get(0).equalsIgnoreCase("*") && attributes.getLeftWhereColumnName()!=null
					&& attributes.getRightWhereExpValue()!=null)
			{
				displayAllRowsByWhereCondition(tableName,attributes.getLeftWhereColumnName(),attributes.getRightWhereExpValue(),attributes.getWhereClauseOperator());
			}
			else if(attributes.getColumnNames().size()==1 && attributes.getColumnNames().get(0).equalsIgnoreCase("*") )
			{
				displayAllRows(tableName);
			}
			else if(attributes.getColumnNames().size()>0){
				displayAllRowsWithColumns(tableName,attributes.getColumnNames());
			}
		}
	}


	private void displayAllRowsByWhereCondition(String tableName,
			String leftWhereColumnName, String rightWhereExpValue,String operator) throws UnknownColumnException, IOException {
		
		  if(!Validator.isColumnExistsInTable(tableName, leftWhereColumnName))
		  {
			  throw new UnknownColumnException(leftWhereColumnName, tableName);	
		  }
		  //TODO Add Validation of Expression Type in LHS and RHS
		  db.getAllRecordsByWhereCondition(tableName, leftWhereColumnName,rightWhereExpValue,operator);
		  
	}

	private void displayAllRowsWithColumns(String tableName,List<String> columnNames) throws TableNotFoundExecption, UnknownColumnException, IOException {
		for(String cols :columnNames)
		{
			if(!Validator.isColumnExistsInTable(tableName, cols))
			{
			   throw new UnknownColumnException(cols, tableName);	
			}
		}
		db.getAllRecordsByColName(tableName,columnNames);
		
	}

	private void displayAllRows(String tableName) {
		long start=System.currentTimeMillis();
		System.out.println("Displaying All Records");
		try {
			db.getAllRecords(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end=System.currentTimeMillis();
		long diff=end-start;
		float timelag=diff;
		System.out.print(timelag+" milliseconds");
		
		
		
		
	}

}
