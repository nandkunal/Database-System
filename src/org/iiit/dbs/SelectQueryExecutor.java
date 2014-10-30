package org.iiit.dbs;

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
	
	public void executeQuery() throws TableNotFoundExecption, UnknownColumnException{
		for(String tableName :attributes.getTableNames()){
			if(!Validator.isTableExists(tableName)){
				throw new TableNotFoundExecption(tableName);
			}
			if(attributes.getColumnNames().size()==1 && attributes.getColumnNames().get(0).equalsIgnoreCase("*") )
			{
				displayAllRows(tableName);
			}else{
				displayAllRowsWithColumns(tableName,attributes.getColumnNames());
			}
		}
	}

	private void displayAllRowsWithColumns(String tableName,List<String> columnNames) throws TableNotFoundExecption, UnknownColumnException {
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
		db.getAllRecords(tableName);
		long end=System.currentTimeMillis();
		long diff=end-start;
		float timelag=diff;
		System.out.print(timelag+" milliseconds");
		
		
		
		
	}

}
