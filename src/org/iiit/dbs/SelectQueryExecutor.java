package org.iiit.dbs;

import org.iiit.dbs.execptions.TableNotFoundExecption;

public class SelectQueryExecutor {
	
	
	private QueryAttributes attributes;
	private DBSystem db;
	
	
	public SelectQueryExecutor(QueryAttributes attributes,DBSystem db){
		this.attributes=attributes;
		this.db=db;
	}
	
	public void executeQuery() throws TableNotFoundExecption{
		for(String tableName :attributes.getTableNames()){
			if(!Validator.isTableExists(tableName)){
				throw new TableNotFoundExecption(tableName);
			}
			if(attributes.getColumnNames().size()==1 && attributes.getColumnNames().get(0).equalsIgnoreCase("*") )
			{
				displayAllRows(tableName);
			}
		}
	}

	private void displayAllRows(String tableName) {
		long start=System.currentTimeMillis();
		System.out.println("Displaying All Records");
		db.getAllRecords("countries");
		long end=System.currentTimeMillis();
		long diff=end-start;
		float timelag=diff;
		System.out.print(timelag+" milliseconds");
		
		
		
		
	}

}
