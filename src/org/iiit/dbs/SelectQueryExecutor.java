package org.iiit.dbs;

import java.util.List;

import org.iiit.dbs.execptions.TableNotFoundExecption;

public class SelectQueryExecutor {
	
	
	private QueryAttributes attributes;
	
	
	public SelectQueryExecutor(QueryAttributes attributes){
		this.attributes=attributes;
	}
	
	public void executeQuery() throws TableNotFoundExecption{
		for(String tableName :attributes.getTableNames()){
			if(!Validator.isTableExists(tableName)){
				throw new TableNotFoundExecption(tableName);
			}
			if(attributes.getColumnNames().size()==1 && attributes.getColumnNames().get(0).equalsIgnoreCase("*") )
			{
				displayAllRows();
			}
		}
	}

	private void displayAllRows() {
		System.out.println("Display All Records");
		
	}

}
