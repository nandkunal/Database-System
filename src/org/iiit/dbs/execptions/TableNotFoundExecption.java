package org.iiit.dbs.execptions;

public class TableNotFoundExecption extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName;
	public TableNotFoundExecption(String tableName){
		this.tableName=tableName;
	}
	public String getMessage(){
		return "Table '"+tableName+"' doesnot exists";
	}
	
	

}
