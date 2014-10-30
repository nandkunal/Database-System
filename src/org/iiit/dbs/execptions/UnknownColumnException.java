package org.iiit.dbs.execptions;

public class UnknownColumnException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String columnName;
	private String tableName;
	
	public UnknownColumnException(String columnName, String tableName)
	{
		this.columnName = columnName;
		this.tableName = tableName;
	}
	public String getMessage()
	{
		return "Unknown column ' "+columnName+" ' in table "+ tableName;
	}

}
