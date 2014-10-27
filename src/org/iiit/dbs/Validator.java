package org.iiit.dbs;
/*
 * This class Validates if Table exists in Database or Attributes for a Tables Exists in Database or not
 */
public class Validator {
	
	
	public static boolean isTableExists(String tableName){
		return DBConfigReader.getInstance().getTableNamesList().contains(tableName);
		
	}
	

}
