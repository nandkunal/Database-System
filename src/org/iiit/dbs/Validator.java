package org.iiit.dbs;

import java.util.List;

/*
 * This class Validates if Table exists in Database or Attributes for a Tables Exists in Database or not
 */
public class Validator {
	
	
	public static boolean isTableExists(String tableName){
		return DBConfigReader.getInstance().getTableNamesList().contains(tableName);
		
	}
	
	public static boolean isColumnExistsInTable(String tableName,String columnName) 
	{
		List<String> colNames = DBConfigReader.getInstance().getTableColsListMap().get(tableName);
		if(colNames!=null){
			if(colNames.contains(removeSpaces(columnName))){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
		
		
	}

	private static String removeSpaces(String str)
	{
		return str.replaceAll("\\s+","");
	}
	

}
