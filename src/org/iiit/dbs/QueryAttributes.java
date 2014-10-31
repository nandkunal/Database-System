package org.iiit.dbs;

import java.util.List;

public class QueryAttributes {

	private List<String> tableNames;
	private List<String> columnNames;
	private List<String> distinctColumnName;
	private List<String> orderByColumnName;
	private List<String> groupByColumnName;
	private String leftWhereColumnName;
	private String rightWhereExpValue;
	private String whereClauseOperator;
	private String havingStatement;
	
	public List<String> getTableNames() {
		return tableNames;
	}
	public void setTableNames(List<String> tableNames) {
		this.tableNames = tableNames;
	}
	public List<String> getColumnNames() {
		return columnNames;
	}
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	public List<String> getDistinctColumnName() {
		return distinctColumnName;
	}
	public void setDistinctColumnName(List<String> distinctColumnName) {
		this.distinctColumnName = distinctColumnName;
	}
	public List<String> getOrderByColumnName() {
		return orderByColumnName;
	}
	public void setOrderByColumnName(List<String> orderByColumnName) {
		this.orderByColumnName = orderByColumnName;
	}
	public List<String> getGroupByColumnName() {
		return groupByColumnName;
	}
	public void setGroupByColumnName(List<String> groupByColumnName) {
		this.groupByColumnName = groupByColumnName;
	}
	public String getHavingStatement() {
		return havingStatement;
	}
	public void setHavingStatement(String havingStatement) {
		this.havingStatement = havingStatement;
	}
	public String getLeftWhereColumnName() {
		return leftWhereColumnName;
	}
	public void setLeftWhereColumnName(String leftWhereColumn) {
		this.leftWhereColumnName = leftWhereColumn;
	}
	public String getWhereClauseOperator() {
		return whereClauseOperator;
	}
	public void setWhereClauseOperator(String whereClauseOperator) {
		this.whereClauseOperator = whereClauseOperator;
	}
	public String getRightWhereExpValue() {
		return rightWhereExpValue;
	}
	public void setRightWhereExpValue(String rightWhereExpValue) {
		this.rightWhereExpValue = rightWhereExpValue;
	}
	@Override
	public String toString() {
		return "QueryAttributes [tableNames=" + tableNames + ", columnNames="
				+ columnNames + ", distinctColumnName=" + distinctColumnName
				+ ", orderByColumnName=" + orderByColumnName
				+ ", groupByColumnName=" + groupByColumnName
				+ ", leftWhereColumnName=" + leftWhereColumnName
				+ ", rightWhereExpValue=" + rightWhereExpValue
				+ ", whereClauseOperator=" + whereClauseOperator
				+ ", havingStatement=" + havingStatement + "]";
	}
	
	
}
