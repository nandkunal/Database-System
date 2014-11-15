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
	private String joinType;
	private boolean hasJoins=false;
	private String joinTables;
	private String joinLeftTableName;
	private String joinRightTableName;
	private String joinLeftColumnName;
	private String joinRightColumnName;
	private String joinOperator;
	
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
				+ ", havingStatement=" + havingStatement + ", joinType="
				+ joinType + "]";
	}
	public String getJoinType() {
		return joinType;
	}
	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}
	public boolean isHasJoins() {
		return hasJoins;
	}
	public void setHasJoins(boolean hasJoins) {
		this.hasJoins = hasJoins;
	}
	public String getJoinTables() {
		return joinTables;
	}
	public void setJoinTables(String joinTables) {
		this.joinTables = joinTables;
	}
	public String getJoinLeftTableName() {
		return joinLeftTableName;
	}
	public void setJoinLeftTableName(String joinLeftTableName) {
		this.joinLeftTableName = joinLeftTableName;
	}
	public String getJoinRightTableName() {
		return joinRightTableName;
	}
	public void setJoinRightTableName(String joinRightTableName) {
		this.joinRightTableName = joinRightTableName;
	}
	public String getJoinLeftColumnName() {
		return joinLeftColumnName;
	}
	public void setJoinLeftColumnName(String joinLeftColumnName) {
		this.joinLeftColumnName = joinLeftColumnName;
	}
	public String getJoinRightColumnName() {
		return joinRightColumnName;
	}
	public void setJoinRightColumnName(String joinRightColumnName) {
		this.joinRightColumnName = joinRightColumnName;
	}
	public String getJoinOperator() {
		return joinOperator;
	}
	public void setJoinOperator(String joinOperator) {
		this.joinOperator = joinOperator;
	}
	
	
}
