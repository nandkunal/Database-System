package org.iiit.dbs;

public class Page 
{
	int startRecord;
	int endRecord;
	int offSet;
	public Page() 
	{
		this.startRecord = 0;
		this.endRecord = 0;
		this.offSet = 0 ;
	}
	@Override
	public String toString() {
		return "Page [startRecord=" + startRecord + ", endRecord=" + endRecord
				+ ", offSet=" + offSet + "]";
	}
	

}
