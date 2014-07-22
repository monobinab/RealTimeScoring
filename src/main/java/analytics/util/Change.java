package analytics.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Change {
	public String variableName;
	public Object value;
	public Date expirationDate;
	
	public Change() {}
	
	private static SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public Change(String varName, Object val, Date expDate) {
		this.variableName = varName;
		this.value = val;
		this.expirationDate = expDate;
	}
	
	public Change(Object val, Date expDate) {
		this.value = val;
		this.expirationDate = expDate;
	}
	
	public void setChangeVariable(String varNm) {
		this.variableName = varNm;
	}
	
	public String getChangeVariable() {
		return this.variableName;
	}
	
	public void setValue(Object val) {
		this.value = val;
	}
	
	public Object getValue() {
		return this.value;
	}
	
	public void setExpirationDate(Date expDate) {
		this.expirationDate = expDate;
	}
	
	public Date getExpirationDate() {
		return this.expirationDate;
	}
	
	public String getExpirationDateAsString() {
		return dtFormat.format(this.expirationDate);
	}
}