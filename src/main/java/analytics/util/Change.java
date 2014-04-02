package analytics.util;

import java.util.Date;

public class Change {
	public String variableName;
	public Object value;
	public Date expirationDate;
	
	public Change() {}
	
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
}
