package analytics.util.objects;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Change {
	private String vid;
	private Object value;
	private Date expirationDate;
	private Date effectivDate;
	
	public Change() {}
	
	private static SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public Change(String vid, Object val, Date expDate, Date effDate) {
		this.vid = vid;
		this.value = val;
		this.expirationDate = expDate;
		this.effectivDate = effDate;
	}
	
	public Change(String vid, Object val, Date expDate) {
		this.vid = vid;
		this.value = val;
		this.expirationDate = expDate;
		this.effectivDate = new Date();
	}
	
	public Change(Object val, Date expDate) {
		this.value = val;
		this.expirationDate = expDate;
	}
	
	public void setChangeVariable(String varNm) {
		this.vid = varNm;
	}
	
	public String getChangeVariable() {
		return this.vid;
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

	public Object getEffectiveDateAsString() {
		if(this.effectivDate != null) {
			return dtFormat.format(this.effectivDate);
		}
		else return dtFormat.format(new Date());
	}
}
