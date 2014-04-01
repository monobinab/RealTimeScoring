package analytics.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class TransactionLineItem {
	public String div;
	public String item;
	public String line;
	public boolean searsCardUsed;
	public double amount;
	
	public TransactionLineItem() {
		
	}
	
	public TransactionLineItem(String d, String i, float a) {
		this.div=d;
		this.item=i;
		this.amount=a;
	}
	
	public void setDiv(String d) {
		this.div=d;
	}
	
	public void setItem(String i) {
		this.item=i;
	}
	
	public void setAmount(double a) {
		this.amount=a;
	}
	
	public void setLine(DBCollection divLnItmCollection) {
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", this.div);
		queryLine.put("i", this.item);
		
		DBCursor divLnItm = divLnItmCollection.find(queryLine);
		this.line = divLnItm.next().get("l").toString();
		
	}
	
	
	public String getDiv() {
		return this.div;
	}
	
	public String getLine() {
		return this.line;
	}
	
	public String getItem() {
		return this.item;
	}
	
	public double getAmount() {
		return this.amount;
	}
	
	
}
