package analytics.util;

import java.io.Serializable;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class TransactionLineItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String hashed;
	public String div;
	public String item;
	public String line;
	public boolean searsCardUsed;
	public double amount;
	public List<String> variableList;
	
	public TransactionLineItem() {
		
	}
	
	public TransactionLineItem(String hashed, String d, String i) {
		this.hashed=hashed;
		this.div=d;
		this.item=i;
	}
	
	public TransactionLineItem(String d, String i) {
		this.div=d;
		this.item=i;
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
	
	public void setLine(String l) {
		this.line=l;
	}
	
	public boolean setLineFromCollection(DBCollection divLnItmCollection) {
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", this.div);
		queryLine.put("i", this.item);
		
		DBObject divLnItm = divLnItmCollection.findOne(queryLine);
		
		if(divLnItm.keySet().isEmpty()) {
			return false;
		}
		this.line = divLnItm.get("l").toString();
		return true;
	}
	
	public void setVariableList(List<String> v) {
		this.variableList = v;
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

	public String getHashed() {
		return this.hashed;
	}

	public List<String> getVariableList() {
		return this.variableList;
	}
	
	
}
