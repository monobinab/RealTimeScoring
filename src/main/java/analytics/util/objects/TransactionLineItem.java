package analytics.util.objects;

import java.io.Serializable;
import java.util.List;

public class TransactionLineItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String l_id;
	public String div;
	public String item;
	public String lineOrCategory;//Based on sears or Kmart
//	public boolean searsCardUsed;
	public double amount;
	public List<String> variableList;
	
	public TransactionLineItem() {
		
	}
	
	public TransactionLineItem(String id) {
		this.l_id=id;
	}

	public TransactionLineItem(String id, String d, String i, String l, double a) {
		this.l_id=id;
		this.div=d;
		this.item=i;
		this.lineOrCategory=l;
		this.amount=a;
	}
	
	public TransactionLineItem(String id, String d, String i) {
		this.l_id=id;
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
	
	public void setLineOrCategory(String l) {
		this.lineOrCategory=l;
	}
	
	public void setVariableList(List<String> v) {
		this.variableList = v;
	}
	
	
	public String getDiv() {
		return this.div;
	}
	
	public String getLineOrCategory() {
		return this.lineOrCategory;
	}
	
	public String getItem() {
		return this.item;
	}
	
	public double getAmount() {
		return this.amount;
	}

	public String getL_id() {
		return this.l_id;
	}

	public List<String> getVariableList() {
		return this.variableList;
	}
	
	
}
