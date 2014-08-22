package analytics.util.objects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LineItems implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public List<Object> transactionLineItemList;

	public LineItems(List<Object> l) {
		this.transactionLineItemList = new ArrayList<Object>();
		this.transactionLineItemList=l;
	}
}
