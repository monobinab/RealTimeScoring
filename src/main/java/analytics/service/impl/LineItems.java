package analytics.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.trident.RedisState;

public class LineItems implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(LineItems.class);
	public List<Object> transactionLineItemList;

	public LineItems(List<Object> l) {
		this.transactionLineItemList = new ArrayList<Object>();
		this.transactionLineItemList=l;
	}
}
