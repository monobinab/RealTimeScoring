package analytics.service.impl;

/**
 * @author ddas1
 *
 */

public class OrderDetails {

	private String orderId;
	private String orderSource;
	private String totalTransactions;
	private String transactionSequence;
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getOrderSource() {
		return orderSource;
	}
	public void setOrderSource(String orderSource) {
		this.orderSource = orderSource;
	}
	public String getTotalTransactions() {
		return totalTransactions;
	}
	public void setTotalTransactions(String totalTransactions) {
		this.totalTransactions = totalTransactions;
	}
	public String getTransactionSequence() {
		return transactionSequence;
	}
	public void setTransactionSequence(String transactionSequence) {
		this.transactionSequence = transactionSequence;
	}
}
