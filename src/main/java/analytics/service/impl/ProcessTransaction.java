package analytics.service.impl;

import java.util.List;

public class ProcessTransaction {
	
	//Refer soap_env.xml for creating TO(Transfer Object) 
	private String messageVersion;
	private String memberNumber;
	private String requestorID;
	private String orderStoreNumber;
	private String tenderStoreNumber;
	private String registerNumber;
	private String transactionNumber;
	private String transactionTotal;
	private String transactionTotalTax;
	private String transactionDate;
	private String transactionTime;
	private String associateID;
	private String earnFlag;
	private List<LineItem> lineItemList; 
	
	public String getMessageVersion() {
		return messageVersion;
	}
	public void setMessageVersion(String messageVersion) {
		this.messageVersion = messageVersion;
	}
	public String getMemberNumber() {
		return memberNumber;
	}
	public void setMemberNumber(String memberNumber) {
		this.memberNumber = memberNumber;
	}
	public String getRequestorID() {
		return requestorID;
	}
	public void setRequestorID(String requestorID) {
		this.requestorID = requestorID;
	}
	public String getOrderStoreNumber() {
		return orderStoreNumber;
	}
	public void setOrderStoreNumber(String orderStoreNumber) {
		this.orderStoreNumber = orderStoreNumber;
	}
	public String getTenderStoreNumber() {
		return tenderStoreNumber;
	}
	public void setTenderStoreNumber(String tenderStoreNumber) {
		this.tenderStoreNumber = tenderStoreNumber;
	}
	public String getRegisterNumber() {
		return registerNumber;
	}
	public void setRegisterNumber(String registerNumber) {
		this.registerNumber = registerNumber;
	}
	public String getTransactionNumber() {
		return transactionNumber;
	}
	public void setTransactionNumber(String transactionNumber) {
		this.transactionNumber = transactionNumber;
	}
	public String getTransactionTotal() {
		return transactionTotal;
	}
	public void setTransactionTotal(String transactionTotal) {
		this.transactionTotal = transactionTotal;
	}
	public String getTransactionTotalTax() {
		return transactionTotalTax;
	}
	public void setTransactionTotalTax(String transactionTotalTax) {
		this.transactionTotalTax = transactionTotalTax;
	}
	public String getTransactionDate() {
		return transactionDate;
	}
	public void setTransactionDate(String transactionDate) {
		this.transactionDate = transactionDate;
	}
	public String getTransactionTime() {
		return transactionTime;
	}
	public void setTransactionTime(String transactionTime) {
		this.transactionTime = transactionTime;
	}
	public String getAssociateID() {
		return associateID;
	}
	public void setAssociateID(String associateID) {
		this.associateID = associateID;
	}
	public String getEarnFlag() {
		return earnFlag;
	}
	public void setEarnFlag(String earnFlag) {
		this.earnFlag = earnFlag;
	}
	public List<LineItem> getLineItemList() {
		return lineItemList;
	}
	public void setLineItemList(List<LineItem> lineItemList) {
		this.lineItemList = lineItemList;
	}

}
