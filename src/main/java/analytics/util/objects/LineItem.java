/**
 * 
 */
package analytics.util.objects;

/**
 * @author ddas1
 *
 */
public class LineItem {
	
	private String lineNumber;
	private String itemType;
	private String division;
	private String itemNumber;
	private String lineItemAmountTypeCode;
	private String dollarValuePreDisc;
	private String dollarValuePostDisc;
	private String priceMatchAmount;
	private String priceMatchBonusAmount;
	private String quantity;
	private String taxAmount;
	private String postSalesAdjustmentAmount;
	public String getLineNumber() {
		return lineNumber;
	}
	public void setLineNumber(String lineNumber) {
		this.lineNumber = lineNumber;
	}
	public String getItemType() {
		return itemType;
	}
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	public String getDivision() {
		return division;
	}
	public void setDivision(String division) {
		this.division = division;
	}
	public String getItemNumber() {
		return itemNumber;
	}
	public void setItemNumber(String itemNumber) {
		this.itemNumber = itemNumber;
	}
	public String getLineItemAmountTypeCode() {
		return lineItemAmountTypeCode;
	}
	public void setLineItemAmountTypeCode(String lineItemAmountTypeCode) {
		this.lineItemAmountTypeCode = lineItemAmountTypeCode;
	}
	public String getDollarValuePreDisc() {
		return dollarValuePreDisc;
	}
	public void setDollarValuePreDisc(String dollarValuePreDisc) {
		this.dollarValuePreDisc = dollarValuePreDisc;
	}
	public String getDollarValuePostDisc() {
		return dollarValuePostDisc;
	}
	public void setDollarValuePostDisc(String dollarValuePostDisc) {
		this.dollarValuePostDisc = dollarValuePostDisc;
	}
	public String getPriceMatchAmount() {
		return priceMatchAmount;
	}
	public void setPriceMatchAmount(String priceMatchAmount) {
		this.priceMatchAmount = priceMatchAmount;
	}
	public String getPriceMatchBonusAmount() {
		return priceMatchBonusAmount;
	}
	public void setPriceMatchBonusAmount(String priceMatchBonusAmount) {
		this.priceMatchBonusAmount = priceMatchBonusAmount;
	}
	public String getQuantity() {
		return quantity;
	}
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}
	public String getTaxAmount() {
		return taxAmount;
	}
	public void setTaxAmount(String taxAmount) {
		this.taxAmount = taxAmount;
	}
	public String getPostSalesAdjustmentAmount() {
		return postSalesAdjustmentAmount;
	}
	public void setPostSalesAdjustmentAmount(String postSalesAdjustmentAmount) {
		this.postSalesAdjustmentAmount = postSalesAdjustmentAmount;
	}

}
