package analytics.util.objects;

import java.io.Serializable;

public class TagMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String mdTag;
	private String businessUnit;
	private String subBusinessUnit;
	private String purchaseOccasion;
	private String first5CharMdTag;
	private Double percentile;
	private String emailOptIn;
	private String divLine;
	private String textOptIn;
	private int priority;
	private int sendDuration;
	private int daysToCheckInHistory;

	
	public TagMetadata() {}
	
	/**
	 * @param mdTag
	 * @param businessUnit
	 * @param subBusinessUnit
	 * @param purchaseOccasion
	 * @param first5CharMdTag
	 * @param percentile
	 * @param emailOptIn
	 * @param divLine
	 * @param priority
	 * @param sendDuration
	 * @param daysToCheckInHistory
	 */
	public TagMetadata(String mdTag, String businessUnit,
			String subBusinessUnit, String purchaseOccasion,
			String first5CharMdTag, Double percentile, String emailOptIn,
			String divLine, int priority, int sendDuration, int daysToCheckInHistory) {
		this.mdTag = mdTag;
		this.businessUnit = businessUnit;
		this.subBusinessUnit = subBusinessUnit;
		this.purchaseOccasion = purchaseOccasion;
		this.first5CharMdTag = first5CharMdTag;
		this.percentile = percentile;
		this.emailOptIn = emailOptIn;
		this.divLine = divLine;
		this.priority = priority;
		this.sendDuration = sendDuration;
		this.daysToCheckInHistory = daysToCheckInHistory;
	}

	public TagMetadata(String mdTag, String bu, String subBu, String occName) {
		this.mdTag = mdTag;
		this.businessUnit = bu;
		this.subBusinessUnit = subBu;
		this.purchaseOccasion = occName;
	}
	
	/**
	 * @return the textOptIn
	 */
	public String getTextOptIn() {
		return textOptIn;
	}

	/**
	 * @param textOptIn the textOptIn to set
	 */
	public void setTextOptIn(String textOptIn) {
		this.textOptIn = textOptIn;
	}

	/**
	 * @return the divLine
	 */
	public String getDivLine() {
		return divLine;
	}

	/**
	 * @param divLine the divLine to set
	 */
	public void setDivLine(String divLine) {
		this.divLine = divLine;
	}

	public Double getPercentile() {
		return percentile;
	}

	public void setPercentile(Double percentile) {
		this.percentile = percentile;
	}

	public String getFirst5CharMdTag() {
		return first5CharMdTag;
	}

	public void setFirst5CharMdTag(String first5CharMdTag) {
		this.first5CharMdTag = first5CharMdTag;
	}

	public String getMdTag() {
		return mdTag;
	}

	public void setMdTag(String mdTag) {
		this.mdTag = mdTag;
	}

	public String getBusinessUnit() {
		return businessUnit;
	}

	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}

	public String getSubBusinessUnit() {
		return subBusinessUnit;
	}

	public void setSubBusinessUnit(String subBusinessUnit) {
		this.subBusinessUnit = subBusinessUnit;
	}

	public String getPurchaseOccasion() {
		return purchaseOccasion;
	}

	public void setPurchaseOccassion(String purchaseOccasion) {
		this.purchaseOccasion = purchaseOccasion;
	}

	public void setEmailOptIn(String emailOptIn) {
		this.emailOptIn = emailOptIn;
		
	}
	
	public String getEmailOptIn(){
		return emailOptIn;
	}
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public int getSendDuration() {
		return sendDuration;
	}

	public void setSendDuration(int sendDuration) {
		this.sendDuration = sendDuration;
	}

	public int getDaysToCheckInHistory() {
		return daysToCheckInHistory;
	}

	public void setDaysToCheckInHistory(int daysToCheckInHistory) {
		this.daysToCheckInHistory = daysToCheckInHistory;
	}

}
