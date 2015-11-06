package analytics.util.objects;

public class OccasionInfo {

	private String occasionId;
	private String occasion;
	private String priority;
	private String duration;
	private String daysToCheckInHistory;
	private String intCustEvent;
	private int custEventId;
	private String tagExpiresIn;
	
	/**
	 * @return the occasionId
	 */
	public String getOccasionId() {
		return occasionId;
	}
	/**
	 * @param occasionId the occasionId to set
	 */
	public void setOccasionId(String occasionId) {
		this.occasionId = occasionId;
	}
	/**
	 * @return the occasion
	 */
	public String getOccasion() {
		return occasion;
	}
	/**
	 * @param occasion the occasion to set
	 */
	public void setOccasion(String occasion) {
		this.occasion = occasion;
	}
	/**
	 * @return the priority
	 */
	public String getPriority() {
		return priority;
	}
	/**
	 * @param priority the priority to set
	 */
	public void setPriority(String priority) {
		this.priority = priority;
	}
	/**
	 * @return the duration
	 */
	public String getDuration() {
		return duration;
	}
	/**
	 * @param duration the duration to set
	 */
	public void setDuration(String duration) {
		this.duration = duration;
	}
	/**
	 * @return the intCustEvent
	 */
	public String getIntCustEvent() {
		return intCustEvent;
	}
	/**
	 * @param intCustEvent the intCustEvent to set
	 */
	public void setIntCustEvent(String intCustEvent) {
		this.intCustEvent = intCustEvent;
	}
	/**
	 * @return the custEventId
	 */
	public int getCustEventId() {
		return custEventId;
	}
	/**
	 * @param custEventId the custEventId to set
	 */
	public void setCustEventId(int custEventId) {
		this.custEventId = custEventId;
	}
	
	public String getDaysToCheckInHistory() {
		return daysToCheckInHistory;
	}
	public void setDaysToCheckInHistory(String daysToCheckInHistory) {
		this.daysToCheckInHistory = daysToCheckInHistory;
	}
	/**
	 * @return the tagExpiresIn
	 */
	public String getTagExpiresIn() {
		return tagExpiresIn;
	}
	/**
	 * @param tagExpiresIn the tagExpiresIn to set
	 */
	public void setTagExpiresIn(String tagExpiresIn) {
		this.tagExpiresIn = tagExpiresIn;
	}
}
