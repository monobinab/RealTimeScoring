package analytics.util.objects;

import java.io.Serializable;

public class OccCustEvent implements Serializable{

	private static final long serialVersionUID = 1L;
	private String occasion;
	private String intCustEvent;
	private int custEventId;
	
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
}
