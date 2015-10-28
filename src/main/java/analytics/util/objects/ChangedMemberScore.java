package analytics.util.objects;

import java.io.Serializable;

public class ChangedMemberScore implements Serializable{

	private static final long serialVersionUID = 1L;
	private double score;
	private String minDate;
	private String maxDate;
	private String effDate;
	private String source;
	private String modelId;
	
	//Additional parameters to emit to logging bolt
	private String lId;
	private String messageID;
	
	public ChangedMemberScore(){
		
	}
	public ChangedMemberScore(double score, String minDate, String maxDate, String effDate, String source){
		this.score = score;
		this.minDate = minDate;
		this.maxDate = maxDate;
		this.effDate = effDate;
		this.source = source;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	public String getMinDate() {
		return minDate;
	}
	public void setMinDate(String minDate) {
		this.minDate = minDate;
	}
	public String getMaxDate() {
		return maxDate;
	}
	public void setMaxDate(String maxDate) {
		this.maxDate = maxDate;
	}
	public String getEffDate() {
		return effDate;
	}
	public void setEffDate(String effDate) {
		this.effDate = effDate;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public String getlId() {
		return lId;
	}
	public void setlId(String lId) {
		this.lId = lId;
	}
	public String getMessageID() {
		return messageID;
	}
	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}
}
