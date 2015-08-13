package analytics.util.objects;

public class ChangedMemberScore {
	private double score;
	private String minDate;
	private String maxDate;
	private String effDate;
	private String source;
	private String modelId;
	
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
	
	
}
