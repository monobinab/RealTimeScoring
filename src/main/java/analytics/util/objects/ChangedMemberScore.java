package analytics.util.objects;

public class ChangedMemberScore {
	private double score;
	private String minDate;
	private String maxDate;
	private String effDate;
	private String source;
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
	public String getMinDate() {
		return minDate;
	}
	public String getMaxDate() {
		return maxDate;
	}
	public String getEffDate() {
		return effDate;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	
}
