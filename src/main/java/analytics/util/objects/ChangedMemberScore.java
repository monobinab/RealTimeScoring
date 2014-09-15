package analytics.util.objects;

public class ChangedMemberScore {
	private double score;
	private String minDate;
	private String maxDate;
	private String effDate;
	public ChangedMemberScore(double score, String minDate, String maxDate, String effDate){
		this.score = score;
		this.minDate = minDate;
		this.maxDate = maxDate;
		this.effDate = effDate;
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
	
}
