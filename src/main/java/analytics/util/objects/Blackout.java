package analytics.util.objects;

public class Blackout {

	private boolean blackoutFlag;
	String blackoutVariables;
	
	public boolean isBlackoutFlag() {
		return blackoutFlag;
	}
	public void setBlackoutFlag(boolean blackoutFlag) {
		this.blackoutFlag = blackoutFlag;
	}
	public String getBlackoutVariables() {
		return blackoutVariables;
	}
	public void setBlackoutVariables(String blackoutVariables) {
		this.blackoutVariables = blackoutVariables;
	}

	
}
