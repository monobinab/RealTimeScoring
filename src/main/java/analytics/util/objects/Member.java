package analytics.util.objects;

import java.util.List;

public class Member {
	
	String lId; 
	List<ChangedMemberScore> changedMemberScoreList;
	List<Change> changedMemberVariablesList;
	public String getlId() {
		return lId;
	}
	public void setlId(String lId) {
		this.lId = lId;
	}
	public List<ChangedMemberScore> getChangedMemberScoreList() {
		return changedMemberScoreList;
	}
	public void setChangedMemberScoreList(
			List<ChangedMemberScore> changedMemberScoreList) {
		this.changedMemberScoreList = changedMemberScoreList;
	}
	public List<Change> getChangedMemberVariablesList() {
		return changedMemberVariablesList;
	}
	public void setChangedMemberVariablesList(
			List<Change> changedMemberVariablesList) {
		this.changedMemberVariablesList = changedMemberVariablesList;
	}
	

}
