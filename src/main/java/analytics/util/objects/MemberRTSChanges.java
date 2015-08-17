package analytics.util.objects;

import java.util.List;
import java.util.Map;

public class MemberRTSChanges {
	
	private String lId; 
	private List<ChangedMemberScore> changedMemberScoreList;
	private Map<String, Change> allChangesMap;
	private String source;
	
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
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
	public Map<String, Change> getAllChangesMap() {
		return allChangesMap;
	}
	public void setAllChangesMap(Map<String, Change> allChangesMap) {
		this.allChangesMap = allChangesMap;
	}

	

}
