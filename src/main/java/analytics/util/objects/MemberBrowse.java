package analytics.util.objects;

import java.util.Map;

public class MemberBrowse {
	
	private String l_id;
	
	public String getL_id() {
		return l_id;
	}

	public void setL_id(String l_id) {
		this.l_id = l_id;
	}

	Map<String, DateSpecificMemberBrowse> memberBrowseMap;

	public Map<String, DateSpecificMemberBrowse> getMemberBrowse() {
		return memberBrowseMap;
	}

	public void setMemberBrowse(
			Map<String, DateSpecificMemberBrowse> memberBrowseMap) {
		this.memberBrowseMap = memberBrowseMap;
	}
	
}
