package analytics.util.objects;

import java.util.List;
import java.util.Map;

public class MemberBrowse {

	private String lId;
	Map<String, List<BrowseTag>> browseTags;
	
	public String getlId() {
		return lId;
	}
	public void setlId(String lId) {
		this.lId = lId;
	}
	public Map<String, List<BrowseTag>> getBrowseTags() {
		return browseTags;
	}
	public void setBrowseTags(Map<String, List<BrowseTag>> browseTags) {
		this.browseTags = browseTags;
	}
	
}
