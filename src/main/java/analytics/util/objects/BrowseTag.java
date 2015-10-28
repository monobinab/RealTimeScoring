package analytics.util.objects;

import java.util.Map;

public class BrowseTag {
	
	private String browseTag;
	Map<String, Object> feedCounts;
	public String getBrowseTag() {
		return browseTag;
	}
	public void setBrowseTag(String browseTag) {
		this.browseTag = browseTag;
	}
	public Map<String, Object> getFeedCounts() {
		return feedCounts;
	}
	public void setFeedCounts(Map<String, Object> feedCounts) {
		this.feedCounts = feedCounts;
	}
	
	
}
