package analytics.util.objects;

import java.util.List;
import java.util.Map;

public class DC {
	private List<String> tags;
	private Map<String, Integer> boostStrengthMap;
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public Map<String, Integer> getBoostStrengthMap() {
		return boostStrengthMap;
	}
	public void setBoostStrengthMap(Map<String, Integer> boostStrengthMap) {
		this.boostStrengthMap = boostStrengthMap;
	}
	

}
