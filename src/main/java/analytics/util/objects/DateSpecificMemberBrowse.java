package analytics.util.objects;

import java.util.Map;

public class DateSpecificMemberBrowse {

	private String l_id;
	Map<String,Map<String,Integer>> buSubBu;
	private String date;
	
	public String getL_id() {
		return l_id;
	}

	public void setL_id(String l_id) {
		this.l_id = l_id;
	}

	public Map<String, Map<String, Integer>> getBuSubBu() {
		return buSubBu;
	}

	public void setBuSubBu(Map<String, Map<String, Integer>> tags) {
		this.buSubBu = tags;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
}
