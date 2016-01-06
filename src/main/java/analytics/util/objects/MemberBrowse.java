package analytics.util.objects;

import java.io.Serializable;
import java.util.Map;

public class MemberBrowse implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String l_id;
	Map<String, Map<String,Map<String,Integer>>> dateSpecificBuSubBu;
	public String getL_id() {
		return l_id;
	}

	public void setL_id(String l_id) {
		this.l_id = l_id;
	}

	public Map<String, Map<String, Map<String, Integer>>> getDateSpecificBuSubBu() {
		return dateSpecificBuSubBu;
	}

	public void setDateSpecificBuSubBu(
			Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu) {
		this.dateSpecificBuSubBu = dateSpecificBuSubBu;
	}


	
}
