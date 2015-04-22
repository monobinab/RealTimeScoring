package analytics.util.objects;

public class Responsys {

	private String lyl_id_no;
	private String l_id;
	private org.json.JSONObject jsonObj;
	private TagMetadata tagMetadata;
	private String eid;
	private String customEventName;
	private String topologyName;
	
	
	public String getTopologyName() {
		return topologyName;
	}
	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}
	public String getCustomEventName() {
		return customEventName;
	}
	public void setCustomEventName(String customEventName) {
		this.customEventName = customEventName;
	}
	public String getLyl_id_no() {
		return lyl_id_no;
	}
	public void setLyl_id_no(String lyl_id_no) {
		this.lyl_id_no = lyl_id_no;
	}
	public String getL_id() {
		return l_id;
	}
	public void setL_id(String l_id) {
		this.l_id = l_id;
	}
	public org.json.JSONObject getJsonObj() {
		return jsonObj;
	}
	public void setJsonObj(org.json.JSONObject jsonObj) {
		this.jsonObj = jsonObj;
	}
	public TagMetadata getTagMetadata() {
		return tagMetadata;
	}
	public void setTagMetadata(TagMetadata tagMetadata) {
		this.tagMetadata = tagMetadata;
	}
	public String getEid() {
		return eid;
	}
	public void setEid(String eid) {
		this.eid = eid;
	}
	
	
}
