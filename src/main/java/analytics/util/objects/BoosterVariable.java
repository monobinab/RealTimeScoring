package analytics.util.objects;

import org.bson.types.ObjectId;

public class BoosterVariable {

	private static final long serialVersionUID = 1L;

	private String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getBvid() {
		return bvid;
	}
	public void setBvid(String bvid) {
		this.bvid = bvid;
	}
	public String getStrategy() {
		return strategy;
	}
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}
	private String bvid;
		private String strategy;
}