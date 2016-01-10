package analytics.util.objects;

import java.util.List;

public class ModelBoosts {
	private int modelId;
	private String modelName;
	List<Boost> boostLists;
	
	public List<Boost> getBoostLists() {
		return boostLists;
	}
	public void setBoostLists(List<Boost> boostLists) {
		this.boostLists = boostLists;
	}
	public int getModelId() {
		return modelId;
	}
	public void setModelId(int modelId) {
		this.modelId = modelId;
	}
	public String getModelName() {
		return modelName;
	}
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
		
}