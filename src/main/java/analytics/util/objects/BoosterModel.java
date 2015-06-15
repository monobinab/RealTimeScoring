package analytics.util.objects;

import java.util.Map;

public class BoosterModel {

	int modelId;
	String modelName;
	int month;
	double constant;
	Map<String, Double> boosterVariablesMap;

	public Map<String, Double> getBoosterVariablesMap() {
		return boosterVariablesMap;
	}
	public void setBoosterVariablesMap(Map<String, Double> boosterVariablesMap) {
		this.boosterVariablesMap = boosterVariablesMap;
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
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public double getConstant() {
		return constant;
	}
	public void setConstant(double constant) {
		this.constant = constant;
	}

}