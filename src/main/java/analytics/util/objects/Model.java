	package analytics.util.objects;

import java.util.Map;

public class Model {

	int modelId;
	String modelName;
	int month;
	double constant;
	String modelCode;
	double seasonalConstant;
	Map<String, Variable> variables;
	
	public Model() {}
	
	public Model(int modId, String modelName, int mth, double cnst) {
		this.modelName = modelName;
		this.modelId = modId;
		this.month = mth;
		this.constant = cnst;
	}
	
	public Model(int modId, String modelName, int mth, double cnst, Map<String, Variable> vars) {
		this.modelName = modelName;
		this.modelId = modId;
		this.month = mth;
		this.constant = cnst;
		this.variables = vars;
	}

	public Model(int modelId, String modelName, String modelCode,
			double seasonalConstant) {
		super();
		this.modelId = modelId;
		this.modelName = modelName;
		this.modelCode = modelCode;
		this.seasonalConstant = seasonalConstant;
	}

	public Model(int modId, String modelName,  Map<String, Variable> vars) {
		this.modelName = modelName;
		this.modelId = modId;
		this.variables = vars;
	}

	public Model(int modelId, String modelName, String modelCode) {
		super();
		this.modelId = modelId;
		this.modelName = modelName;
		this.modelCode = modelCode;
	}

	public int getModelId() {
		return this.modelId;
	}
	
	public int getMonth() {
		return this.month;
	}

	public double getConstant() {
		return this.constant;
	}

	public Map<String, Variable> getVariables() {
		return this.variables;
	}
	
	public String getModelName() {
		return this.modelName;
	}
	
	public double getSeasonalConstant(){
		return this.seasonalConstant;
	}

}
