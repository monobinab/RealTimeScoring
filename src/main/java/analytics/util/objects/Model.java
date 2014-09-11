package analytics.util.objects;

import java.util.Map;

public class Model {

	int modelId;
	int month;
	double constant;
	Map<String, Variable> variables;
	
	public Model() {}
	
	public Model(int modId, int mth, double cnst) {
		this.modelId = modId;
		this.month = mth;
		this.constant = cnst;
	}
	
	public Model(int modId, int mth, double cnst, Map<String, Variable> vars) {
		this.modelId = modId;
		this.month = mth;
		this.constant = cnst;
		this.variables = vars;
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

}
