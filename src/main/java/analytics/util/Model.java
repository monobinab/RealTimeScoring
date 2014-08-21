package analytics.util;

import java.util.Collection;

public class Model {

	int modelId;
	int month;
	double constant;
	Collection<Variable> variables;
	
	public Model() {}
	
	public Model(int modId, int mth, double cnst) {
		this.modelId = modId;
		this.month = mth;
		this.constant = cnst;
	}
	
	public Model(int modId, int mth, double cnst, Collection<Variable> vars) {
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

	public Collection<Variable> getVariables() {
		return this.variables;
	}

}
