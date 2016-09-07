	package analytics.util.objects;

import java.util.Map;

public class Model {

	private int modelId;
	private String modelName;
	private int month;
	private double constant;
	private String modelCode;
	private double seasonalConstant;
	private Map<String, Variable> variables;
	
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

	/**
	 * @return the modelId
	 */
	public int getModelId() {
		return modelId;
	}

	/**
	 * @param modelId the modelId to set
	 */
	public void setModelId(int modelId) {
		this.modelId = modelId;
	}

	/**
	 * @return the modelName
	 */
	public String getModelName() {
		return modelName;
	}

	/**
	 * @param modelName the modelName to set
	 */
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	/**
	 * @return the month
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * @param month the month to set
	 */
	public void setMonth(int month) {
		this.month = month;
	}

	/**
	 * @return the constant
	 */
	public double getConstant() {
		return constant;
	}

	/**
	 * @param constant the constant to set
	 */
	public void setConstant(double constant) {
		this.constant = constant;
	}

	/**
	 * @return the modelCode
	 */
	public String getModelCode() {
		return modelCode;
	}

	/**
	 * @param modelCode the modelCode to set
	 */
	public void setModelCode(String modelCode) {
		this.modelCode = modelCode;
	}

	/**
	 * @return the seasonalConstant
	 */
	public double getSeasonalConstant() {
		return seasonalConstant;
	}

	/**
	 * @param seasonalConstant the seasonalConstant to set
	 */
	public void setSeasonalConstant(double seasonalConstant) {
		this.seasonalConstant = seasonalConstant;
	}

	/**
	 * @return the variables
	 */
	public Map<String, Variable> getVariables() {
		return variables;
	}

	/**
	 * @param variables the variables to set
	 */
	public void setVariables(Map<String, Variable> variables) {
		this.variables = variables;
	}
}
