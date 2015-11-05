package analytics.util.objects;

import java.io.Serializable;
import java.util.List;

public class ModelVariable implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private int modelId;
	private String modelDescription;
	private Double constant;
	private String modelName;
	private int month;
	private List<Variable> variable;
	
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
	 * @return the modelDescription
	 */
	public String getModelDescription() {
		return modelDescription;
	}
	/**
	 * @param modelDescription the modelDescription to set
	 */
	public void setModelDescription(String modelDescription) {
		this.modelDescription = modelDescription;
	}
	/**
	 * @return the constant
	 */
	public Double getConstant() {
		return constant;
	}
	/**
	 * @param constant the constant to set
	 */
	public void setConstant(Double constant) {
		this.constant = constant;
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
	 * @return the variable
	 */
	public List<Variable> getVariable() {
		return variable;
	}
	/**
	 * @param variable the variable to set
	 */
	public void setVariable(List<Variable> variable) {
		this.variable = variable;
	}
}
