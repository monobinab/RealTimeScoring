/**
 * 
 */
package analytics.util.objects;

import java.io.Serializable;

/**
 * @author spannal
 *
 */
public class VariableModel implements Serializable{
	
	private static final long serialVersionUID = 1L;
	String variable;
	int modelId;
	String score;
	/**
	 * @return the variable
	 */
	public String getVariable() {
		return variable;
	}
	/**
	 * @param variable the variable to set
	 */
	public void setVariable(String variable) {
		this.variable = variable;
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
	 * @return the score
	 */
	public String getScore() {
		return score;
	}
	/**
	 * @param score the score to set
	 */
	public void setScore(String score) {
		this.score = score;
	}
	
}
