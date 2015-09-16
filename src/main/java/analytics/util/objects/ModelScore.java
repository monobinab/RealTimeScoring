/**
 * 
 */
package analytics.util.objects;

import com.google.gson.annotations.Expose;

/**
 * @author spannal
 *
 */
public class ModelScore {
	
	@Expose
	private String modelId;
	@Expose
	private double score;
	@Expose
	private int percentile;
	
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	/**
	* 
	* @return
	* The percentile
	*/
	public int getPercentile() {
	return percentile;
	}

	/**
	* 
	* @param percentile
	* The percentile
	*/
	public void setPercentile(int percentile) {
	this.percentile = percentile;
	}


	
}
