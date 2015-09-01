package analytics.util.objects;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class ScoresInfo {

	@Expose
	private String modelId;
	@Expose
	private String modelName;
	@Expose
	private String format;
	@Expose
	private String category;
	@Expose
	private String tag;
	@Expose
	private String mdTag;
	@Expose
	private String occassion;
	@Expose
	private String subBusinessUnit;
	@Expose
	private String businessUnit;
	@Expose
	private String scoreDate;
	@Expose
	private Double score;
	@Expose
	private Integer percentile;
	@Expose
	private Integer rank;
	@Expose
	private Double index;
	@Expose
	private String buTag;

	/**
	 * 
	 * @return The modelId
	 */
	public String getModelId() {
		return modelId;
	}

	/**
	 * 
	 * @param modelId
	 *            The modelId
	 */
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	/**
	 * 
	 * @return The modelName
	 */
	public String getModelName() {
		return modelName;
	}

	/**
	 * 
	 * @param modelName
	 *            The modelName
	 */
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	/**
	 * 
	 * @return The format
	 */
	public String getFormat() {
		return format;
	}

	/**
	 * 
	 * @param format
	 *            The format
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	/**
	 * 
	 * @return The category
	 */
	public String getCategory() {
		return category;
	}

	/**
	 * 
	 * @param category
	 *            The category
	 */
	public void setCategory(String category) {
		this.category = category;
	}

	/**
	 * 
	 * @return The tag
	 */
	public String getTag() {
		return tag;
	}

	/**
	 * 
	 * @param tag
	 *            The tag
	 */
	public void setTag(String tag) {
		this.tag = tag;
	}

	/**
	 * 
	 * @return The mdTag
	 */
	public String getMdTag() {
		return mdTag;
	}

	/**
	 * 
	 * @param mdTag
	 *            The mdTag
	 */
	public void setMdTag(String mdTag) {
		this.mdTag = mdTag;
	}

	/**
	 * 
	 * @return The occassion
	 */
	public String getOccassion() {
		return occassion;
	}

	/**
	 * 
	 * @param occassion
	 *            The occassion
	 */
	public void setOccassion(String occassion) {
		this.occassion = occassion;
	}

	/**
	 * 
	 * @return The subBusinessUnit
	 */
	public String getSubBusinessUnit() {
		return subBusinessUnit;
	}

	/**
	 * 
	 * @param subBusinessUnit
	 *            The subBusinessUnit
	 */
	public void setSubBusinessUnit(String subBusinessUnit) {
		this.subBusinessUnit = subBusinessUnit;
	}

	/**
	 * 
	 * @return The businessUnit
	 */
	public String getBusinessUnit() {
		return businessUnit;
	}

	/**
	 * 
	 * @param businessUnit
	 *            The businessUnit
	 */
	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}

	/**
	 * 
	 * @return The scoreDate
	 */
	public String getScoreDate() {
		return scoreDate;
	}

	/**
	 * 
	 * @param scoreDate
	 *            The scoreDate
	 */
	public void setScoreDate(String scoreDate) {
		this.scoreDate = scoreDate;
	}

	/**
	 * 
	 * @return The score
	 */
	public Double getScore() {
		return score;
	}

	/**
	 * 
	 * @param score
	 *            The score
	 */
	public void setScore(Double score) {
		this.score = score;
	}

	/**
	 * 
	 * @return The percentile
	 */
	public Integer getPercentile() {
		return percentile;
	}

	/**
	 * 
	 * @param percentile
	 *            The percentile
	 */
	public void setPercentile(Integer percentile) {
		this.percentile = percentile;
	}

	/**
	 * 
	 * @return The rank
	 */
	public Integer getRank() {
		return rank;
	}

	/**
	 * 
	 * @param rank
	 *            The rank
	 */
	public void setRank(Integer rank) {
		this.rank = rank;
	}

	/**
	 * 
	 * @return The index
	 */
	public Double getIndex() {
		return index;
	}

	/**
	 * 
	 * @param index
	 *            The index
	 */
	public void setIndex(Double index) {
		this.index = index;
	}

	/**
	 * @return the buTag
	 */
	public String getBuTag() {
		return buTag;
	}

	/**
	 * @param buTag the buTag to set
	 */
	public void setBuTag(String buTag) {
		this.buTag = buTag;
	}
}