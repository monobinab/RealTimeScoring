package analytics.util.objects;

public class ModelScoreInfo {

	private String modelId;
	private String modelName;
	private String format;
	private String category;
	private String tag;
	private String mdTag;
	private String occasion;
	private String brand;
	private String subBusinessUnit;
	private String businessUnit;
	private String scoredate;
	private String score;
	private String percentile;
	private String rank;
	
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public String getModelName() {
		return modelName;
	}
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getMdTag() {
		return mdTag;
	}
	public void setMdTag(String mdTag) {
		this.mdTag = mdTag;
	}
	public String getOccasion() {
		return occasion;
	}
	public void setOccasion(String occasion) {
		this.occasion = occasion;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getSubBusinessUnit() {
		return subBusinessUnit;
	}
	public void setSubBusinessUnit(String subBusinessUnit) {
		this.subBusinessUnit = subBusinessUnit;
	}
	public String getBusinessUnit() {
		return businessUnit;
	}
	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}
	public String getScoredate() {
		return scoredate;
	}
	public void setScoredate(String scoredate) {
		this.scoredate = scoredate;
	}
	public String getScore() {
		return score;
	}
	public void setScore(String score) {
		this.score = score;
	}
	public String getPercentile() {
		return percentile;
	}
	public void setPercentile(String percentile) {
		this.percentile = percentile;
	}
	public String getRank() {
		return rank;
	}
	public void setRank(String rank) {
		this.rank = rank;
	}
	
	@Override
	public String toString() {
		return "ModelScoreInfo [modelId=" + modelId + ", modelName="
				+ modelName + ", format=" + format + ", category=" + category
				+ ", tag=" + tag + ", mdTag=" + mdTag + ", occasion="
				+ occasion + ", brand=" + brand + ", subBusinessUnit="
				+ subBusinessUnit + ", businessUnit=" + businessUnit
				+ ", scoredate=" + scoredate + ", score=" + score
				+ ", percentile=" + percentile + ", rank=" + rank + "]";
	}
	
	
	
	
	
	

}
