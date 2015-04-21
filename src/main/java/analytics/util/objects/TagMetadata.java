package analytics.util.objects;

import java.io.Serializable;
import java.util.Map;

public class TagMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String mdTags;
	private String businessUnit;
	private String subBusinessUnit;
	private String purchaseOccasion;
	private String first5CharMdTag;
	private Double percentile;
	

	public Double getPercentile() {
		return percentile;
	}

	public void setPercentile(Double percentile) {
		this.percentile = percentile;
	}

	public String getFirst5CharMdTag() {
		return first5CharMdTag;
	}

	public void setFirst5CharMdTag(String first5CharMdTag) {
		this.first5CharMdTag = first5CharMdTag;
	}

	public String getMdTags() {
		return mdTags;
	}

	public void setMdTags(String mdTags) {
		this.mdTags = mdTags;
	}

	public String getBusinessUnit() {
		return businessUnit;
	}

	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}

	public String getSubBusinessUnit() {
		return subBusinessUnit;
	}

	public void setSubBusinessUnit(String subBusinessUnit) {
		this.subBusinessUnit = subBusinessUnit;
	}

	public String getPurchaseOccasion() {
		return purchaseOccasion;
	}

	public void setPurchaseOccassion(String purchaseOccasion) {
		this.purchaseOccasion = purchaseOccasion;
	}
}
