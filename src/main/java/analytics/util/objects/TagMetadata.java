package analytics.util.objects;

import java.io.Serializable;

public class TagMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String mdTags;
	private String businessUnit;
	private String subBusinessUnit;
	private String purchaseOccasion;

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
