package analytics.util.objects;

import java.io.Serializable;

public class TagMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String mdTags;
	private String businessUnit;

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

	public String getSubBussinessUnit() {
		return subBussinessUnit;
	}

	public void setSubBussinessUnit(String subBussinessUnit) {
		this.subBussinessUnit = subBussinessUnit;
	}

	public String getPurchaseOccassion() {
		return purchaseOccassion;
	}

	public void setPurchaseOccassion(String purchaseOccassion) {
		this.purchaseOccassion = purchaseOccassion;
	}

	private String subBussinessUnit;
	private String purchaseOccassion;
}
