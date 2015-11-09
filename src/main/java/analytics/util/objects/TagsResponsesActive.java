package analytics.util.objects;

import java.io.Serializable;

public class TagsResponsesActive implements Serializable{

	private static final long serialVersionUID = 1L;
	private String bu;
	private String occ;
	
	/**
	 * @return the bu
	 */
	public String getBu() {
		return bu;
	}
	/**
	 * @param bu the bu to set
	 */
	public void setBu(String bu) {
		this.bu = bu;
	}
	/**
	 * @return the occ
	 */
	public String getOcc() {
		return occ;
	}
	/**
	 * @param occ the occ to set
	 */
	public void setOcc(String occ) {
		this.occ = occ;
	}
}
