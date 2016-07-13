package analytics.util.objects;

import java.util.List;

import com.google.gson.JsonElement;

public class Sweep {

	private String memberId;
	private JsonElement jsonMemberId;
	private String category;
	private String subCategory;
	private List<String> modelId;
	private String priority;
	
	/**
	 * @return the memberId
	 */
	public String getMemberId() {
		return memberId;
	}
	/**
	 * @param memberId the memberId to set
	 */
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	/**
	 * @return the jsonMemberId
	 */
	public JsonElement getJsonMemberId() {
		return jsonMemberId;
	}
	/**
	 * @param jsonMemberId the jsonMemberId to set
	 */
	public void setJsonMemberId(JsonElement jsonMemberId) {
		this.jsonMemberId = jsonMemberId;
	}
	/**
	 * @return the category
	 */
	public String getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(String category) {
		this.category = category;
	}
	/**
	 * @return the subCategory
	 */
	public String getSubCategory() {
		return subCategory;
	}
	/**
	 * @param subCategory the subCategory to set
	 */
	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}
	/**
	 * @return the modelId
	 */
	public List<String> getModelId() {
		return modelId;
	}
	/**
	 * @param modelId the modelId to set
	 */
	public void setModelId(List<String> modelId) {
		this.modelId = modelId;
	}
	/**
	 * @return the priority
	 */
	public String getPriority() {
		return priority;
	}
	/**
	 * @param priority the priority to set
	 */
	public void setPriority(String priority) {
		this.priority = priority;
	}
}
