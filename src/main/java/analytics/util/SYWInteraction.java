package analytics.util;

import java.util.List;

public class SYWInteraction {
	//Below variable names do not follow conventional lowerCase start 
	//because the json does not and Gson fails to convert to object if the cases are different
	private String InteractionId;
	private int UserId;
	private int UserSearsId;
	private List<SYWEntity> Entities;
	private String InteractionType;
	private String Time;
	private String Client;
	private Object AdditionalData;
	public String getInteractionId() {
		return InteractionId;
	}
	public void setInteractionId(String InteractionId) {
		this.InteractionId = InteractionId;
	}
	public int getUserId() {
		return UserId;
	}
	public void setUserId(int userId) {
		this.UserId = userId;
	}
	public int getSearsUserId() {
		return UserSearsId;
	}
	public void setSearsUserId(int searsUserId) {
		this.UserSearsId = searsUserId;
	}
	public List<SYWEntity> getEntities() {
		return Entities;
	}
	public void setEntities(List<SYWEntity> entities) {
		this.Entities = entities;
	}
	public String getType() {
		return InteractionType;
	}
	public void setType(String type) {
		this.InteractionType = type;
	}
	public String getTimestamp() {
		return Time;
	}
	public void setTimestamp(String timestamp) {
		this.Time = timestamp;
	}
	public String getClient() {
		return Client;
	}
	public void setClient(String client) {
		this.Client = client;
	}
	public Object getAdditionalData() {
		return AdditionalData;
	}
	public void setAdditionalData(Object additionalData) {
		this.AdditionalData = additionalData;
	}
	@Override
	public String toString() {
	   return "SYWInteraction [InteractionId=" + InteractionId + ", UserId=" + UserId + ", UserSearsId="+ UserSearsId + ", Entities=" + Entities +
			   ", InteractionType=" + InteractionType + ", Time=" + Time + ", Client=" + Client + ", AdditionalData=" + AdditionalData  + "]";

	}

}

