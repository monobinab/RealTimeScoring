package analytics.util.objects;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

public class APIResponse {
	
	private String status;
	private String statusCode;
	private String memberId;
	private Date lastUpdated;
	private ModelScoreInfo[] scoresInfo;
	
	public APIResponse(String status, String statusCode, String memberId,
			Date lastUpdated, ModelScoreInfo[] scoresInfo) {
		super();
		this.status = status;
		this.statusCode = statusCode;
		this.memberId = memberId;
		this.lastUpdated = lastUpdated;
		this.scoresInfo = scoresInfo;
	}

	public APIResponse() {
		super();		
	}
	
	public String getMemberId() {
		return memberId;
	}

	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}

	public ModelScoreInfo[] getScoresInfo() {
		return scoresInfo;
	}

	public void setScoresInfo(ModelScoreInfo[] scoresInfo) {
		this.scoresInfo = scoresInfo;
	}
	
	@Override
	public String toString() {
		return "APIResponse [status=" + status + ", statusCode=" + statusCode
				+ ", memberId=" + memberId + ", lastUpdated=" + lastUpdated
				+ ", scoresInfo=" + Arrays.toString(scoresInfo) + "]";
	}
	

}
