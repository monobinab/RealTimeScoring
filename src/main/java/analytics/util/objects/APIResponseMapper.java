package analytics.util.objects;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("org.jsonschema2pojo")
public class APIResponseMapper {

	@Expose
	private String status;
	@Expose
	private String statusCode;
	@Expose
	private String memberId;
	@Expose
	private String lastUpdated;
	@Expose
	private List<ScoresInfo> scoresInfo = new ArrayList<ScoresInfo>();

	/**
	 * 
	 * @return The status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * 
	 * @param status
	 *            The status
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * 
	 * @return The statusCode
	 */
	public String getStatusCode() {
		return statusCode;
	}

	/**
	 * 
	 * @param statusCode
	 *            The statusCode
	 */
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}

	/**
	 * 
	 * @return The memberId
	 */
	public String getMemberId() {
		return memberId;
	}

	/**
	 * 
	 * @param memberId
	 *            The memberId
	 */
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}

	/**
	 * 
	 * @return The lastUpdated
	 */
	public String getLastUpdated() {
		return lastUpdated;
	}

	/**
	 * 
	 * @param lastUpdated
	 *            The lastUpdated
	 */
	public void setLastUpdated(String lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	/**
	 * 
	 * @return The scoresInfo
	 */
	public List<ScoresInfo> getScoresInfo() {
		return scoresInfo;
	}

	/**
	 * 
	 * @param scoresInfo
	 *            The scoresInfo
	 */
	public void setScoresInfo(List<ScoresInfo> scoresInfo) {
		this.scoresInfo = scoresInfo;
	}

}