package analytics.util.objects;

import java.util.Date;

public class EmailPackage {	
	
	private TagMetadata mdTagMetaData;
	private Date addedDateTime;
	private Date sendDate;
	private Date sentDateTime;
	private Integer status;
	
	//below fields are needed in the construction of xml to responsys
	private String memberId;	
	private MemberInfo memberInfo;
	private String custEventNm;	
	private String topologyName;
	
	public EmailPackage() {		
		this.addedDateTime = new Date(System.currentTimeMillis());
		this.status = 0; //0 - ToBeSent, 1-Sent, 2-SendFailed		
	}
	
	/**
	 * @param mdTagMetaData
	 * @param addedDateTime
	 * @param sendDate
	 * @param sentDateTime
	 * @param status
	 * @param memberId
	 * @param memberInfo
	 * @param custEventNm
	 * @param topologyName
	 */
	public EmailPackage(TagMetadata mdTagMetaData, Date addedDateTime,
			Date sendDate, Date sentDateTime, Integer status,
			String memberId, MemberInfo memberInfo, String custEventNm,
			String topologyName) {
		this.mdTagMetaData = mdTagMetaData;
		this.addedDateTime = addedDateTime;
		this.sendDate = sendDate;
		this.sentDateTime = sentDateTime;
		this.status = status;
		this.memberId = memberId;
		this.memberInfo = memberInfo;
		this.custEventNm = custEventNm;
		this.topologyName = topologyName;
	}
	
	public EmailPackage(String memberId,TagMetadata mdTagMetaData){
		this.memberId = memberId;
		this.mdTagMetaData = mdTagMetaData;
		this.addedDateTime = new Date(System.currentTimeMillis());
		this.status = 0; //0 - ToBeSent, 1-Sent, 2-SendFailed			
	}
	
	public EmailPackage(String memberId, TagMetadata mdTagMetaData, Date addedDateTime, Date sendDate,
			Date sentDateTime, Integer status) {
		super();
		this.memberId = memberId;
		this.mdTagMetaData = mdTagMetaData;
		this.addedDateTime = addedDateTime;
		this.sendDate = sendDate;
		this.sentDateTime = sentDateTime;
		this.status = status;
	}	

	public EmailPackage(String memberId, TagMetadata tagMetadata, Date addedDateTime, Date sendDate, int status) {
		super();
		this.memberId = memberId;
		this.mdTagMetaData = tagMetadata;
		this.addedDateTime = addedDateTime;
		this.sendDate = sendDate;
		this.status = status;
	}

	public String getMemberId() {
		return memberId;
	}

	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	
	public TagMetadata getMdTagMetaData() {
		return mdTagMetaData;
	}

	public void setMdTagMetaData(TagMetadata mdTagMetaData) {
		this.mdTagMetaData = mdTagMetaData;
	}

	public Date getAddedDateTime() {
		return addedDateTime;
	}

	public void setAddedDateTime(Date addedDateTime) {
		this.addedDateTime = addedDateTime;
	}

	public Date getSendDate() {
		return sendDate;
	}

	public void setSendDate(Date sendDate) {
		this.sendDate = sendDate;
	}

	public Date getSentDateTime() {
		return sentDateTime;
	}

	public void setSentDateTime(Date sentDateTime) {
		this.sentDateTime = sentDateTime;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}
	
	public MemberInfo getMemberInfo() {
		return memberInfo;
	}

	public void setMemberInfo(MemberInfo memberInfo) {
		this.memberInfo = memberInfo;
	}

	public String getCustEventNm() {
		return custEventNm;
	}

	public void setCustEventNm(String custEventNm) {
		this.custEventNm = custEventNm;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EmailPackage [mdTagMetaData=");
		builder.append(mdTagMetaData);
		builder.append(", addedDateTime=");
		builder.append(addedDateTime);
		builder.append(", sendDate=");
		builder.append(sendDate);
		builder.append(", sentDateTime=");
		builder.append(sentDateTime);
		builder.append(", status=");
		builder.append(status);
		builder.append(", memberId=");
		builder.append(memberId);
		builder.append(", memberInfo=");
		builder.append(memberInfo);
		builder.append(", custEventNm=");
		builder.append(custEventNm);
		builder.append(", topologyName=");
		builder.append(topologyName);
		builder.append("]");
		return builder.toString();
	}
}
