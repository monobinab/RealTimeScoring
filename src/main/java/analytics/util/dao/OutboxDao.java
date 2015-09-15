package analytics.util.dao;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.exception.RealTimeScoringException;
import analytics.util.Constants;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.OccasionInfo;
import analytics.util.objects.TagMetadata;

public class OutboxDao extends AbstractMySQLDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OutboxDao.class);
	public List<EmailPackage> getQueuedEmailPackages(String lyl_id_no, List<OccasionInfo> occasionsInfo) throws RealTimeScoringException {
		// This method returns all pending emails from the outbox
		List<EmailPackage>emlPackageList = new ArrayList<EmailPackage>();
			StringBuilder query = new StringBuilder();
			query.append("SELECT * ")
				.append("FROM cp_outbox ")
				.append("WHERE loy_id= ? AND status=0;");
        	PreparedStatement statement;
			try {
				statement = connection.prepareStatement(query.toString());
				statement.setString(1, lyl_id_no);
				ResultSet rs = statement.executeQuery();
		        
		        while (rs.next()) {
		             //System.out.println("loyalty id: " + rs.getString("loy_id"));
		             TagMetadata tagMetadata = new TagMetadata(rs.getString("md_tag"),rs.getString("bu"),rs.getString("sub_bu"),rs.getString("occasion_name"));
		             for(OccasionInfo occasion :occasionsInfo){
		            	 if(rs.getString("occasion_name").equals(occasion.getOccasion()))
		            	 {
		            		 tagMetadata.setPriority(Integer.parseInt(occasion.getPriority()));
		    	             tagMetadata.setSendDuration(Integer.parseInt(occasion.getDuration()));
		    	             emlPackageList.add(new EmailPackage(rs.getString("loy_id")
		    		             		, tagMetadata
		    		             		, rs.getTimestamp("added_datetime") /*addedDateTime*/
		    		             		, rs.getDate("send_date") /*sendDate*/	             		
		    		             		, rs.getInt("status")));
		    	             break;
		            	 }
		             }
		            
		             
		        }
		        statement.close();
			} catch (SQLException e) {
				throw new RealTimeScoringException("SQL Exception occured in getting queued EmailPackages : errorcode - " + e.getErrorCode() + " , " + e.getMessage());
			}
					
		
		return emlPackageList;
	}

	public void queueEmailPackages(List<EmailPackage> emailPackages) throws SQLException {
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Integer queueLength =0;
		
		for(EmailPackage emlPack: emailPackages) {
			if(queueLength < Constants.CPS_QUEUE_LENGTH){
				StringBuilder query = new StringBuilder();
				query.append("INSERT INTO rts_member.cp_outbox ")
					.append("(loy_id, bu, sub_bu, md_tag, occasion_name, added_datetime, send_date, status, customer_id, sears_opt_in, kmart_opt_in, syw_opt_in) ")
					.append("VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");
		    	PreparedStatement statement = connection.prepareStatement(query.toString());
				statement.setString(1, emlPack.getMemberId());
				statement.setString(2, emlPack.getMdTagMetaData().getBusinessUnit());
				statement.setString(3, emlPack.getMdTagMetaData().getSubBusinessUnit());
				statement.setString(4, emlPack.getMdTagMetaData().getMdTag());
				statement.setString(5, emlPack.getMdTagMetaData().getPurchaseOccasion());
				statement.setTimestamp(6, new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis()));
				if(emlPack.getSendDate()!=null){
					statement.setDate(7,  new java.sql.Date(emlPack.getSendDate().getTime()));
				}
				else
					LOGGER.error("Send date is set to null for memberid: " + emlPack.getMemberId() + " with mdtag - "+ emlPack.getMdTagMetaData().getMdTag());
				statement.setString(8, emlPack.getStatus().toString());
				if(emlPack.getMemberInfo()!=null){
					if(emlPack.getMemberInfo().getEid()!=null){
						statement.setString(9, emlPack.getMemberInfo().getEid());
					}
					else
					{
						LOGGER.error("emlPack.getMemberInfo().getEid() is set to null for memberid: " + emlPack.getMemberId() + "EID is saved as empty string to outbox.");
						statement.setString(9, "");
					}
					if(emlPack.getMemberInfo().getSrs_opt_in()!=null){
						statement.setString(10, emlPack.getMemberInfo().getSrs_opt_in());
					}
					else
					{
						LOGGER.error("emlPack.getMemberInfo().getSrs_opt_in() is set to null for memberid: " + emlPack.getMemberId() + "Srs_opt_in is saved as empty string to outbox.");
						statement.setString(10, "");
					}
					if(emlPack.getMemberInfo().getKmt_opt_in()!=null){
						statement.setString(11, emlPack.getMemberInfo().getKmt_opt_in());
					}
					else
					{
						LOGGER.error("emlPack.getMemberInfo().getKmt_opt_in() is set to null for memberid: " + emlPack.getMemberId() + "Kmt_opt_in is saved as empty string to outbox.");
						statement.setString(11, "");
					}
					if(emlPack.getMemberInfo().getSyw_opt_in()!=null){
						statement.setString(12, emlPack.getMemberInfo().getSyw_opt_in());
					}
					else
					{
						LOGGER.error("emlPack.getMemberInfo().getSyw_opt_in() is set to null for memberid: " + emlPack.getMemberId() + "Syw_opt_in is saved as empty string to outbox.");
						statement.setString(12, "");
					}					
					
				}
				else{
					LOGGER.error("emlPack.getMemberInfo() is set to null for memberid: " + emlPack.getMemberId() + "Fields of memberInfo are saved as empty strings to outbox.");
					statement.setString(9, "");
					statement.setString(10,"");
					statement.setString(11, "");
					statement.setString(12, "");
				}
				
				statement.executeUpdate();	
				queueLength = queueLength+ emlPack.getMdTagMetaData().getSendDuration();
				statement.close();
			}
			else
			{
				break;
			}
			
		}
		
		
	}

/*	public void removeFromQueue(EmailPackage queuedEP) {
		// TODO Auto-generated method stub
		
	}
	public java.sql.Date getSentDate(EmailPackage emailPackage) {
		// TODO Auto-generated method stub
		return null;
	}*/

	/*public Integer getEmailPackageStatus(EmailPackage emailPackage,	Integer daysToCheck) {
		// This method is expected to return 1 if the emailPackage was sent in the past "daysToCheck"
		// It should return 0 if the emailPackage was not sent
		java.util.Calendar cal = java.util.Calendar.getInstance();
		
		if(emailPackage.getStatus() == 1 
				&& emailPackage.getSentDateTime() != null 
				// TODO: Do you want to include the send date with daysToCheck? e.g. if days to check is 1 do you include yesterday or after [today - (daysToCheck+1)]?
				&& emailPackage.getSentDateTime().after(new Date(cal.getTimeInMillis() - 24l * 60l * 60l * 1000l * (daysToCheck.longValue() + 1l)))) {
			return 1;
		} else {
			return 0;
		}
	}*/

	public void deleteQueuedEmailPackages(String lyl_id_no) throws SQLException {
		StringBuilder query = new StringBuilder();
		query.append("DELETE from rts_member.cp_outbox WHERE loy_id=? AND status=0;");
    	PreparedStatement statement = connection.prepareStatement(query.toString());
		statement.setString(1, lyl_id_no);
		statement.executeUpdate();
		statement.close();
	}

	public Date getSentDate(String lyl_id_no, String occasionName) throws SQLException {
		//this method checks if the occasion was sent - if so, return sentDate. Otherwise - returns null 
		Date sentDate = null;
		 
		StringBuilder query = new StringBuilder();
		query.append("SELECT sent_datetime ")
			.append("FROM rts_member.cp_outbox ")
			.append("WHERE loy_id=? AND status=1 AND occasion_name = ?;");
    	PreparedStatement statement = connection.prepareStatement(query.toString());
		statement.setString(1, lyl_id_no);
		statement.setString(2, occasionName);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {   
        	sentDate= rs.getTimestamp("sent_datetime"); /*sentDateTime*/
       }
        statement.close();
		return sentDate;		
	}

	public Date getSentDate(EmailPackage emailPackage) throws SQLException {
		Date sentDate = null;
		 
		StringBuilder query = new StringBuilder();
		query.append("SELECT sent_datetime ")
			.append("FROM rts_member.cp_outbox ")
			.append("WHERE loy_id=? AND status=1 AND occasion_name = ? AND bu = ? AND sub_bu = ?;");
    	PreparedStatement statement = connection.prepareStatement(query.toString());
		statement.setString(1, emailPackage.getMemberId());
		statement.setString(2, emailPackage.getMdTagMetaData().getPurchaseOccasion());
		statement.setString(3,emailPackage.getMdTagMetaData().getBusinessUnit());
		statement.setString(4,emailPackage.getMdTagMetaData().getSubBusinessUnit());
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {   
        	sentDate = rs.getTimestamp("sent_datetime"); /*sentDateTime*/
       }
        statement.close();
		return sentDate;	
	}

	public EmailPackage getInProgressPackage(String lyl_id_no, List<OccasionInfo> occasionsInfo) throws SQLException {
				
		EmailPackage inProgressOccasion = null;
		String query = "SELECT count(*) FROM rts_member.cp_outbox WHERE loy_id=? AND status=1";
		PreparedStatement statement = connection.prepareStatement(query);
		statement.setString(1, lyl_id_no);
		ResultSet rs = statement.executeQuery();
		if(rs.next() && rs.getInt(1) > 0){
			query = "SELECT bu,sub_bu,md_tag,occasion_name, added_datetime, send_date,status, max(sent_datetime) FROM rts_member.cp_outbox WHERE loy_id=? AND status=1;";
			statement = connection.prepareStatement(query);
			statement.setString(1, lyl_id_no);
			ResultSet rs1 = statement.executeQuery();
			 
			while (rs1.next()) { 
	        	inProgressOccasion = new EmailPackage();
	        	inProgressOccasion.setMemberId(lyl_id_no);
	        	TagMetadata tagMetadata = new TagMetadata(rs1.getString("md_tag"),rs1.getString("bu"),rs1.getString("sub_bu"),rs1.getString("occasion_name"));	  
	        	 for(OccasionInfo occasion :occasionsInfo){
	            	 if(rs1.getString("occasion_name").equals(occasion.getOccasion()))
	            	 {
	            		 tagMetadata.setPriority(Integer.parseInt(occasion.getPriority()));
	    	             tagMetadata.setSendDuration(Integer.parseInt(occasion.getDuration()));	
	    	             tagMetadata.setDaysToCheckInHistory(Integer.parseInt(occasion.getDaysToCheckInHistory()));
	    	             break;
	            	 }
	             }
	        	inProgressOccasion.setMdTagMetaData(tagMetadata);
	        	inProgressOccasion.setAddedDateTime( rs1.getTimestamp("added_datetime"));
	        	inProgressOccasion.setSendDate(rs1.getDate("send_date"));
	        	inProgressOccasion.setStatus( rs1.getInt("status")); 		        	
	       }	
		
		}
		
		 statement.close();
		return inProgressOccasion;
	}
	

}
