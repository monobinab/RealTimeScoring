package analytics.util.dao;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.exception.RealTimeScoringException;
import analytics.util.CalendarUtil;
import analytics.util.Constants;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.OccasionInfo;
import analytics.util.objects.TagMetadata;

public class OutboxDao extends AbstractMySQLDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OutboxDao.class);
	public List<EmailPackage> getQueuedEmailPackages(String lyl_id_no, List<OccasionInfo> occasionsInfo) throws RealTimeScoringException, SQLException {
		// This method returns all pending emails from the outbox
		List<EmailPackage>emlPackageList = new ArrayList<EmailPackage>();
		StringBuilder query = new StringBuilder();
		query.append("SELECT * ")
			.append("FROM cp_outbox ")
			.append("WHERE loy_id= ? AND status=0;");
    	PreparedStatement statement;
		
			statement = connection.prepareStatement(query.toString());
			statement.setString(1, lyl_id_no);
			
			LOGGER.info("query being executed for getting queued email packages: " + statement);
			ResultSet rs = statement.executeQuery();
	        
	        while (rs.next()) {
	             TagMetadata tagMetadata = new TagMetadata(rs.getString("md_tag"),rs.getString("bu"),rs.getString("sub_bu"),rs.getString("occasion_name"));
	             for(OccasionInfo occasion :occasionsInfo){
	            	 if(rs.getString("occasion_name").equalsIgnoreCase(occasion.getOccasion()))
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
	        return emlPackageList;
	}

	public List<EmailPackage> queueEmailPackages(List<EmailPackage> emailPackages) throws SQLException {
		Integer queueLength =0;
		List<EmailPackage> queuedEmailPackages=new ArrayList<EmailPackage>();
		
		for(EmailPackage emlPack: emailPackages) {
			if(queueLength < Constants.CPS_QUEUE_LENGTH){
				StringBuilder query = new StringBuilder();
				query.append("INSERT INTO rts_member.cp_outbox ")
					.append("(loy_id, bu, sub_bu, md_tag, occasion_name, added_datetime, send_date, status, cust_event_name, customer_id, sears_opt_in, kmart_opt_in, syw_opt_in) ")
					.append("VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);");
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
					LOGGER.error("PERSIST:Send date is set to null for memberid: " + emlPack.getMemberId() + " with mdtag - "+ emlPack.getMdTagMetaData().getMdTag());
				statement.setString(8, emlPack.getStatus().toString());
				statement.setString(9, emlPack.getCustEventNm().toString());
				if(emlPack.getMemberInfo()!=null){
					if(emlPack.getMemberInfo().getEid()!=null){
						statement.setString(10, emlPack.getMemberInfo().getEid());
					}
					else
					{
						LOGGER.error("PERSIST:emlPack.getMemberInfo().getEid() is set to null for memberid: " + emlPack.getMemberId() + " EID is saved as empty string to outbox.");
						statement.setString(10, "");
					}
					if(emlPack.getMemberInfo().getSrs_opt_in()!=null){
						statement.setString(11, emlPack.getMemberInfo().getSrs_opt_in());
					}
					else
					{
						LOGGER.error("PERSIST:emlPack.getMemberInfo().getSrs_opt_in() is set to null for memberid: " + emlPack.getMemberId() + " Srs_opt_in is saved as empty string to outbox.");
						statement.setString(11, "");
					}
					if(emlPack.getMemberInfo().getKmt_opt_in()!=null){
						statement.setString(12, emlPack.getMemberInfo().getKmt_opt_in());
					}
					else
					{
						LOGGER.error("PERSIST:emlPack.getMemberInfo().getKmt_opt_in() is set to null for memberid: " + emlPack.getMemberId() + "Kmt_opt_in is saved as empty string to outbox.");
						statement.setString(12, "");
					}
					if(emlPack.getMemberInfo().getSyw_opt_in()!=null){
						statement.setString(13, emlPack.getMemberInfo().getSyw_opt_in());
					}
					else
					{
						LOGGER.error("PERSIST:emlPack.getMemberInfo().getSyw_opt_in() is set to null for memberid: " + emlPack.getMemberId() + " Syw_opt_in is saved as empty string to outbox.");
						statement.setString(13, "");
					}					
					
				}
				else{
					LOGGER.error("PERSIST:emlPack.getMemberInfo() is set to null for memberid: " + emlPack.getMemberId() + " Fields of memberInfo are saved as empty strings to outbox.");
					statement.setString(10, "");
					statement.setString(11,"");
					statement.setString(12, "");
					statement.setString(13, "");
				}
				LOGGER.info("query being executed for queing email package: " + statement);
				
				statement.executeUpdate();	
				queueLength = queueLength+ emlPack.getMdTagMetaData().getSendDuration();
				statement.close();
				queuedEmailPackages.add(emlPack);
				
			}
			else
			{
				
				//gather the tags that are not queued 
				String ignoredTags = " ";
				for (int i =emailPackages.indexOf(emlPack); i<emailPackages.size(); i++){
					ignoredTags = ignoredTags+emailPackages.get(i).getMdTagMetaData().getMdTag() + "  ";					
				}
				LOGGER.info("PERSIST: MemberId : "+ emlPack.getMemberId() + " | CPS STATUS : NOT QUEUED | REASON: Queue Length reached threshold. | " + " NOT QUEUED TAGS : " + ignoredTags) ;
				
				break;
			}
					}
		
		return queuedEmailPackages;
	}

	public void deleteQueuedEmailPackages(String lyl_id_no) throws SQLException {
		List<String> deletedTags = new ArrayList<String>();
		String query= "Select * from rts_member.cp_outbox WHERE loy_id=? AND status=0;";
		PreparedStatement statement = connection.prepareStatement(query);
		statement.setString(1, lyl_id_no);
		ResultSet rs = statement.executeQuery();
		while (rs.next()) {  
			deletedTags.add(rs.getString("md_tag"));	        	
	    }
		statement.close();
		
		
		query = "DELETE from rts_member.cp_outbox WHERE loy_id=? AND status=0;";
		statement = connection.prepareStatement(query);
		statement.setString(1, lyl_id_no);
		statement.executeUpdate();
		statement.close();
		
		if(deletedTags.size()>0)
			LOGGER.info("PERSIST: MemberId : "+ lyl_id_no + " | CPS STATUS : DELETED | REASON : Deleted to requeue the current active tags. | DELETED TAGS : " + deletedTags ) ;
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
		LOGGER.info("query being executed to get the sentDate for history check :" + statement);
		
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
		LOGGER.info("query to get the sentDate for history check : " + statement);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {   
        	sentDate = rs.getTimestamp("sent_datetime"); /*sentDateTime*/
       }
        statement.close();
		return sentDate;	
	}

	public EmailPackage getInProgressPackage(String lyl_id_no, List<OccasionInfo> occasionsInfo) throws SQLException, ParseException {
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
 		String todaysDateStr = sdformat.format(new DateTime().toDate());
 		Date todaysDate = sdformat.parse(todaysDateStr);
		
		EmailPackage inProgressOccasion = null;

		String query = "SELECT bu,sub_bu,md_tag,occasion_name, added_datetime, send_date,status, sent_datetime recentSentDate FROM rts_member.cp_outbox WHERE loy_id=? AND status=1 order by sent_datetime desc limit 1;";
		PreparedStatement statement = connection.prepareStatement(query);
			statement.setString(1, lyl_id_no);
			LOGGER.info("query to get the latest emailPackage sent for this member : " + statement);
			ResultSet rs1 = statement.executeQuery();
			 
			while (rs1.next() ) {				
				
				//get the latest sent package				
				java.util.Date sentDate = sdformat.parse(sdformat.format(rs1.getDate("recentSentDate")));
				//add the send duration to the sent date and see if it is less than today's date
				//if it is less - it means it is not in progress anymore				
				
	        	TagMetadata tagMetadata = new TagMetadata(rs1.getString("md_tag"),rs1.getString("bu"),rs1.getString("sub_bu"),rs1.getString("occasion_name"));	  
	        	 for(OccasionInfo occasion :occasionsInfo){
	            	 if(rs1.getString("occasion_name").equalsIgnoreCase(occasion.getOccasion()))
	            	 {
	            		 tagMetadata.setPriority(Integer.parseInt(occasion.getPriority()));
	    	             tagMetadata.setSendDuration(Integer.parseInt(occasion.getDuration()));	
	    	             tagMetadata.setDaysToCheckInHistory(Integer.parseInt(occasion.getDaysToCheckInHistory()));
	    	             break;
	            	 }
	             }	     		
	     			     		
	        	if( CalendarUtil.getNewDate(sentDate, tagMetadata.getSendDuration()).after(todaysDate) ||
	        			CalendarUtil.getNewDate(sentDate, tagMetadata.getSendDuration()).equals(todaysDate)){
	        		inProgressOccasion = new EmailPackage();
		        	inProgressOccasion.setMemberId(lyl_id_no);		        	
		        	inProgressOccasion.setMdTagMetaData(tagMetadata);
		        	inProgressOccasion.setAddedDateTime( rs1.getTimestamp("added_datetime"));
		        	inProgressOccasion.setSendDate(rs1.getDate("send_date"));
		        	inProgressOccasion.setStatus( rs1.getInt("status")); 
		        	inProgressOccasion.setSentDateTime(rs1.getTimestamp("recentSentDate"));
	        	}	
	        			        	
	       }
		
		 statement.close();
		return inProgressOccasion;
	}
	

}
