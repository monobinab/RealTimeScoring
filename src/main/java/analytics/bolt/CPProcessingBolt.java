package analytics.bolt;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.exception.RealTimeScoringException;
import analytics.util.CPSFiler;
import analytics.util.RTSAPICaller;
import analytics.util.dao.ClientApiKeysDAO;
import analytics.util.objects.EmailPackage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class CPProcessingBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;
	private static final String api_Key_Param="CPS";
	private static final Logger LOGGER = LoggerFactory.getLogger(CPProcessingBolt.class);
	private OutputCollector outputCollector;
	private RTSAPICaller rtsApiCaller;
	private String api_key;
	private CPSFiler cpsFiler;
	public CPProcessingBolt(String env) {
		super(env);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		rtsApiCaller = RTSAPICaller.getInstance();
		api_key = new ClientApiKeysDAO().findkey(api_Key_Param);
		cpsFiler = new CPSFiler();
		try {
			cpsFiler.initDAO();
		} catch (RealTimeScoringException e) {
			LOGGER.error("Exception Occured in CPProcessingBolt Prepare :: ", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("CPProcessingBolt_input_count");
		String lyl_id_no = null; 
		String l_id = null; 
		countMetric.scope("entering_CPProcessing_bolt").incr();		
		//LOGGER.info("cps input contains message : " + lyl_id_no );
			
		if(input != null && input.contains("lyl_id_no"))
		{
			lyl_id_no = input.getStringByField("lyl_id_no");	
			l_id = input.getStringByField("l_id");
			
			try{
				//call rts api and get response for this l_id 
				//20 - level, rtsTOtec is the apikey for internal calls to RTS API from topologies
				String rtsAPIResponse = rtsApiCaller.getRTSAPIResponse(lyl_id_no, "20", api_key, "sears", Boolean.FALSE, "");
				List<EmailPackage> emailPackages = cpsFiler.prepareEmailPackages(rtsAPIResponse,lyl_id_no,l_id);
				if(emailPackages!= null && emailPackages.size()>0)
				{
					cpsFiler.fileEmailPackages(emailPackages);
					LOGGER.info("PERSIST: Queued Tags in CPS Outbox for lyl_id_no " + lyl_id_no+ " : "+getLogMsg(emailPackages));
					outputCollector.ack(input);
				}					
			} catch (SQLException e){
				LOGGER.error("SQLException Occured in CPProcessingBolt :: ", e);
				outputCollector.fail(input);					
			} catch (Exception e){
				LOGGER.error("Exception Occured in CPProcessingBolt :: ", e);
				outputCollector.fail(input);	
			}
				
		} else {
			redisCountIncr("null_lid");			
			outputCollector.fail(input);				
		}
	}

	private String getLogMsg(List<EmailPackage> emailPackages) {
		String logMsg = "  ";
		for(EmailPackage emailPackage : emailPackages)
		{
			logMsg = logMsg.concat(emailPackage.getMdTagMetaData().getMdTag()).concat("  "); 
		}
		return logMsg;
	}
	
	/*private List<EmailPackage> prepareEmailPackages(String rtsAPIResponse, String lyl_id_no) throws JSONException, SQLException {
		List<EmailPackage> emailPackages = new ArrayList<EmailPackage>(); 
		List<TagMetadata> occasionsList = occasionDao.getOccasions();
		List<TagMetadata> mdTagMetadataList = getValidOccasionsList(rtsAPIResponse);
		String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
		MemberInfo memberInfo  = memberInfoDao.getMemberInfo(l_id);
		for(TagMetadata mdTagMetaData : mdTagMetadataList) {
			String custEventName = occasionCustomeEventDao.getCustomeEventName(mdTagMetaData.getPurchaseOccasion());
			mdTagMetaData.setPriority(occasionDao.getPriority(mdTagMetaData.getPurchaseOccasion()));
			mdTagMetaData.setSendDuration(occasionDao.getSendDuration(mdTagMetaData.getPurchaseOccasion()));
			EmailPackage emailPackage = new EmailPackage(); //status and addedDateTime are set by default constructor
			emailPackage.setMemberId(lyl_id_no);
			emailPackage.setCustEventNm(custEventName);
			emailPackage.setMdTagMetaData(mdTagMetaData);
			emailPackage.setMemberInfo(memberInfo);
			//emailPackage.setTopologyName(topologyName);
			emailPackages.add(emailPackage);
		}
		//List<EmailPackage> queuedEmailPackages = outboxDao.getQueuedEmailPackages(lyl_id_no);
		//decideSendDates(emailPackages, queuedEmailPackages);
		
		//Remove all the queued email packages as API gives all the packages that are not expired.
		outboxDao.deleteQueuedEmailPackages(lyl_id_no);
		//ignore sending an emailPackage if it has been within last 60days starting after sendDuration th day ( ex: duress > 8 & < 68)
		emailPackages = ignorePackagesWithin60Days(emailPackages,occasionsList);
		emailPackages = decideSendDates(emailPackages, occasionsList);
		return emailPackages;
	}

	private List<EmailPackage> ignorePackagesWithin60Days(List<EmailPackage> emailPackages, List<TagMetadata> occasionsList) throws SQLException {
		// this method ignores all the packages that are sent between sendDuration days and 60 + sendDuration
		// ex :if the incoming occasion is duress - it will not send the occasion, if duress occasion was sent in > 8 and < 68 days
		Date sentDate = null;
		Integer sendDuration = null;
		for(EmailPackage emailPackage : emailPackages){
			sentDate = outboxDao.getSentDate(emailPackage);
			sendDuration = occasionDao.getSendDuration(emailPackage.getMdTagMetaData().getPurchaseOccasion());
			if(sentDate != null) //this means the occasion  was sent in the past
			{
				//If the occasion is sent in between sendDuration and 60+sendDuration - don't queue the emailPackage
				int dateDiffDays = CalendarUtil.getDatesDiff(CalendarUtil.getTodaysDate(), sentDate);
				if((dateDiffDays > sendDuration) &&(dateDiffDays < defaultDaysToCheck+sendDuration) )
					emailPackages.remove(emailPackage);				
			}
			
		}
		return null;
	}

	private List<EmailPackage> decideSendDates(List<EmailPackage> emailPackages,List<TagMetadata> occasionsList) throws SQLException {
		 ****************** LOGIC *********************************	
			prepare the list of occasions(TagMetaData - this will have occasion name, priority, sendDuration)
			loop through the email packages
			{
				incomingPkgPriority = get priority of the incoming occasion
				find if there are occasions of higher priority sent in the last send duration days.
				for (int i =0; i< incomingPkgPriority ; i++)
				{
					get sentDate of occasionList.get(i)
					if sentDate not null
						if (today-sentDate < sendDuration )
							set send date of emailPackage = sentDate + sendDuration
					else
						set send date = today
			}
		}
		*************************************************************
		for(EmailPackage emailPackage : emailPackages){			
			int incomingPkgPriority = emailPackage.getMdTagMetaData().getPriority();
			//find if there are occasions of higher priority sent in the last send duration days.
			for(int i=0; i<incomingPkgPriority; i++)
			{
				TagMetadata mdTagMetaData = occasionsList.get(i);
				
				EmailPackage sentEmailPackage = new EmailPackage();
				sentEmailPackage.setMdTagMetaData(mdTagMetaData);
				sentEmailPackage.setMemberId(emailPackage.getMemberId());
				Date sentDate = outboxDao.getSentDate(emailPackage.getMemberId(), occasionsList.get(i));
				if(sentDate != null) //this means an occasion of higher priority is sent in the past
				{
					//If the communication is schedule is in progress - set the date of incoming occasion
					// to the next date after the communication schedule is completed
					if(CalendarUtil.getDatesDiff(CalendarUtil.getTodaysDate(), sentDate) < occasionsList.get(i).getSendDuration() )
						emailPackage.setSendDate(CalendarUtil.getNewDate(sentDate, occasionsList.get(i).getSendDuration()));					
				}
				else
				{
					emailPackage.setSendDate(CalendarUtil.getTodaysDate());
				}
			}
		}		
		return emailPackages;
	}*/
 
	/*private List<TagMetadata> getValidOccasionsList(String rtsAPIResponse) throws JSONException {
		List<TagMetadata> mdTagsList = new ArrayList<TagMetadata>() ;
		
		org.json.JSONObject obj = new org.json.JSONObject(rtsAPIResponse);
		try {			
				
				List<String> activeTags = tagResponsysActiveDao.getActiveResponsysTagsList();
				JSONArray scoresInfo =  obj.getJSONArray("scoresInfo");
				TagMetadata tagMetaData;
				for(int i=0; i<scoresInfo.length(); i++){
					JSONObject scoreInfo = scoresInfo.getJSONObject(i);
					tagMetaData = new TagMetadata();
					//check if the member has mdTags
					String[] scoreElements = JSONObject.getNames(scoreInfo);
				
					if(scoreInfo.has("occassion")){
						tagMetaData.setPurchaseOccassion(scoreInfo.getString("occassion")!= null ? scoreInfo.getString("occassion"):null);
						tagMetaData.setBusinessUnit(scoreInfo.getString("businessUnit")!= null ? scoreInfo.getString("businessUnit"): null);
						tagMetaData.setSubBusinessUnit(scoreInfo.getString("subBusinessUnit")!= null ? scoreInfo.getString("subBusinessUnit"): null);
						tagMetaData.setMdTag(scoreInfo.getString("mdTag")!= null ? scoreInfo.getString("mdTag") : null);
						
						//check if the first occasion in the JSONArray is "Top 5% of MSM" mdTag  && if it is an active tag
						//if it is, we don't need to iterate further
						if(scoreInfo.getString("occassion").equals(top5PercentTag) && activeTags.contains(scoreInfo.getString("occassion").substring(0, 5)))
						{
							if(i==0){
								mdTagsList.add(tagMetaData);
								return mdTagsList;	
							}														
						}else if(!scoreInfo.getString("occassion").equals(top5PercentTag) && activeTags.contains(scoreInfo.getString("occassion").substring(0, 5)))
						{
							mdTagsList.add(tagMetaData);
						}							
					}				
				}			
			} catch (org.json.JSONException e) {
				LOGGER.error("Exception Occured in CPProcessingBolt :: " , e.getMessage());
			}		
		return mdTagsList;	
	}*/


	
	/*	private List<EmailPackage> decideSendDates(List<EmailPackage> emailPackages, List<EmailPackage> queuedEmailPackages ) 
	{
		Integer priority = 0;
		Integer sendDuration = 0;
		Date sendDate = null;
		Date sentDate = null;
		// Delete the expired occasions from the queuedEmailPackages
		for(EmailPackage queuedEP : queuedEmailPackages)
		{
			if(!emailPackages.contains(queuedEP))
			{
				outboxDao.removeFromQueue(queuedEP);
				queuedEmailPackages.remove(queuedEP);
			}
		}		
		
		 Loop through the list of email packages
		 * for each email package - check if this occasion is sent in the last 30 days
		 * if the occasion is sent remove it from the list
		 * if the occasion is not sent check what is queued already for this member and re prioritize
		 * 
		//for(EmailPackage emailPackage : emailPackages)
		for(int i = 0; i<emailPackages.size(); i++)
		{
			sendDuration = getSendDuration(emailPackages.get(i).getMdTagMetaData().getPurchaseOccasion());
			//was occasion sent in the past - defaultDaysToCheck
			if(wasOccasionSent(emailPackages.get(i), defaultDaysToCheck+sendDuration))
			{	
				emailPackages.remove(emailPackages.get(i));
				continue
				
				//was occasion sent in the last "sendDuration" days of this occasion
				if(wasOccasionSent(emailPackages.get(i), sendDuration))
					sentDate = outboxDao.getSentDate(emailPackages.get(i));				
			}else{
				//is it of higher priority than the occasions in the queue
				if(isHigherPriority(queuedEmailPackages, emailPackages.get(i)))
				{
					emailPackages.get(i).setSendDate(CalendarUtil.getTodaysDate());
					moveQueuedPackagesSendDates(queuedEmailPackages,sendDuration);
				}
				else
				{
					//get the sentDate of the last package in the queue and set the new date by adding the sendduartion to the send date
					int lastPackage = queuedEmailPackages.size() - 1;
					EmailPackage lastPackageInQueue = queuedEmailPackages.get(lastPackage);
					emailPackages.get(i).setSendDate(CalendarUtil.getNewDate(lastPackageInQueue.getSendDate(), getSendDuration(lastPackageInQueue.getMdTagMetaData().getPurchaseOccasion())));						
				}
					
			}
			
			
							
		}
		
		//get the priorites and send duration of the queuedEmailPackages
		
		
		return null;
	}*/
	
	/*	private boolean wasOccasionSent(EmailPackage emailPackage, Integer daysToCheck) {
	// TODO Auto-generated method stub
	Integer status = outboxDao.getEmailPackageStatus(emailPackage, daysToCheck);
	if(status == 1)
		return true;
	else
		return false;
}*/


}
