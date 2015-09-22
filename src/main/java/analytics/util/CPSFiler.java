package analytics.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.exception.RealTimeScoringException;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.OccasionDao;
import analytics.util.dao.OccationCustomeEventDao;
import analytics.util.dao.OutboxDao;
import analytics.util.objects.APIResponseMapper;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.OccasionInfo;
import analytics.util.objects.ScoresInfo;
import analytics.util.objects.TagMetadata;


public class CPSFiler {
	
	private static final Logger logger = LoggerFactory.getLogger(CPSFiler.class);
	private OutboxDao outboxDao;
	private OccasionDao occasionDao;
	private MemberInfoDao memberInfoDao;

	public void initDAO() throws RealTimeScoringException{
		outboxDao = new OutboxDao();
		memberInfoDao = new MemberInfoDao();		
		occasionDao = OccasionDao.getInstance();
	}
	
	public List<EmailPackage> prepareEmailPackages(String rtsAPIResponse, String lyl_id_no,String l_id) throws JSONException, SQLException, Exception {
		
		List<EmailPackage> emailPackages = new ArrayList<EmailPackage>(); 
		OccasionInfo occasionInfo;		
		MemberInfo memberInfo  = memberInfoDao.getMemberInfo(l_id);
		
		if(memberInfo!=null){
			if(StringUtils.isNotBlank(memberInfo.getEid())){
				List<TagMetadata> validOccasions = this.getValidOccasionsList(rtsAPIResponse);
				
				if(validOccasions != null && validOccasions.size() > 0){
					for(TagMetadata validOccasion : validOccasions){
						if(validOccasion != null){
							EmailPackage emailPackage = new EmailPackage();
							occasionInfo = occasionDao.getOccasionInfo(validOccasion.getPurchaseOccasion());					
							if(occasionInfo != null){
								validOccasion.setPriority(Integer.parseInt(occasionInfo.getPriority()));
								validOccasion.setSendDuration(Integer.parseInt(occasionInfo.getDuration()));	
								validOccasion.setDaysToCheckInHistory(Integer.parseInt(occasionInfo.getDaysToCheckInHistory()));	
								emailPackage.setCustEventNm(occasionInfo.getIntCustEvent());						
							}					
							emailPackage.setMemberId(lyl_id_no);
							emailPackage.setMdTagMetaData(validOccasion);
							emailPackage.setMemberInfo(memberInfo);
							emailPackages.add(emailPackage);
						}
						
					}
				}else{
				logger.info("There are no valid mdtags for this member: " + lyl_id_no);
				}
				
				if(emailPackages != null && emailPackages.size() > 0){
					
					//ignore sending an emailPackage if it has been sent in the history -( ex: duress > 0 & < 38)
					List<EmailPackage> emailPackagesToBeSent = this.ignorePackagesSentInHistory(emailPackages);
					
					//compare the incoming list of occasions with the ones in existing queue
					//if both the lists are same - no need of re queuing
					EmailPackage inProgressPackage = getInProgressOccasion(lyl_id_no);			
					List<EmailPackage> queuedPackages = getQueuedPackages(lyl_id_no);			
					if(inProgressPackage!=null){
						queuedPackages.add(0, inProgressPackage); 
					}			
					List<EmailPackage> currentPackages = queuedPackages;//in progress + queued			
					compareAndDelete(lyl_id_no,emailPackagesToBeSent, currentPackages);
					
					//Rule - top 5 % occasions are not queued
					if(emailPackagesToBeSent != null && emailPackagesToBeSent.size() > 0){
						emailPackagesToBeSent = filterTop5PercentOccasions(emailPackagesToBeSent, inProgressPackage);
					}
					
					if(emailPackagesToBeSent != null && emailPackagesToBeSent.size() > 0){
						return this.decideSendDates(emailPackagesToBeSent,inProgressPackage);
					}
				}
				
			}
			
		}
		else
		{
			logger.info("PERSIST: Occasions are not queued for member - " + lyl_id_no + " in CPS. There is no memeberInfo found for member - " + lyl_id_no );
		}
		
		
		return emailPackages;
	}

	protected List<EmailPackage> filterTop5PercentOccasions(List<EmailPackage> emailPackagesToBeSent,
															EmailPackage inProgressPackage)	{
		List<EmailPackage> filteredList = new ArrayList<EmailPackage>();
		
		//remove all top5 % occasions from the emailPackagesToBeSent if there is an active ongoing communication
		//top 5 % occasions are not queued
		if(inProgressPackage!=null){
			boolean inProgressActive = false;
			for(EmailPackage e : emailPackagesToBeSent){			
				if(e.getMdTagMetaData().getMdTag().equals(inProgressPackage.getMdTagMetaData().getMdTag())){						
					inProgressActive = true;
					break;
				}
			}
			
			if( inProgressActive){
				filteredList = removeTop5PercentOccasion(emailPackagesToBeSent);				
			}	
			else{
				//consider sending the top5 occasion only if it comes as the first occasion in the list
				if(isOccasionTop5Percent(emailPackagesToBeSent.get(0).getMdTagMetaData().getMdTag())){
					filteredList.add(emailPackagesToBeSent.get(0));
				}else{
					filteredList = removeTop5PercentOccasion(emailPackagesToBeSent);
				}
			}
		}
		else{
			//consider sending the top5 occasion only if it comes as the first occasion in the list
			if(isOccasionTop5Percent(emailPackagesToBeSent.get(0).getMdTagMetaData().getMdTag())){
				filteredList.add(emailPackagesToBeSent.get(0));
			}
			else
				filteredList = removeTop5PercentOccasion(emailPackagesToBeSent);
			
		}
		return filteredList;		
	}

	/**given a list of email packages, this method returns a list excluding all top5 % MSM elements from the given list
	 * @param emailPackagesToBeSent
	 */
	protected List<EmailPackage> removeTop5PercentOccasion(List<EmailPackage> emailPackagesToBeSent) {
		Iterator<EmailPackage> emailPackagesItr = emailPackagesToBeSent.iterator();
		while(emailPackagesItr.hasNext()){
			EmailPackage emailPackage = emailPackagesItr.next();
			if(emailPackage!= null){
				if(isOccasionTop5Percent(emailPackage.getMdTagMetaData().getMdTag())){
					emailPackagesItr.remove();							
				}
			}
		}
		return emailPackagesToBeSent;
	}
	
	protected List<EmailPackage> decideSendDates(	List<EmailPackage> emailPackagesToBeSent, EmailPackage inProgressPackage) {
		boolean inProgressActive = false;
		Iterator<EmailPackage> emailPackagesItr = emailPackagesToBeSent.iterator();
		if(inProgressPackage != null){
			while(emailPackagesItr.hasNext()){
				EmailPackage emailPackage = emailPackagesItr.next();
				if(emailPackage.getMdTagMetaData().getMdTag().equals(inProgressPackage.getMdTagMetaData().getMdTag())){
					emailPackagesItr.remove();
					inProgressActive = true;				
				}
			}
		}
		
		//Rule1 1 - If the inProgressPackage is not active or if there is no inProgress occasion - queue the incoming 
		if(inProgressPackage == null){
			return queueStartingToday(emailPackagesToBeSent);			
		}
		
		if(!inProgressActive){			
			return queueStartingTomorrow(emailPackagesToBeSent);			
		}
		
		
		EmailPackage previousOccasion = inProgressPackage;
		for(EmailPackage emailPackage : emailPackagesToBeSent){			
				
			//Rule - if incoming occasion is of higher priority - interrupt the in progress occasion
			if( emailPackage.getMdTagMetaData().getPriority() < previousOccasion.getMdTagMetaData().getPriority()){
				emailPackage.setSendDate(new DateTime().plusDays(1).toDate());
			}			
			//Rule - if incoming occasion is of same priority as the inProgress occasion - (check for same bu, sub bu ??) and queue the incoming occasion
			//Rule - if incoming occasion is of lesser priority than the inProgress occasion - queue the incoming occasion
			else if(( emailPackage.getMdTagMetaData().getPriority() == previousOccasion.getMdTagMetaData().getPriority() 
					//&& emailPackage.getMdTagMetaData().getFirst5CharMdTag().equals(previousOccasion.getMdTagMetaData().getFirst5CharMdTag())
					) || (emailPackage.getMdTagMetaData().getPriority() > previousOccasion.getMdTagMetaData().getPriority())) {
				emailPackage.setSendDate(CalendarUtil.getNewDate(previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));							
			}			
			previousOccasion = emailPackage;						
			
		}
		
		return emailPackagesToBeSent;
	}

	/**
	 * @param emailPackagesToBeSent
	 */
	protected List<EmailPackage> queueStartingToday(List<EmailPackage> emailPackagesToBeSent) {
		EmailPackage previousOccasion;
		previousOccasion = null;
		for(EmailPackage emailPackage : emailPackagesToBeSent){
			if(previousOccasion == null){
				emailPackage.setSendDate(new DateTime().toDate());						
			}
			else{
				emailPackage.setSendDate(CalendarUtil.getNewDate(previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));				
			}
			previousOccasion = emailPackage;	
		}
		
		return emailPackagesToBeSent;
	}
	
	protected List<EmailPackage> queueStartingTomorrow(List<EmailPackage> emailPackagesToBeSent) {
		EmailPackage previousOccasion;
		previousOccasion = null;
		for(EmailPackage emailPackage : emailPackagesToBeSent){
			if(previousOccasion == null){
				emailPackage.setSendDate(new DateTime().plusDays(1).toDate());				
			}
			else{
				emailPackage.setSendDate(CalendarUtil.getNewDate(previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));				
			}
			previousOccasion = emailPackage;	
		}
		
		return emailPackagesToBeSent;
	}

	protected boolean isInProgressOccasionActive(	List<EmailPackage> emailPackagesToBeSent, EmailPackage inProgressPackage) {
		for(EmailPackage e : emailPackagesToBeSent){
			if(e.getMdTagMetaData().getMdTag().equals(inProgressPackage.getMdTagMetaData().getMdTag()))
				return true;
		}
		return false;
	}

	protected List<EmailPackage> getQueuedPackages(String lyl_id_no) throws Exception, SQLException {
		List<OccasionInfo> occasionsInfo = occasionDao.getOccasionsInfo();
		return outboxDao.getQueuedEmailPackages(lyl_id_no, occasionsInfo);
	}

	protected void compareAndDelete(String lyl_id_no, List<EmailPackage> emailPackagesToBeSent, List<EmailPackage> currentPackages) throws SQLException {
		//if exisitng queue and incoming is same - do nothing.		
		if(emailPackagesToBeSent != null && currentPackages != null){
			if(!arePackagesSame(emailPackagesToBeSent, currentPackages))	
			{
				if(currentPackages.size()>0)
					outboxDao.deleteQueuedEmailPackages(lyl_id_no);	
			}
			
		}
		
						
	}

	protected boolean arePackagesSame(List<EmailPackage> emailPackagesToBeSent,List<EmailPackage> currentPackages) {	
			if(emailPackagesToBeSent.size() != currentPackages.size())
				return false;
			
			for(int i = 0; i<emailPackagesToBeSent.size();i++){
				if(!emailPackagesToBeSent.get(i).getMdTagMetaData().getMdTag().equals(currentPackages.get(i).getMdTagMetaData().getMdTag()))
					return false;
			}
			
			return true;			
	}

	protected EmailPackage getInProgressOccasion(String lyl_id_no) throws SQLException, Exception {
		List<OccasionInfo> occasionsInfo = occasionDao.getOccasionsInfo();
		return outboxDao.getInProgressPackage(lyl_id_no,occasionsInfo);
	}

	protected List<TagMetadata> getValidOccasionsList(String rtsAPIResponse) throws JSONException{
		List<TagMetadata> mdTagsList = new ArrayList<TagMetadata>() ;
		if(StringUtils.isNotEmpty(rtsAPIResponse)){
			SingletonJsonParser singletonJsonParser = SingletonJsonParser.getInstance();
			if(singletonJsonParser != null){
				APIResponseMapper apiResponseMapper = singletonJsonParser.getGsonInstance().fromJson(rtsAPIResponse, APIResponseMapper.class);  //Parsing response
				List<ScoresInfo> scoresInfos = apiResponseMapper.getScoresInfo();
				if(apiResponseMapper != null && scoresInfos != null && scoresInfos.size() > 0){
					for(ScoresInfo scoresInfo : scoresInfos){
						if(scoresInfo != null){
							//Check if the occasion has an mdtag
							if(StringUtils.isNotBlank(scoresInfo.getMdTag())){
								TagMetadata tagMetaData = new TagMetadata();
								tagMetaData.setPurchaseOccassion(scoresInfo.getOccassion());
								tagMetaData.setBusinessUnit(scoresInfo.getBusinessUnit());
								tagMetaData.setSubBusinessUnit(scoresInfo.getSubBusinessUnit());
								tagMetaData.setMdTag(scoresInfo.getMdTag());	
								mdTagsList.add(tagMetaData);
							}								
						}
					}
				}
			}
		}	
		return mdTagsList;	
	}

	protected boolean isOccasionTop5Percent(String mdtag) {
		
		if(StringUtils.isNotBlank(mdtag)){
			//Check for the 6th char of the mdtag to be 8
			if(mdtag.substring(5, 6).equals(MongoNameConstants.top5PercentTag))
				return true;
			else
				return false;			
		}
		return false;
		
	}
	
	protected List<EmailPackage> ignorePackagesSentInHistory(List<EmailPackage> emailPackages) throws SQLException {
		// this method ignores all the packages that are sent between sendDuration days and 60 + sendDuration
		// ex :if the incoming occasion is duress - it will not send the occasion, if duress occasion was sent in > 8 and < 68 days
		if(emailPackages != null && emailPackages.size() > 0){
			Iterator<EmailPackage> emailPackagesItr = emailPackages.iterator();
			while(emailPackagesItr.hasNext()){
			EmailPackage emailPackage = emailPackagesItr.next();
			if(emailPackage!= null){
				Date sentDate = outboxDao.getSentDate(emailPackage);
				//First Rule - Check that when the package was last sent and is it sent with-in sendDuration
				if(sentDate != null){
					OccasionInfo occasionInfo = occasionDao.getOccasionInfo(emailPackage.getMdTagMetaData().getPurchaseOccasion());
					if(occasionInfo  != null){
					    int sendDuration = Integer.parseInt(occasionInfo.getDuration());
					    int daysToCheckInHistory = Integer.parseInt(occasionInfo.getDaysToCheckInHistory());
						int dateDiffDays = CalendarUtil.getDatesDiff(CalendarUtil.getTodaysDate(), sentDate);
							if((dateDiffDays >sendDuration ) &&(dateDiffDays < daysToCheckInHistory+sendDuration)){
								emailPackagesItr.remove();
							}
						}
					}
				}
			}
		}
		return emailPackages;
	}
	
	public void fileEmailPackages(List<EmailPackage> emailPackages) throws SQLException {
		//don't queue if sendDate is null
		if(emailPackages != null && emailPackages.size() > 0){			
			outboxDao.queueEmailPackages(filterNullDatePackages(emailPackages));
		}
	}
	
	public List<EmailPackage> filterNullDatePackages(List<EmailPackage> emailPackages){
		Iterator<EmailPackage> emailPackagesItr = emailPackages.iterator();
        while(emailPackagesItr.hasNext()){
        	EmailPackage emailPackage = emailPackagesItr.next();
	        if(emailPackage.getSendDate() == null){
	        	emailPackagesItr.remove();
			}
        }
		return emailPackages;
	}
	
	/*public List<EmailPackage> decideSendDates(List<EmailPackage> emailPackages, EmailPackage inProgressPackage) throws SQLException, RealTimeScoringException {
		/******************* LOGIC *********************************	
			prepare the list of occasions(this will have occasion name, priority, sendDuration)
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
		**************************************************************//*
		EmailPackage previousEmlPackage = null;
		for(EmailPackage emailPackage : emailPackages){	
			if(previousEmlPackage == null)
			{
				int incomingPkgPriority = emailPackage.getMdTagMetaData().getPriority();
				//find if there are occasions of higher or equal priority sent in the last send duration days.
				for(int i=0; i<=incomingPkgPriority; i++)
				{
					OccasionInfo occasionInfo = occasionDao.getOccasionInfo(String.valueOf(incomingPkgPriority));
					int sendDuration = Integer.parseInt(occasionInfo.getDuration());
					Date sentDate = outboxDao.getSentDate(emailPackage.getMemberId(), occasionInfo.getOccasion());
					if(sentDate != null) //this means an occasion of higher priority is sent in the past
					{
						//check if the sentdate is within the limit of the duration of 
						//the occasion to determine if the communication is still in progress. 
						//If it is in progress - set the send date of incoming occasion
						// to the next date after the communication schedule is completed
						// otherwise set it to today's date
						int daysSentSoFar = CalendarUtil.getDatesDiff(CalendarUtil.getTodaysDate(), sentDate);
						if(daysSentSoFar < sendDuration )
							emailPackage.setSendDate(CalendarUtil.getNewDate(sentDate, sendDuration));					
					}
					else
					{
						if(previousEmlPackage == null)
							emailPackage.setSendDate(CalendarUtil.getTodaysDate());						
						else					
							emailPackage.setSendDate(CalendarUtil.getNewDate(previousEmlPackage.getSendDate(), previousEmlPackage.getMdTagMetaData().getSendDuration()));							
					}
					previousEmlPackage = emailPackage;
					break; // terminate loop as there will be only one occasion being sent at any point.
					
				}
			}
			else
			{
				emailPackage.setSendDate(CalendarUtil.getNewDate(previousEmlPackage.getSendDate(), previousEmlPackage.getMdTagMetaData().getSendDuration()));
				previousEmlPackage = emailPackage;
			}
		}
		return emailPackages;
	} 
*/
	
	/*	protected List<EmailPackage> decideSendDates(	List<EmailPackage> emailPackagesToBeSent, EmailPackage inProgressPackage) {
	List<EmailPackage> filteredPackageswithSendDates;
	if(emailPackagesToBeSent != null){
		EmailPackage previousOccasion = inProgressPackage;
		
		//Rule - If the incoming oocasionList doesn't contain the inProgressPackage - interrupt the in progress occasion.
		if(inProgressPackage != null){
			if(isInProgressOccasionActive(emailPackagesToBeSent,inProgressPackage)){
				for(EmailPackage emailPackage : emailPackagesToBeSent){
					if(previousOccasion == null){
						emailPackage.setSendDate(CalendarUtil.getTodaysDate());						
					}
					else{
						
						//Rule - if incoming occasion is of higher priority - interrupt the in progress occasion
						if( emailPackage.getMdTagMetaData().getPriority() < previousOccasion.getMdTagMetaData().getPriority()){
							emailPackage.setSendDate(CalendarUtil.getTodaysDate());								
						}
						
						//Rule - if incoming occasion is already in Progress - don't queue it again
						else if(emailPackage.getMdTagMetaData().getMdTag().equals(inProgressPackage.getMdTagMetaData().getMdTag())){
							emailPackage.setSendDate(inProgressPackage.getSendDate());
							emailPackage.setSentDateTime(inProgressPackage.getSentDateTime());
							emailPackage.setStatus(Constants.SENT);
						}
						//Rule - if incoming occasion is of same priority as the inProgress occasion - (check for same bu, sub bu ??) and queue the incoming occasion
						//Rule - if incoming occasion is of lesser priority than the inProgress occasion - queue the incoming occasion
						else if(( emailPackage.getMdTagMetaData().getPriority() == previousOccasion.getMdTagMetaData().getPriority() 
								//&& emailPackage.getMdTagMetaData().getFirst5CharMdTag().equals(previousOccasion.getMdTagMetaData().getFirst5CharMdTag())
								) || (emailPackage.getMdTagMetaData().getPriority() > previousOccasion.getMdTagMetaData().getPriority())) {
							emailPackage.setSendDate(CalendarUtil.getNewDate(previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));							
						}
						
							
					}
					
					previousOccasion = emailPackage;						
					
				}					
			}	
			else{ //interrupt the in progress occasion as it is not active today.
				emailPackagesToBeSent =queueStartingToday(emailPackagesToBeSent);
				
			}
		}
		else{
			emailPackagesToBeSent = queueStartingToday(emailPackagesToBeSent);
			
		}
							
	}
	
	filteredPackageswithSendDates = filterSentPackages(emailPackagesToBeSent);
	return filteredPackageswithSendDates;
}*/
	
	/* checking for top 5% when validating the incoming occasions 
	 * 		check if the first occasion in the JSONArray is "Top 5% of MSM" mdTag  && if it is an active tag
								//if it is, we don't need to iterate further
								if(this.isOccasionTop5Percent(scoresInfo)){
									mdTagsList.add(tagMetaData);
									return mdTagsList;														
								}else if(!this.isOccasionTop5Percent(scoresInfo)){ //TODO: To check that we have to read %5 also
									mdTagsList.add(tagMetaData);
								}*/		
	 
	
}
