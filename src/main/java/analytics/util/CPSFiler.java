package analytics.util;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.exception.RealTimeScoringException;
import analytics.util.CalendarUtil;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.OccasionDao;
import analytics.util.dao.OutboxDao;
import analytics.util.dao.TagResponsysActiveDao;
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
	private TagResponsysActiveDao tagResponsysActiveDao;
	private List<String> activeTags;

	public void initDAO() throws RealTimeScoringException{
		outboxDao = new OutboxDao();
		memberInfoDao = new MemberInfoDao();
		occasionDao = new OccasionDao();
		tagResponsysActiveDao = new TagResponsysActiveDao();
		activeTags= tagResponsysActiveDao.getActiveResponsysTagsList();
		
	}
	
	public void setActiveTags(List<String> activeTags){
		this.activeTags = activeTags;
	}
	public List<EmailPackage> prepareEmailPackages(String rtsAPIResponse, String lyl_id_no,String l_id) throws JSONException, SQLException, Exception {
		
		List<EmailPackage> emailPackages = new ArrayList<EmailPackage>(); 

		OccasionInfo occasionInfo;		
		MemberInfo memberInfo  = memberInfoDao.getMemberInfo(l_id);
		
		if(memberInfo!=null){
			if(StringUtils.isNotBlank(memberInfo.getEid())&& Integer.parseInt(memberInfo.getEid())!=0){
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
					logger.info("PERSIST: MemberId : " + lyl_id_no + " | CPS STATUS : NO TAGS | REASON : No md/rtsTags from API.");
				}
				
				if(emailPackages != null ){
					
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
					emailPackagesToBeSent = compareAndDelete(lyl_id_no,emailPackagesToBeSent, currentPackages);
					
					//Rule - top 5 % occasions are not queued
					if(emailPackagesToBeSent != null && emailPackagesToBeSent.size() > 0){
						emailPackagesToBeSent = filterTop5PercentOccasions(emailPackagesToBeSent, inProgressPackage);
					}
					
					if(emailPackagesToBeSent != null && emailPackagesToBeSent.size() > 0){
						return this.decideSendDates(emailPackagesToBeSent,inProgressPackage);
					}
				}
				
			}
			else
			{
				logger.info("PERSIST: MemberId : " + lyl_id_no + " | CPS STATUS : NOT QUEUED | REASON : Eid is null, blank or 0.");
			}
			
		}
		else
		{
			logger.info("PERSIST: MemberId : " + lyl_id_no + " | CPS STATUS : NOT QUEUED | REASON : memberInfo not found ." );
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
		String removedTop5tags = "";
		String memberId = null;
		while(emailPackagesItr.hasNext()){
			EmailPackage emailPackage = emailPackagesItr.next();
			if(emailPackage!= null){
				if(isOccasionTop5Percent(emailPackage.getMdTagMetaData().getMdTag())){
					memberId = emailPackage.getMemberId();
					removedTop5tags = removedTop5tags+ emailPackage.getMdTagMetaData().getMdTag()+ " ";
					emailPackagesItr.remove();							
				}
			}
		}
		if(StringUtils.isNotBlank(memberId))
		{
			logger.info("PERSIST: MemberId : "+ memberId + " | TAGS : " + removedTop5tags + " | CPS STATUS : NOT QUEUED | REASON : Higher priority tags in queue already." );
		}
		return emailPackagesToBeSent;
	}
	
	protected List<EmailPackage> decideSendDates(	List<EmailPackage> emailPackagesToBeSent, EmailPackage inProgressPackage) throws ParseException {
		
		boolean inProgressActive = false;
		
		Iterator<EmailPackage> emailPackagesItr = emailPackagesToBeSent.iterator();
		if(inProgressPackage != null){
			while(emailPackagesItr.hasNext()){
				EmailPackage emailPackage = emailPackagesItr.next();
				if(emailPackage.getMdTagMetaData().getMdTag().equals(inProgressPackage.getMdTagMetaData().getMdTag())){
					logger.info("PERSIST: MemberId : "+ emailPackage.getMemberId() + " | TAG : " + emailPackage.getMdTagMetaData().getMdTag() + " | CPS STATUS : NOT QUEUED | REASON : This tag is inProgress already." );					
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
			// 1. queueStartingTomorrow if inProgress is not active anymore
				return queueStartingTomorrow(emailPackagesToBeSent);
		}
		
	   	EmailPackage previousOccasion = inProgressPackage;
		for(EmailPackage emailPackage : emailPackagesToBeSent){			
				
			//Rule - if incoming occasion is of higher priority - interrupt the in progress occasion
			//       and send it tomorrow			//
			if( emailPackage.getMdTagMetaData().getPriority() < previousOccasion.getMdTagMetaData().getPriority()){
				
				emailPackage.setSendDate(new DateTime().plusDays(1).toDate());
			}			
			//Rule - if incoming occasion is of same priority as the inProgress occasion - (check for same bu, sub bu ??) and queue the incoming occasion
			//Rule - if incoming occasion is of lesser priority than the inProgress occasion - queue the incoming occasion
			else if(( emailPackage.getMdTagMetaData().getPriority() == previousOccasion.getMdTagMetaData().getPriority() 
					//&& emailPackage.getMdTagMetaData().getFirst5CharMdTag().equals(previousOccasion.getMdTagMetaData().getFirst5CharMdTag())
					) || (emailPackage.getMdTagMetaData().getPriority() > previousOccasion.getMdTagMetaData().getPriority())) {
				
				//If the remaining duration of inProgress is greater than the queue length, no need to queue anymore occasions
				if(inProgressPackage.getMdTagMetaData().getSendDuration()- CalendarUtil.getDatesDiff(new Date(),inProgressPackage.getSentDateTime())>Constants.CPS_QUEUE_LENGTH){
					return null;			
				}
				emailPackage.setSendDate(CalendarUtil.getNewDate((previousOccasion.getSentDateTime()!=null)?previousOccasion.getSentDateTime():previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));
				//emailPackage.setSendDate(CalendarUtil.getNewDate(previousOccasion.getSendDate(), previousOccasion.getMdTagMetaData().getSendDuration()));							
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

	protected List<EmailPackage> compareAndDelete(String lyl_id_no, List<EmailPackage> emailPackagesToBeSent, List<EmailPackage> currentPackages) throws SQLException {
		//if existing queue and incoming is same - do nothing.		
		if(emailPackagesToBeSent != null && currentPackages != null){
			if(!arePackagesSame(emailPackagesToBeSent, currentPackages))	
			{
				if(currentPackages.size()>0)
					outboxDao.deleteQueuedEmailPackages(lyl_id_no);					
				return emailPackagesToBeSent;
			}
			else
			{
				return null;
			}
			
		}
		return emailPackagesToBeSent;
						
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
				if(null != apiResponseMapper)	{
					List<ScoresInfo> scoresInfos = apiResponseMapper.getScoresInfo();
						if(scoresInfos != null && scoresInfos.size() > 0){
							for(ScoresInfo scoresInfo : scoresInfos){
								if(scoresInfo != null){
									//Check if the occasion has an mdtag
									String mdtag = scoresInfo.getMdTag();
									if(StringUtils.isNotBlank(mdtag)){
										if(this.isOccasionTop5Percent(mdtag) && !isOccasionResponsysReady(mdtag)){
											logger.info("PERSIST: MemberId : "+ apiResponseMapper.getMemberId() + " | TAG : "+ mdtag +" | CPS STATUS : NOT QUEUED | REASON : Responsys not ready tag.");
											continue;								
										}	
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
	
	protected List<EmailPackage> ignorePackagesSentInHistory(List<EmailPackage> emailPackages) throws SQLException, ParseException {
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
 		String todaysDateStr = sdformat.format(new DateTime().toDate());
 		Date todaysDate = sdformat.parse(todaysDateStr);
 		List<EmailPackage> sentEmails=new ArrayList<EmailPackage>();
 		
 		//MySQL lookup to prefetch the data for email sent
 		
 		if(emailPackages.size()>0)
 		{
 			sentEmails= outboxDao.getSentEmailPackages(emailPackages.get(0).getMemberId());
 		
 		}
		
		// this method ignores all the packages that are sent between sendDuration days and 60 + sendDuration
		// ex :if the incoming occasion is duress - it will not send the occasion, if duress occasion was sent in > 8 and < 68 days
		if(emailPackages != null && emailPackages.size() > 0){
			Iterator<EmailPackage> emailPackagesItr = emailPackages.iterator();
			while(emailPackagesItr.hasNext()){
			EmailPackage emailPackage = emailPackagesItr.next();

				//Individual lookup. This is replaced by the mySQL lookup above.
				//Date sentDate = outboxDao.getSentDate(emailPackage);	
				
				Date sentDate =null;
				for(EmailPackage sentEmailPackage:sentEmails)
				{
					if(comparePackage(sentEmailPackage,emailPackage))
					{
						sentDate = sentEmailPackage.getSentDateTime();
					}
					
				}
								
				//First Rule - Check that when the package was last sent and is it sent with-in sendDuration
				if(sentDate != null){
					OccasionInfo occasionInfo = occasionDao.getOccasionInfo(emailPackage.getMdTagMetaData().getPurchaseOccasion());
					if(occasionInfo  != null){
					    int sendDuration = Integer.parseInt(occasionInfo.getDuration());
					    int daysToCheckInHistory = Integer.parseInt(occasionInfo.getDaysToCheckInHistory());
						int dateDiffDays = CalendarUtil.getDatesDiff(todaysDate, sdformat.parse(sdformat.format(sentDate)));
							if((dateDiffDays >sendDuration ) &&(dateDiffDays < daysToCheckInHistory+sendDuration)){
								logger.info("PERSIST: MemberId : "+ emailPackage.getMemberId() + " | TAG : "+ emailPackage.getMdTagMetaData().getMdTag() +" | CPS STATUS : NOT QUEUED | REASON :  Tag is already sent on "+ sdformat.format(sentDate));
								emailPackagesItr.remove();
							}
						}
					}
				
			}
		}
		return emailPackages;
	}
	//Prasanth new method to verify that the historical package is same as current package.
	
	private boolean comparePackage(EmailPackage sentEmailPackage,
			EmailPackage emailPackage) {
		
   	   	if(! sentEmailPackage.getMdTagMetaData().getPurchaseOccasion().equalsIgnoreCase(emailPackage.getMdTagMetaData().getPurchaseOccasion()))
				return false;
			
			if(!sentEmailPackage.getMdTagMetaData().getBusinessUnit().equalsIgnoreCase(emailPackage.getMdTagMetaData().getBusinessUnit()))
				return false;
			
			if(!sentEmailPackage.getMdTagMetaData().getSubBusinessUnit().equalsIgnoreCase(emailPackage.getMdTagMetaData().getSubBusinessUnit()))
				return false;			   
	 
		return true;
	}

	public List<EmailPackage> fileEmailPackages(List<EmailPackage> emailPackages) throws SQLException {
		//don't queue if sendDate is null
		List<EmailPackage> queuedEmailPackages = null;
		if(emailPackages != null && emailPackages.size() > 0)
		{
			queuedEmailPackages = outboxDao.queueEmailPackages(filterNullDatePackages(emailPackages));
		}
			
		return queuedEmailPackages;
		
	}
	
	public List<EmailPackage> filterNullDatePackages(List<EmailPackage> emailPackages){
		Iterator<EmailPackage> emailPackagesItr = emailPackages.iterator();
        while(emailPackagesItr.hasNext()){
        	EmailPackage emailPackage = emailPackagesItr.next();
	        if(emailPackage.getSendDate() == null){
	        	logger.info("PERSIST: MemberId : "+ emailPackage.getMemberId() + " | TAG : "+ emailPackage.getMdTagMetaData().getMdTag() +" | CPS STATUS : NOT QUEUED | REASON : SendDate = null");
				emailPackagesItr.remove();
			}
        }
		return emailPackages;
	}
	
	public boolean isOccasionResponsysReady(String mdtag) {
		return activeTags.contains(mdtag.substring(0, 5));			
	}
	
	public MemberInfo getMemberInfoObj(String l_id ){
		return memberInfoDao.getMemberInfo(l_id);
	}
	
	
}
