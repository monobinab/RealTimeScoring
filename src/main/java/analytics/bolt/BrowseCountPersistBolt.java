package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.BrowseUtils;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.BoostBrowseBuSubBuDao;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberBrowseDao;
import analytics.util.dao.SourceFeedDao;
import analytics.util.objects.BoostBrowseBuSubBu;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BrowseCountPersistBolt extends EnvironmentBolt{
	
	private static final int THRESHOLD = 4;
	private static final int NUMBER_OF_DAYS = 1;
	static final Logger LOGGER = LoggerFactory.getLogger(BrowseCountPersistBolt.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
	private String source;
	
	MemberBrowseDao memberBrowseDao;
	
	SimpleDateFormat dateFormat;
	private String browseKafkaTopic;
	private CpsOccasionsDao cpsOccasionsDao;		
	private String web;
	private SourceFeedDao sourceFeedDao;
	protected BoostBrowseBuSubBuDao boostBrowseBuSubBuDao;

	private BrowseUtils browseUtils;

	public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
	
	public BrowseCountPersistBolt(String systemProperty, String source, String web, String browseKafkaTopic) {
		super(systemProperty);
		this.web = web;
		this.source = source;
		this.browseKafkaTopic = browseKafkaTopic;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
        browseUtils = new BrowseUtils(System.getProperty(MongoNameConstants.IS_PROD), browseKafkaTopic);
        memberBrowseDao = new MemberBrowseDao();
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cpsOccasionsDao = new CpsOccasionsDao();
        sourceFeedDao = new SourceFeedDao();
        boostBrowseBuSubBuDao = new BoostBrowseBuSubBuDao();
 	}

	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, String> incomingBoostsMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		
		
		MemberBrowse memberBrowse = (MemberBrowse) input.getValueByField("memberBrowse");
		String loyalty_id = input.getStringByField("loyalty_id");
		String l_id =  SecurityUtils.hashLoyaltyId(loyalty_id);
		Set<String> buSubBuListToKafka = updateMemberBrowseAndKafkaList(loyalty_id, l_id, incomingBoostsMap, memberBrowse);
		
	//	System.out.println("published to kafka " + buSubBuListToKafka);
		publishToKafka(buSubBuListToKafka, loyalty_id, l_id);
		outputCollector.ack(input);
	}

	public Set<String> updateMemberBrowseAndKafkaList(String loyalty_id, String l_id, Map<String, String> incomingModelCodeMap, MemberBrowse memberBrowse) {
	
		Map<String, String> sourceMap = sourceFeedDao.getSourceFeedMap();
		String todayDate = dateFormat.format(new Date());
		LOGGER.info("PERSIST: Incoming modelCode or busSubBu for Browse tag " + l_id + " from " + sourceMap.get(source) +": " + incomingModelCodeMap);	
		//System.out.println("PERSIST: Incoming buSubBu for " + l_id + " from " + sourceMap.get(source) +": " + incomingModelCodeMap);
		Set<String> buSubBuListToKafka = new HashSet<String>();
		Map<String, Integer> previousModelCodeMap = new HashMap<String, Integer>(); 
		
		/*
		 * no record in memberBrowse for this member, insert the member and publish to kafka
		 */
		if(memberBrowse == null ){
			LOGGER.info("PERSIST: Record getting inserted for " + l_id + " from " + sourceMap.get(source));
			buSubBuListToKafka = getBuSubBuList(loyalty_id, l_id, incomingModelCodeMap, previousModelCodeMap);
			insertMemberBrowseToday(l_id, incomingModelCodeMap, todayDate, sourceMap);
		}
		else{
			Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
			previousModelCodeMap = browseUtils.getPreviousBoostCounts(l_id, loyalty_id, incomingModelCodeMap, NUMBER_OF_DAYS, memberBrowse);
				for(String key : previousModelCodeMap.keySet()){
					//System.out.println("PC " + key +": " + previousModelCodeMap.get(key));
					LOGGER.info("PERSIST: PC modelCode or buSubBu for Browse tag " + l_id + "-- "+ key +": " + previousModelCodeMap.get(key));
				}
				buSubBuListToKafka = getBuSubBuList(loyalty_id, l_id, incomingModelCodeMap, previousModelCodeMap);
				/*
				 * If the member has document for today, update the counts with incoming buSubBu counts
				 */
				if(dateSpeBuSubBuMap.containsKey(todayDate)){
					updateMemberBrowseToday(l_id, incomingModelCodeMap,  memberBrowse, todayDate, sourceMap);
					LOGGER.info("PERSIST: Today's record getting updated for " + l_id + " from " + sourceMap.get(source));
				}

				/*
				 * If the member does not have document for today, insert a new document for today
				 */
				else{
					insertMemberBrowseToday(l_id, incomingModelCodeMap, todayDate, sourceMap);
					LOGGER.info("PERSIST: Today's record getting inserted for " + l_id + " from " + sourceMap.get(source));
				}
		}
				return buSubBuListToKafka;
	}

	private void updateMemberBrowseToday(String l_id, Map<String, String> incomingBuSubBuMap, MemberBrowse memberBrowse, String todayDate, Map<String, String> sourceMap) {
		
		Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
		
		if(dateSpeBuSubBuMap.containsKey(todayDate)){
			Map<String, Map<String, Integer>> buSubBuMap = dateSpeBuSubBuMap.get(todayDate);
			
			for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
				//if the member already had the incoming tag, check whether the current source has a count
				if(buSubBuMap.containsKey(browseBuSubBu)){
					Map<String, Integer> feedCountsMap = buSubBuMap.get(browseBuSubBu);
					
					//if the current source has a count, add the incoming tag count to the existing one 
					if(feedCountsMap.containsKey(sourceMap.get(source))){
						int count = feedCountsMap.get(sourceMap.get(source)) + Integer.valueOf(incomingBuSubBuMap.get(browseBuSubBu));
						feedCountsMap.put(sourceMap.get(source), count);
					}
					
					//if the current source does not have a count, create a map for it
					else{
						feedCountsMap.put(sourceMap.get(source), Integer.valueOf(incomingBuSubBuMap.get(browseBuSubBu)));
					}
				}
				
				//if the member does not have the incoming tag, create a document for the tag
				else{
					Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
					newFeedCountsMap.put(sourceMap.get(source), Integer.valueOf(incomingBuSubBuMap.get(browseBuSubBu)));
					dateSpeBuSubBuMap.get(todayDate).put(browseBuSubBu, newFeedCountsMap);
				}
			}
				memberBrowse.setL_id(l_id);
				memberBrowse.setDateSpecificBuSubBu(dateSpeBuSubBuMap);
				memberBrowseDao.updateMemberBrowse(memberBrowse, todayDate);
		}
	}

	public void insertMemberBrowseToday(String l_id, Map<String, String> incomingBuSubBuMap, String todayDate, Map<String, String> sourceMap){

		MemberBrowse memberBrowse = new MemberBrowse();
		memberBrowse.setL_id(l_id);
		Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = new HashMap<String, Map<String,Map<String,Integer>>>();
		Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
		 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
			 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
			 newFeedCountsMap.put(sourceMap.get(source), Integer.valueOf(incomingBuSubBuMap.get(browseBuSubBu)));
			 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
		 }
		 dateSpeBuSubBuMap.put(todayDate, browseBuSubBufeedCountsMap);
		 memberBrowse.setDateSpecificBuSubBu(dateSpeBuSubBuMap);
		 memberBrowseDao.updateMemberBrowse(memberBrowse, todayDate);
	}

	public Set<String> getBuSubBuList(String loyalty_id, String l_id, Map<String, String> incomingModelCodeMap, Map<String, Integer> existingModelCodeMap){
		Map<String, BoostBrowseBuSubBu> modelCodeToBoostBusSubBuMap = boostBrowseBuSubBuDao.getBoostBuSubBuFromModelCode();
		Set<String> buSubBuList = new HashSet<String>(); 
		
		for(String modelCode : incomingModelCodeMap.keySet()){
			int IC = Integer.valueOf(incomingModelCodeMap.get(modelCode));
			if(!existingModelCodeMap.isEmpty() && existingModelCodeMap.containsKey(modelCode)){
				int PC = existingModelCodeMap.get(modelCode);
				if(PC < THRESHOLD && (PC + IC) >= THRESHOLD && modelCodeToBoostBusSubBuMap.containsKey(modelCode) && modelCodeToBoostBusSubBuMap.get(modelCode).getBuSubBu() != null){
					buSubBuList.add(modelCode+cpsOccasionsDao.getcpsOccasionId().get(web));
				}
			}
			else{
				if(IC >= THRESHOLD && modelCodeToBoostBusSubBuMap.containsKey(modelCode) && modelCodeToBoostBusSubBuMap.get(modelCode).getBuSubBu() != null){
					buSubBuList.add(modelCode+cpsOccasionsDao.getcpsOccasionId().get(web));
				}
			}
		}
			return buSubBuList;
	}

	public void publishToKafka(Set<String> buSubBuListToKafka, String loyalty_id, String l_id){
		//publish to kafka
		if(buSubBuListToKafka != null && !buSubBuListToKafka.isEmpty()){
			browseUtils.publishToKafka(buSubBuListToKafka, loyalty_id, l_id, source, browseKafkaTopic);
			redisCountIncr("browse_to_kafka");
		}
		else{
			redisCountIncr("browse_not_to_kafka");
		}
	}
}
