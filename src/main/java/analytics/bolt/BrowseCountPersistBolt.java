package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import analytics.util.JsonUtils;
import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberBrowseDao;
import analytics.util.dao.SourceFeedDao;
import analytics.util.objects.DateSpecificMemberBrowse;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BrowseCountPersistBolt extends EnvironmentBolt{
	
	private static final int THRESHOLD = 4;
	private static final int NUMBER_OF_DAYS = 1;
	static final Logger LOGGER = LoggerFactory
			.getLogger(BrowseCountPersistBolt.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
	Map<String, String> sourceMap = new HashMap<String, String>();
	private String source;
	private String todayDate;
	MemberBrowseDao memberBrowseDao;
	private LocalDate date;
	SimpleDateFormat dateFormat;
	private KafkaUtil kafkaUtil;
	private String browseKafkaTopic;
	private Map<String, String> occasionIdMap;
	private CpsOccasionsDao cpsOccasionsDao;		
	private String web;
	private SourceFeedDao sourceFeedDao;
	
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
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
        kafkaUtil= new KafkaUtil(System.getProperty(MongoNameConstants.IS_PROD));
        /*sourceMap.put("SB", "SG");
        sourceMap.put("InternalSearch", "IS");
        sourceMap.put("BROWSE", "PR");*/
        memberBrowseDao = new MemberBrowseDao();
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        todayDate = dateFormat.format(new Date());
        date = new LocalDate(new Date());
        cpsOccasionsDao = new CpsOccasionsDao();
        occasionIdMap = cpsOccasionsDao.getcpsOccasionId();
        
        sourceFeedDao = new SourceFeedDao();
        sourceMap = sourceFeedDao.getSourceFeedMap();
 	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, Integer> incomingBuSubBuMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		String loyalty_id = input.getStringByField("loyalty_id");
		String l_id =  SecurityUtils.hashLoyaltyId(loyalty_id);
		List<String> buSubBuListToKafka = updateMemberBrowseAndKafkaList(loyalty_id, l_id, incomingBuSubBuMap);
		publishToKafka(buSubBuListToKafka, loyalty_id, l_id);
		outputCollector.ack(input);
	}

	public List<String> updateMemberBrowseAndKafkaList(String loyalty_id, String l_id, Map<String, Integer> incomingBuSubBuMap) {
	
		LOGGER.info("Incoming buSubBu for " + l_id + " from " + sourceMap.get(source) +": " + incomingBuSubBuMap);	
		System.out.println("incoming " + incomingBuSubBuMap);
		Map<String, Integer> buSubBuCountsMap = new HashMap<String, Integer>();
	 
		MemberBrowse memberBrowse = memberBrowseDao.getEntireMemberBrowse(l_id);
		List<String> buSubBuListToKafka = new ArrayList<String>();
		//no record in memberBrowse for this member, insert the member and publish to kafka
		if(memberBrowse == null ){
			LOGGER.info("Record getting inserted for " + l_id + " from " + sourceMap.get(source));
		//	validateAndEmitToKafka(loyalty_id, l_id, incomingBuSubBuMap, buSubBuCountsMap);
			buSubBuListToKafka = getBuSubBuList(loyalty_id, l_id, incomingBuSubBuMap, buSubBuCountsMap);
			insertMemberBrowseToday(l_id, incomingBuSubBuMap);
		}
		
		/*If the member has records, iterate through the required number of dates and 
		 *aggregate the counts for every incoming buSubBu from all feedTypes and publish to kafka*/
		else{
			Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
			for(int i=0; i<NUMBER_OF_DAYS; i++){
				Date currentdate = date.minusDays(i).toDateMidnight().toDate();
				String stringDate = dateFormat.format(currentdate);
				
				if(dateSpeBuSubBuMap.containsKey(stringDate)){
					Map<String, Map<String, Integer>> buSubBuMap = dateSpeBuSubBuMap.get(stringDate);
					
					for(String buSubBu : incomingBuSubBuMap.keySet()){
						int pc = 0;
						if(buSubBuMap.containsKey(buSubBu)){
							Map<String, Integer> feedCountsMap = buSubBuMap.get(buSubBu);
							for(String feed : feedCountsMap.keySet()){
								pc = pc + feedCountsMap.get(feed);
							}
							if(!buSubBuCountsMap.containsKey(buSubBu)){
								buSubBuCountsMap.put(buSubBu, pc);
							}
							else{
								int count = buSubBuCountsMap.get(buSubBu) + pc;
								buSubBuCountsMap.put(buSubBu, count);
							}
						}
						
						else{
							if(!buSubBuCountsMap.containsKey(buSubBu)){
								buSubBuCountsMap.put(buSubBu, 0);
							}
						}
					}
				}
			}
				
			for(String key : buSubBuCountsMap.keySet()){
				System.out.println("PC " + key +": " + buSubBuCountsMap.get(key));
			}
			//validateAndEmitToKafka(loyalty_id, l_id, incomingBuSubBuMap, buSubBuCountsMap);
			buSubBuListToKafka = getBuSubBuList(loyalty_id, l_id, incomingBuSubBuMap, buSubBuCountsMap);
			/*
			 * If the member has document for today, update the counts with incoming buSubBu counts
			 */
			if(dateSpeBuSubBuMap.containsKey(todayDate)){
				updateMemberBrowseToday(l_id, incomingBuSubBuMap, memberBrowse);
				LOGGER.info("Today's record getting updated for " + l_id + " from " + sourceMap.get(source));
			}
			/*
			 * If the member does not have document for today, insert a new document for today
			 */
			else{
				insertMemberBrowseToday(l_id, incomingBuSubBuMap);
				LOGGER.info("Today's record getting inserted for " + l_id + " from " + sourceMap.get(source));
			}
		}
			return buSubBuListToKafka;
	}

	private void updateMemberBrowseToday(String l_id, Map<String, Integer> incomingBuSubBuMap, MemberBrowse memberBrowse) {
	
		Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
		
		if(dateSpeBuSubBuMap.containsKey(todayDate)){
			Map<String, Map<String, Integer>> buSubBuMap = dateSpeBuSubBuMap.get(todayDate);
			
			for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
				//if the member already had the incoming tag, check whether the current source has a count
				if(buSubBuMap.containsKey(browseBuSubBu)){
					Map<String, Integer> feedCountsMap = buSubBuMap.get(browseBuSubBu);
					
					//if the current source has a count, add the incoming tag count to the existing one 
					if(feedCountsMap.containsKey(sourceMap.get(source))){
						int count = feedCountsMap.get(sourceMap.get(source)) + incomingBuSubBuMap.get(browseBuSubBu);
						feedCountsMap.put(sourceMap.get(source), count);
					}
					
					//if the current source does not have a count, create a map for it
					else{
						feedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
					}
				}
				
				//if the member does not have the incoming tag, create a document for the tag
				else{
					Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
					newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
					dateSpeBuSubBuMap.get(todayDate).put(browseBuSubBu, newFeedCountsMap);
				}
			}
				memberBrowse.setL_id(l_id);
				memberBrowse.setDateSpecificBuSubBu(dateSpeBuSubBuMap);
				memberBrowseDao.updateMemberBrowse(memberBrowse);
		}
	}
	
	
	
	public void insertMemberBrowseToday(String l_id, Map<String, Integer> incomingBuSubBuMap){
		MemberBrowse memberBrowse = new MemberBrowse();
		memberBrowse.setL_id(l_id);
		Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = new HashMap<String, Map<String,Map<String,Integer>>>();
		Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
		 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
			 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
			 newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
			 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
		 }
		 dateSpeBuSubBuMap.put(todayDate, browseBuSubBufeedCountsMap);
		 memberBrowse.setDateSpecificBuSubBu(dateSpeBuSubBuMap);
		 memberBrowseDao.updateMemberBrowse(memberBrowse);
	}
	
	public void validateAndEmitToKafka(String loyalty_id, String l_id, Map<String, Integer> incomingBuSubBuMap, Map<String, Integer> buSubBuCountsMap){

		
		List<String> buSubBuList = getBuSubBuList(loyalty_id, l_id, incomingBuSubBuMap, buSubBuCountsMap); 
			
		//publish to kafka
		if(buSubBuList != null && !buSubBuList.isEmpty()){
			Map<String, Object> mapToBeSend = new HashMap<String, Object>();
			mapToBeSend.put("memberId", loyalty_id);
			//mapToBeSend.put("occasionId", occasionIdMap.get(web)); 
			mapToBeSend.put("buSubBu", buSubBuList);
			String jsonToBeSend = new Gson().toJson(mapToBeSend );
			try {
				kafkaUtil.sendKafkaMSGs(jsonToBeSend.toString(), browseKafkaTopic);
				System.out.println("to kafka " + buSubBuList);
				LOGGER.info(l_id + "--" + loyalty_id + " published to kafka for " + sourceMap.get(source));
				redisCountIncr("browse_to_kafka");
			} catch (ConfigurationException e) {
				LOGGER.error("Exception in kakfa " + ExceptionUtils.getFullStackTrace(e));
			}
		}
		
		else{
			redisCountIncr("browse_not_to_kafka");
		}
		
	}
	
	
	public List<String> getBuSubBuList(String loyalty_id, String l_id, Map<String, Integer> incomingBuSubBuMap, Map<String, Integer> existingBuSubBuCountsMap){
		List<String> buSubBuList = new ArrayList<String>(); 
		/*System.out.println("Previous counts: ");
		for(String key: buSubBuCountsMap.keySet()){
			System.out.println(key +": " + buSubBuCountsMap.get(key));
		}*/
		if(!existingBuSubBuCountsMap.isEmpty()){
			for(String buSubBu : existingBuSubBuCountsMap.keySet()){
				int PC = existingBuSubBuCountsMap.get(buSubBu);
				int IC = incomingBuSubBuMap.get(buSubBu);
				if(PC < THRESHOLD && (PC + IC) >= THRESHOLD){
					buSubBuList.add(buSubBu+occasionIdMap.get(web));
				}
			}
		}
		
		else{
			for(String buSubBu : incomingBuSubBuMap.keySet()){
				if(incomingBuSubBuMap.get(buSubBu) >= THRESHOLD){
					buSubBuList.add(buSubBu+occasionIdMap.get(web));
				}
			}
		}
		return buSubBuList;
	}
	
	
	public void publishToKafka(List<String> buSubBuListToKafka, String loyalty_id, String l_id){
		//publish to kafka
				if(buSubBuListToKafka != null && !buSubBuListToKafka.isEmpty()){
					Map<String, Object> mapToBeSend = new HashMap<String, Object>();
					mapToBeSend.put("memberId", loyalty_id);
					//mapToBeSend.put("occasionId", occasionIdMap.get(web)); 
					mapToBeSend.put("buSubBu", buSubBuListToKafka);
					String jsonToBeSend = new Gson().toJson(mapToBeSend );
					try {
						kafkaUtil.sendKafkaMSGs(jsonToBeSend.toString(), browseKafkaTopic);
						System.out.println("to kafka " + buSubBuListToKafka);
						LOGGER.info(l_id + "--" + loyalty_id + " published to kafka for " + sourceMap.get(source));
						redisCountIncr("browse_to_kafka");
					} catch (ConfigurationException e) {
						LOGGER.error("Exception in kakfa " + ExceptionUtils.getFullStackTrace(e));
					}
				}
				
				else{
					redisCountIncr("browse_not_to_kafka");
				}
	}

}
