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
        sourceMap.put("SB", "SG");
        memberBrowseDao = new MemberBrowseDao();
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        todayDate = dateFormat.format(new Date());
        date = new LocalDate(new Date());
        cpsOccasionsDao = new CpsOccasionsDao();
        occasionIdMap = cpsOccasionsDao.getcpsOccasionId();
 	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, Integer> incomingBuSubBuMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		System.out.println(incomingBuSubBuMap);
		String loyalty_id = input.getStringByField("loyalty_id");
		String l_id =  SecurityUtils.hashLoyaltyId(loyalty_id);
			
		Map<String, Integer> buSubBuCountsMap = new HashMap<String, Integer>();
	 
		MemberBrowse memberBrowse = memberBrowseDao.getEntireMemberBrowse(l_id);
		
		//no record in memberBrowse for this member, insert the member and publish to kafka
		if(memberBrowse == null ){
			insertMemberBrowseToday(l_id, incomingBuSubBuMap);
		}
		
		/*If the member has records, iterate through the required number of dates and 
		 *aggregate the counts for every incoming buSubBu from all feedTypes and publish to kafka*/
		else{
			Map<String, DateSpecificMemberBrowse> memberBrowseMap = memberBrowse.getMemberBrowse();
		
			for(int i=0; i<NUMBER_OF_DAYS; i++){
				Date currentdate = date.minusDays(i).toDateMidnight().toDate();
				String stringDate = dateFormat.format(currentdate);
				
				if(memberBrowseMap.containsKey(stringDate)){
					DateSpecificMemberBrowse dateSpecificMemberBrowse = memberBrowseMap.get(stringDate);
					
					for(String buSubBu : incomingBuSubBuMap.keySet()){
						int pc = 0;
						if(dateSpecificMemberBrowse.getBuSubBu().containsKey(buSubBu)){
							Map<String, Integer> feedCountsMap = dateSpecificMemberBrowse.getBuSubBu().get(buSubBu);
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
				emitToKafka(loyalty_id, incomingBuSubBuMap, buSubBuCountsMap);
				/*
				 * If the member has document for today, update the counts with incoming buSubBu counts
				 */
				if(memberBrowseMap.containsKey(todayDate)){
					updateMemberBrowseToday(incomingBuSubBuMap, l_id, memberBrowseMap);
				}
				/*
				 * If the member does not have document for today, insert a new document for today
				 */
				else{
					insertMemberBrowseToday(l_id, incomingBuSubBuMap);
				}
				
		}
	}

	private void updateMemberBrowseToday(Map<String, Integer> incomingBuSubBuMap,
			String l_id, Map<String, DateSpecificMemberBrowse> memberBrowseMap) {
		if(memberBrowseMap.containsKey(todayDate)){
		DateSpecificMemberBrowse dateSpecificMemberBrowse = memberBrowseMap.get(todayDate);
		for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
			//if the member already had the incoming tag, check whether the current source has a count
			if(dateSpecificMemberBrowse.getBuSubBu().keySet().contains(browseBuSubBu)){
				Map<String, Integer> feedCountsMap = dateSpecificMemberBrowse.getBuSubBu().get(browseBuSubBu);
				
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
				dateSpecificMemberBrowse.getBuSubBu().put(browseBuSubBu, newFeedCountsMap);
			}
		}
			updateMemberBrowse( dateSpecificMemberBrowse, l_id);
		}
	}
	
	public void insertMemberBrowseToday(String l_id, Map<String, Integer> incomingBuSubBuMap){
		DateSpecificMemberBrowse dateSpecificMemberBrowse = new DateSpecificMemberBrowse();
		Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
		 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
			 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
			 newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
			 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
		 }
	
		 dateSpecificMemberBrowse.setBuSubBu(browseBuSubBufeedCountsMap);
		 updateMemberBrowse( dateSpecificMemberBrowse, l_id);
	}
	
	public void updateMemberBrowse(DateSpecificMemberBrowse dateSpecificMemberBrowse, String l_id){
		dateSpecificMemberBrowse.setDate(todayDate);
		memberBrowseDao.updateMemberBrowse(l_id, dateSpecificMemberBrowse);
	}
	
	public void emitToKafka(String loyalty_id, Map<String, Integer> incomingBuSubBuMap, Map<String, Integer> buSubBuCountsMap){

		
		List<String> buSubBuList = new ArrayList<String>(); 
		/*System.out.println("Previous counts: ");
		for(String key: buSubBuCountsMap.keySet()){
			System.out.println(key +": " + buSubBuCountsMap.get(key));
		}*/
		if(!buSubBuCountsMap.isEmpty()){
			for(String buSubBu : buSubBuCountsMap.keySet()){
				int PC = buSubBuCountsMap.get(buSubBu);
				int IC = incomingBuSubBuMap.get(buSubBu);
				if(PC < THRESHOLD && (PC + IC) >= THRESHOLD){
					buSubBuList.add(buSubBu+occasionIdMap.get(web));
				}
			}
		}
		
		else{
			for(String buSubBu : incomingBuSubBuMap.keySet()){
				if(incomingBuSubBuMap.get(buSubBu) >= 4){
					buSubBuList.add(buSubBu+occasionIdMap.get(web));
				}
			}
		}
		
		//publish to kafka
		if(!buSubBuList.isEmpty()){
			Map<String, Object> mapToBeSend = new HashMap<String, Object>();
			mapToBeSend.put("memberId", loyalty_id);
			//mapToBeSend.put("occasionId", occasionIdMap.get(web)); 
			mapToBeSend.put("buSubBu", buSubBuList);
			String jsonToBeSend = new Gson().toJson(mapToBeSend );
			try {
				kafkaUtil.sendKafkaMSGs(jsonToBeSend.toString(), browseKafkaTopic);
			} catch (ConfigurationException e) {
				LOGGER.error("Exception in kakfa " + ExceptionUtils.getFullStackTrace(e));
			}
		}
		
	}

}
