package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.util.SecurityUtils;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.objects.TagVariable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class TagProcessingBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TagProcessingBolt.class);
	private OutputCollector outputCollector;
	Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
	Map<String, String> modelScoreMap = new HashMap<String, String>();
	private MemberMDTags2Dao memberMDTags2Dao;
	private CpsOccasionsDao cpsOccasion;
	private HashMap<String, String> cpsOccasionPriorityMap;
	private HashMap<String, String> cpsOccasionDurationMap;
	

	public TagProcessingBolt(String systemProperty, String host, int port) {
		super(systemProperty);
	}

	public TagProcessingBolt(String systemProperty) {
		super(systemProperty);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;

		memberMDTags2Dao = new MemberMDTags2Dao();
		cpsOccasion = new CpsOccasionsDao();
		cpsOccasionPriorityMap = cpsOccasion.getcpsOccasionPriority();
		cpsOccasionDurationMap = cpsOccasion.getcpsOccasionDurations();
	}

	@Override
	public void execute(Tuple input) {

		redisCountIncr("TagProcessingBolt_input_count");	
		
		try {
			
			LOGGER.info("~~~~~~~~~~~Incoming tuple in TagProcessingBolt: " + input);

			// Fetch l_id from json
			String lyl_id_no;
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = (JsonElement) parser.parse((String)input.getValue(0));
			if (jsonElement.getAsJsonObject().get("memberId") != null) {
				lyl_id_no = jsonElement.getAsJsonObject().get("memberId").getAsString();
			} else {
				LOGGER.error("Invalid incoming json");
				outputCollector.ack(input);
				return;
			}
			if (lyl_id_no == null || lyl_id_no.length() != 16) {
				LOGGER.error("empty_lid");
				outputCollector.ack(input);
				return;
			}
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);

			// Get list of tags from json
			StringBuilder tagsString = new StringBuilder();
			JsonArray tags = (JsonArray) jsonElement.getAsJsonObject().get("buSubBu");;

			LOGGER.info("PERSIST: Input Tags for Lid " + lyl_id_no + " : "+ tags.toString());
			
			// List<Object> emitToPersist = new ArrayList<Object>();
			persistTagsToMemberTagsColl(l_id, tagsString, tags);
			LOGGER.debug("Scoring for " + l_id);
			
			//Persisting the MdTags into Mongo
			if(tagsString != null && !tagsString.toString().isEmpty()){
				List<String> tagsLst = new ArrayList<String>();
				String[] tagsArray = tagsString.toString().split(",");
				tagsLst = Arrays.asList(tagsArray);
				
				//Write to the mdTags with dates collection as well...
				memberMDTags2Dao.addRtsMemberTags(l_id, tagsLst,cpsOccasionDurationMap,cpsOccasionPriorityMap);
			}
			else{
				memberMDTags2Dao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST MD Tags DELETE: " + l_id);
			}


			if ( tagsString != null && !tagsString.toString().isEmpty()) {
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit = new ArrayList<Object>();
				listToEmit.add(lyl_id_no);
				listToEmit.add(l_id);
				this.outputCollector.emit(listToEmit);
			}
			else{
				LOGGER.info("PERSIST: No Tags found for lyl_id_no " + input.getStringByField("memberId"));
				countMetric.scope("no_lyl_id_no").incr();
			}
			
			redisCountIncr("TagProcessingBolt_output_count");	
				
				
		} catch (Exception e) {
			LOGGER.error("exception in TagProcessingBolt: " + e);
		} 
		// LOGGER.info("TIME:" + messageID + "-Exiting ParsingboltOccasion-" +
		outputCollector.ack(input);
	}

	
	public void persistTagsToMemberTagsColl(String l_id,
			StringBuilder tagsString, JsonArray tags) {
		for (int i = 0; i < tags.size(); i++) {
			tagsString.append(tags.get(i).getAsString());
			if (i != tags.size() - 1)
				tagsString.append(",");
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declareStream("kafka_stream", new Fields("lyl_id_no"));
	}
}
