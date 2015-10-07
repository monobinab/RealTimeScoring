package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.util.SecurityUtils;
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

public class ParsingBoltOccassion extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private MemberMDTagsDao memberTagDao;
	Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
	Map<String, String> modelScoreMap = new HashMap<String, String>();
	private MemberMDTags2Dao memberMDTags2Dao;
	

	public ParsingBoltOccassion(String systemProperty, String host, int port) {
		super(systemProperty);
	}

	public void setMemberTagsDao() {
		memberTagDao = new MemberMDTagsDao();
	}

	public ParsingBoltOccassion(String systemProperty) {
		super(systemProperty);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberTagDao = new MemberMDTagsDao();
		memberMDTags2Dao = new MemberMDTags2Dao();
	}

	@Override
	public void execute(Tuple input) {
		// System.out.println("IN PARSING BOLT: " + input);
		redisCountIncr("ParsingBoltOccassion_input_count");	
		
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
			}
			LOGGER.debug("TIME:" + messageID + "-Entering ParsingboltOccasion-"
					+ System.currentTimeMillis());
			
			LOGGER.info("Message Being Received " + messageID);

			// LOGGER.info("~~~~~~~~~~~Incoming tuple in ParsingboltOccasion: "
			// + input);
			//countMetric.scope("incoming_tuples").incr();
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = null;
			jsonElement = getParsedJson(input, parser);

			// Fetch l_id from json
			JsonElement lyl_id_no = null;
			if (jsonElement.getAsJsonObject().get("lyl_id_no") != null) {
				lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
			} else {
				LOGGER.error("Invalid incoming json");
				outputCollector.ack(input);
				return;
			}
			if (lyl_id_no == null || lyl_id_no.getAsString().length() != 16) {
				LOGGER.error("empty_lid");
				outputCollector.ack(input);
				return;
			}
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
			// System.out.println(l_id);

			// Get list of tags from json
			StringBuilder tagsString = new StringBuilder();
			JsonArray tags = null;
			tags = getTagsFromInput(jsonElement);

	
			LOGGER.info("PERSIST: Input Tags for Lid " + lyl_id_no + " : "+ tags.toString());
			
			// List<Object> emitToPersist = new ArrayList<Object>();
			persistTagsToMemberTagsColl(l_id, tagsString, tags);
			LOGGER.debug("Scoring for " + l_id);
			
			//Persisting the MdTags into Mongo
			if(tagsString != null && !tagsString.toString().isEmpty()){
				List<String> tagsLst = new ArrayList<String>();
				String[] tagsArray = tagsString.toString().split(",");
				tagsLst = Arrays.asList(tagsArray);
				memberTagDao.addMemberMDTags(l_id, tagsLst);
				
				//Write to the mdTags with dates collection as well...
				memberMDTags2Dao.addMemberMDTags(l_id, tagsLst);
			}
			else{
				memberTagDao.deleteMemberMDTags(l_id);
				memberMDTags2Dao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST OCCATION DELETE: " + l_id);
			}


			if ( tagsString != null && !tagsString.toString().isEmpty()) {
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit = new ArrayList<Object>();
				listToEmit.add(lyl_id_no.getAsString());
				listToEmit.add(tagsString.toString());
				listToEmit.add(messageID);
				this.outputCollector.emit(listToEmit);
			}
			else{
				LOGGER.info("PERSIST: No Tags found for lyl_id_no " + input.getStringByField("lyl_id_no"));
				countMetric.scope("no_lyl_id_no").incr();
			}
			
			redisCountIncr("ParsingBoltOccassion_output_count");	
				
				
		} catch (Exception e) {
			LOGGER.error("exception in parsing: " + e);
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
		// return tagsString.toString();
	}
	

	public JsonElement getParsedJson(Tuple input, JsonParser parser)
			throws JsonSyntaxException {
		JsonElement jsonElement = parser.parse(input
				.getStringByField("message"));
		return jsonElement;
	}

	public JsonArray getTagsFromInput(JsonElement jsonElement) {
		JsonArray tags = (JsonArray) jsonElement.getAsJsonObject().get("tags");
		return tags;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no", "tags", "messageID"));
	}
}
