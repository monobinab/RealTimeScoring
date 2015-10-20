package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagResponsysActiveDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class CPParsePersistBolt extends EnvironmentBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(CPParsePersistBolt.class);
	private OutputCollector outputCollector;
	private MemberMDTags2Dao memberMDTags2Dao;

	private CpsOccasionsDao cpsOccasion;
	private HashMap<String, String> cpsOccasionPriorityMap;
	private HashMap<String, String> cpsOccasionDurationMap;
	
	public CPParsePersistBolt(String env) {
		super(env);		
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
		
		redisCountIncr("input_count");	
		
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
				LOGGER.info("messageID = " + messageID);
			}
			LOGGER.debug("TIME:" + messageID + "- Entering CPParsePersistBolt-"
					+ System.currentTimeMillis());
			
			LOGGER.info("Message Being Received " + input.getString(0));
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = null;
			jsonElement = getParsedJson(input, parser);
			String tagsString = convertTagsJsonToString(jsonElement);
				
			// Fetch l_id from json
			JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
			if (lyl_id_no == null) {
				LOGGER.error("Invalid incoming json with empty loyalty id");
				outputCollector.ack(input);
				redisCountIncr("invalid_loy_id_count");
				return;
			}
			if (lyl_id_no.getAsString().length() != 16) {
				LOGGER.error("PERSIST: Invalid loyalty id -" +lyl_id_no.getAsString());
				outputCollector.ack(input);
				redisCountIncr("invalid_loy_id_count");
				return;
			}
			
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());		

			// Get list of tags from incoming json			
			List<String> tagsList = getTagsFromJsonString(tagsString);	
			LOGGER.info("PERSIST: Input Tags for Lid " + lyl_id_no + " : "+ tagsList.toString());
			
			if(tagsList != null && tagsList.size()>0){
				//Persist MdTags into memberMdTagsWithDates collection
			    if(tagsList != null && tagsList.size()>0){
			    	if(jsonElement.getAsJsonObject().has("tagIdentifier") && 
			    		 jsonElement.getAsJsonObject().get("tagIdentifier").toString().contains("RTS"))
			    		memberMDTags2Dao.addRtsMemberTags(l_id, tagsList,cpsOccasionDurationMap,cpsOccasionPriorityMap);
			    	else
			    		memberMDTags2Dao.addMemberMDTags(l_id, tagsList,cpsOccasionDurationMap,cpsOccasionPriorityMap);
			    }			
			}
			if(tagsList != null && tagsList.size()==0){
				memberMDTags2Dao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST: OCCASION DELETE: " + l_id);
			}
			
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit = new ArrayList<Object>();
			listToEmit.add(lyl_id_no.getAsString());
			//listToEmit.add(l_id);				
			this.outputCollector.emit(listToEmit);			
			
			redisCountIncr("output_count");	
				
				
		} catch (Exception e) {			
			LOGGER.error("PERSIST: CPParsePersistBolt: exception in parsing for memberId :: "+ input.getString(0) + " : " + ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) +"  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			redisCountIncr("exception_count");	
			//outputCollector.fail(input);
		
		} 
		// LOGGER.info("TIME:" + messageID + "-Exiting ParsingboltOccasion-" +
		outputCollector.ack(input);
	}

	public JsonElement getParsedJson(Tuple input, JsonParser parser)
			throws JsonSyntaxException {
		JsonElement jsonElement = parser.parse(input.getString(0));
		return jsonElement;
	}

	public List<String> getTagsFromJsonString(String tagsString) {	
		List<String> tagsLst = new ArrayList<String>();				
		if(StringUtils.isNotEmpty(tagsString)){						
			String[] tagsArray = tagsString.toString().split(",");
			tagsLst = Arrays.asList(tagsArray);
		}					
		return tagsLst;							
	}

	/**
	 * @param jsonElement
	 */
	private String convertTagsJsonToString( JsonElement jsonElement) {
		StringBuilder tagsString = new StringBuilder();		
		JsonArray tags = (JsonArray) jsonElement.getAsJsonObject().get("tags");		
		for (int i = 0; i < tags.size(); i++) {
			tagsString.append(tags.get(i).getAsString());
			if (i != tags.size() - 1)
				tagsString.append(",");
		}
		return tagsString.toString();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no"));
	}
	

	
}
