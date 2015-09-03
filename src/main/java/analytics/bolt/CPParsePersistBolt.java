package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.bolt.ParsingBoltOccassion;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
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

public class CPParsePersistBolt extends ParsingBoltOccassion{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private TagMetadataDao tagsMetaDataDao;
	private TagResponsysActiveDao tagResponsysActiveDao;
	private List<String> activeTags;
	
	public CPParsePersistBolt(String env) {
		super(env);		
	}		

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		tagsMetaDataDao = new TagMetadataDao();
		memberMDTags2Dao = new MemberMDTags2Dao();
		tagResponsysActiveDao = new TagResponsysActiveDao();
		activeTags= tagResponsysActiveDao.getActiveResponsysTagsList();
	}

	@Override
	public void execute(Tuple input) {
		
		redisCountIncr("CPParsePersistBolt_input_count");	
		
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
				LOGGER.info("messageID = " + messageID);
			}
			LOGGER.debug("TIME:" + messageID + "-Entering ParsingboltOccasion-"
					+ System.currentTimeMillis());
			
			LOGGER.info("Message Being Received " + input.getString(0));
			redisCountIncr("incoming_tuples");
			
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = null;
			jsonElement = getParsedJson(input, parser);
			String tagsString = convertTagsJsonToString(jsonElement);
				
			// Fetch l_id from json
			JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
			if (lyl_id_no == null) {
				LOGGER.error("Invalid incoming json with empty loyalty id");
				outputCollector.ack(input);
				return;
			}
			if (lyl_id_no.getAsString().length() != 16) {
				LOGGER.error("invalid loyalty id");
				outputCollector.ack(input);
				return;
			}
			
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());		

			// Get list of tags from incoming json			
			List<String> tagsList = getTagsFromJsonString(tagsString);	
			LOGGER.info("PERSIST: Input Tags for Lid " + lyl_id_no + " : "+ tagsList.toString());
			
			if(tagsList != null && tagsList.size()>0){
				//filter top5% tags that responsys is not ready for.
				List<String> filteredTagsList = filterResponsysNotReadyTop5PercentTags(l_id,tagsList);
				
				//Persist filtered MdTags into memberMdTagsWithDates collection
				if(filteredTagsList != null && filteredTagsList.size()>0)
					memberMDTags2Dao.addMemberMDTags(l_id, filteredTagsList);				
			}
			else{
				memberMDTags2Dao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST OCCASION DELETE: " + l_id);
			}


			if ( StringUtils.isNotEmpty(tagsString)) {
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit = new ArrayList<Object>();
				listToEmit.add(lyl_id_no.getAsString());
				listToEmit.add(l_id);				
				this.outputCollector.emit(listToEmit);
			}
			else{
				LOGGER.info("PERSIST: No Tags found for lyl_id_no " + input.getStringByField("lyl_id_no"));
				countMetric.scope("no_lyl_id_no").incr();
			}
			
			redisCountIncr("CPParsePersistBolt_output_count");	
				
				
		} catch (Exception e) {
			LOGGER.error("exception in parsing: " + e);
		} 
		// LOGGER.info("TIME:" + messageID + "-Exiting ParsingboltOccasion-" +
		outputCollector.ack(input);
	}

	
	private List<String> filterResponsysNotReadyTop5PercentTags(String l_id, List<String> tagsList) {
		List<String> filteredTagsLst = new ArrayList<String>();
		List<String> inactiveTop5TagsLst = new ArrayList<String>();
		for(String mdtag : tagsList){
			if(isTop5Percent(mdtag)){
				if(!isOccasionResponsysReady(mdtag))
				{
					inactiveTop5TagsLst.add(mdtag);
				}
				else
					filteredTagsLst.add(mdtag);
			}
			else
				filteredTagsLst.add(mdtag);
		}
		//Delete the top5percent mdtags that responsys is not ready for, from mdTagsWithDates collection.
		if(inactiveTop5TagsLst.size() > 0)
			memberMDTags2Dao.deleteMemberMDTags(l_id,inactiveTop5TagsLst);
		return filteredTagsLst;
	}


	private boolean isOccasionResponsysReady(String mdtag) {
		return activeTags.contains(mdtag.substring(0, 5));			
	}

	private boolean isTop5Percent(String mdtag) {
		TagMetadata tagMetaData = tagsMetaDataDao.getDetails(mdtag);
		if(tagMetaData != null){
			//Check for the 6th char of the mdtag to be 8
			if(tagMetaData.getMdTag().substring(5, 6).equals(MongoNameConstants.top5PercentTag))
				return true;
			else
				return false;			
		}
		return false;		
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
		declarer.declare(new Fields("lyl_id_no", "l_id"));
	}
}