package analytics.bolt;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.TagMetadata;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParsingBoltOccassion extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private TagMetadataDao tagMetadataDao;
	private TagVariableDao tagVariableDao;
	private MemberMDTagsDao memberTagDao;
	private ModelPercentileDao modelPercDao;
	private JedisPool jedisPool;
	private String host;
	private int port;
	Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
	Map<String, String> modelScoreMap = new HashMap<String, String>();
	private static String unknownTagDigits = "0,7,8";
	

	public ParsingBoltOccassion(String systemProperty, String host, int port) {
		super(systemProperty);
		this.host = host;
		this.port = port;
	}

	public void setMemberTagsDao() {
		memberTagDao = new MemberMDTagsDao();
	}

	public void setTagMetadataDao() {
		tagMetadataDao = new TagMetadataDao();
	}

	public void setTagVariableDao() {
		tagVariableDao = new TagVariableDao();
	}

	public void setModelPercDao() {
		modelPercDao = new ModelPercentileDao();
	}

	public ParsingBoltOccassion(String systemProperty) {
		super(systemProperty);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		tagMetadataDao = new TagMetadataDao();
		tagVariableDao = new TagVariableDao();
		memberTagDao = new MemberMDTagsDao();
		modelPercDao = new ModelPercentileDao();
		
		getTagVariableMap();
		getModelScoreMap();
		//JedisPoolConfig poolConfig = new JedisPoolConfig();
		//poolConfig.setMaxActive(100);
		//jedisPool = new JedisPool(poolConfig, host, port, 100);
	}

	@Override
	public void execute(Tuple input) {
		// System.out.println("IN PARSING BOLT: " + input);
		Jedis jedis = null;
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
			}
			LOGGER.debug("TIME:" + messageID + "-Entering ParsingboltOccasion-"
					+ System.currentTimeMillis());

			// LOGGER.info("~~~~~~~~~~~Incoming tuple in ParsingboltOccasion: "
			// + input);
			countMetric.scope("incoming_tuples").incr();
			//Map<String, String> variableValueTagsMap = new HashMap<String, String>();
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = null;
			jsonElement = getParsedJson(input, parser);

			// Fetch l_id from json
			JsonElement lyl_id_no = null;
			if (jsonElement.getAsJsonObject().get("lyl_id_no") != null) {
				lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
			} else {
				LOGGER.error("Invalid incoming json");
				return;
			}
			if (lyl_id_no == null || lyl_id_no.getAsString().length() != 16) {
				LOGGER.error("empty_lid");
				countMetric.scope("empty_lid").incr();
				outputCollector.ack(input);
				return;
			}
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
			// System.out.println(l_id);

			// Get list of tags from json
			StringBuilder tagsString = new StringBuilder();
			JsonArray tags = null;
			tags = getTagsFromInput(jsonElement);

			/**
			 * Sree. Get the Difference in Tags (Input vs Existing)
			 */
			/*
			 * ArrayList<String> diffTags = findDiffTags(l_id, tags); String
			 * diffTagsString = ""; if(diffTags!= null && diffTags.size()>0){
			 * diffTagsString = getStringFromArray(diffTags); }
			 */

			/**
			 * 4-21-2015. Sree. Get all tags from input and put it on Redis
			 */
			String diffTagsString = "";
			if (tags.size() > 0) {
				for (int i = 0; i < tags.size(); i++) {
					diffTagsString = diffTagsString + tags.get(i).getAsString()
							+ ",";
				}
				diffTagsString = diffTagsString.substring(0,
						diffTagsString.length() - 1);
				
			}
			else{
				LOGGER.info("Empty Input Tags for Lid " + lyl_id_no);
			}
			
			LOGGER.info("PERSIST: Input Tags for Lid " + lyl_id_no + " : "+ diffTagsString);
			
			jedis = new Jedis(host, port, 1800);
			jedis.connect();
			jedis.set("Responses:" + l_id, diffTagsString);
			jedis.expire("Responses:" + l_id, 172800);
			jedis.disconnect();

			// reset the variableValueMap to 0 before persisting new incoming
			// tags
			
			//resetVariableValuesMap(variableValueTagsMap, l_id);

			// List<Object> emitToPersist = new ArrayList<Object>();
			persistTagsToMemberTagsColl(l_id, tagsString, tags);
			// emitToPersist.add(l_id);
			// emitToPersist.add(tagsString.toString());
			// this.outputCollector.emit("persist_stream", emitToPersist);
			LOGGER.debug("Scoring for " + l_id);

			/*if (tags != null && tags.size() != 0) {
				for (JsonElement tag : tags) {
					TagMetadata tagMetaData = getTagMetaData(tag);
					if (tagMetaData != null) {// TODO: Add list of tagMetadatas
												// you
												// can process. Egh- ignore
												// unwanted

						Map<String, String> tagVariable = getTagVariable(tag);
						String tagVariableValue = getTagVarValue(tagVariable
								.get(tagVariable.keySet().iterator().next()));

						if (tagVariable != null
								&& tagVariableValue != null
								&& !tagVariable.isEmpty()
								&& (!(tag.toString().charAt(6) == '0')
										&& !(tag.toString().charAt(6) == '7') && !(tag
										.toString().charAt(6) == '8'))) {
							populateVariableValueTagsMap(variableValueTagsMap,
									tagVariableValue, tagVariable.keySet()
											.iterator().next());
							countMetric.scope("tag_variable_added").incr();
						} else {
							LOGGER.info("No Tag Variable or Tag belongs to 'Unknown/browse/top5%' for lid "+ lyl_id_no +" for tag " + tag);
							countMetric.scope("no_tag_variable").incr();
						}
					} else {
						LOGGER.info("Unwanted tag metadata for lid "+ lyl_id_no +" for tag " + tag);
						countMetric.scope("unwanted_tag_metadata").incr();
					}
				}
			}*/
			
			
			
			/*for(JsonElement tag : tags){
				
				//If the Tag belongs to custom Unknown tag recognized by the 6th digit, then just don't score. 
				if(unknownTagDigits.contains(tag.getAsString().substring(5, 6)))
					continue;
				
				TagVariable tagVar = tagVariablesMap.get(tag.getAsString().substring(0, 5));
				
				if(tagVar !=null){
					String score = modelScoreMap.get(tagVar.getModelId());
					if(score!=null){
			
						populateVariableValueTagsMap(variableValueTagsMap,
								score, tagVar.getVariable());
						
					}
				}
			}*/
			
			// Even if there are no new tags and the list is null, we need to
			// process the deletes
			/*if (variableValueTagsMap != null && !variableValueTagsMap.isEmpty()) {
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit.add(l_id);
				listToEmit.add(tagsString.toString());
				listToEmit.add(JsonUtils
						.createJsonFromStringStringMap(variableValueTagsMap));
				listToEmit.add("PurchaseOccasion");
				listToEmit.add(lyl_id_no.getAsString());
				listToEmit.add(messageID);
				countMetric.scope("emitted_to_scoring").incr();
				this.outputCollector.emit(listToEmit);
			} else {*/
				LOGGER.debug("variableValueTagsMap is null or empty or lid " + lyl_id_no);
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit.add(l_id);
				listToEmit.add(tagsString.toString());
				listToEmit.add("");
				listToEmit.add("");
				listToEmit.add(lyl_id_no.getAsString());
				listToEmit.add(messageID);
				this.outputCollector.emit(listToEmit);
				countMetric.scope("no_variables_affected").incr();
			//}
		} catch (Exception e) {
			LOGGER.error("exception in parsing: " + e);
		} finally {
			//jedisPool.returnResource(jedis);
			if(jedis!=null)
				jedis.disconnect();
		}
		// LOGGER.info("TIME:" + messageID + "-Exiting ParsingboltOccasion-" +
		// System.currentTimeMillis());
		outputCollector.ack(input);
	}

	
	public Map<String, TagVariable> getTagVariableMap(){
		
		if(tagVariablesMap.isEmpty()){
			tagVariablesMap = tagVariableDao.getTagVariables();
			
		}
		return tagVariablesMap;
	}
	
	public Map<String, String> getModelScoreMap(){
		
		if(modelScoreMap.isEmpty()){
			modelScoreMap = modelPercDao.getModelWith98Percentile();
			
		}
		return modelScoreMap;
	}
	
	
	/**
	 * Sree.
	 * 
	 * @param diffTags
	 * @return comma separated string with the elements in the arraylist.
	 */
	private String getStringFromArray(ArrayList<String> diffTags) {
		StringBuilder string = new StringBuilder();
		for (Object str : diffTags) {
			string.append(str.toString());
			string.append(",");
		}
		return (string.toString().substring(0, string.toString().length() - 1));
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

	private void populateVariableValueTagsMap(
			Map<String, String> variableValueTagsMap, String tagVariableValue,
			String tagVariable) {
		variableValueTagsMap.put(tagVariable, tagVariableValue);
	}

	public Map<String, String> getTagVariable(JsonElement tag) {
		Map<String, String> tagVariable = tagVariableDao.getTagVariable(tag
				.getAsString().substring(0, 5));
		return tagVariable;
	}

	public String getTagVarValue(String modelId) {
		String tagVariableValue = modelPercDao
				.getSingleModelPercentile(modelId);

		return tagVariableValue;
	}

	public TagMetadata getTagMetaData(JsonElement tag) {
		TagMetadata tagMetaData = tagMetadataDao.getDetails(tag.getAsString());
		return tagMetaData;
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

	public void resetVariableValuesMap(
			Map<String, String> variableValueTagsMap, String l_id) {
		// Reset all variables to 0
		List<String> memberTags = memberTagDao
				.getMemberMDTagsForVariables(l_id);
		if (memberTags != null) {
			List<String> tagVarList = tagVariableDao
					.getTagVariablesList(memberTags);
			if (!tagVarList.isEmpty()) {
				for (String var : tagVarList) {
					variableValueTagsMap.put(var, "0");
				}
			}else
				LOGGER.info("No reset Variable values for lid " + l_id +"~~"+ memberTags);
		}
	}

	/**
	 * Get the Difference between the Input Tags and the Already existing Tags
	 * in the DB
	 * 
	 * @param l_id
	 * @param newTags
	 * @return List of only new tags...
	 */
	public ArrayList<String> findDiffTags(String l_id, JsonArray newTags) {
		List<String> memberTags = memberTagDao.getMemberMDTags(l_id);

		ArrayList<String> diffTags = new ArrayList<String>();
		for (int i = 0; i < newTags.size(); i++) {
			if (memberTags == null
					|| !memberTags.contains(newTags.get(i).getAsString()))
				diffTags.add(newTags.get(i).getAsString());
		}
		return diffTags;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "tags", "lineItemAsJsonString",
				"source", "lyl_id_no", "messageID"));
	}

}
