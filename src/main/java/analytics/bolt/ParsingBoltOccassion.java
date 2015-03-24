package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import analytics.util.HostPortUtility;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.OccasionVariableDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingBoltOccassion extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private TagMetadataDao tagMetadataDao;
	private TagVariableDao tagVariableDao;
	private MultiCountMetric countMetric;
	private MemberMDTagsDao memberTagDao;
	private ModelPercentileDao modelPercDao;
	
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }
	 
	 public void setMemberTagsDao(){
		 memberTagDao = new MemberMDTagsDao();
	 }
	 public void setTagMetadataDao(){
		 tagMetadataDao = new TagMetadataDao();
	 }
	 public void setTagVariableDao(){
		 tagVariableDao = new TagVariableDao();
	 }
		 
	 public void setModelPercDao(){
		 modelPercDao = new ModelPercentileDao();
	 }

	 public ParsingBoltOccassion(String systemProperty){
		 super(systemProperty);
		
	 }
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		tagMetadataDao = new TagMetadataDao();
		tagVariableDao = new TagVariableDao();
		memberTagDao = new MemberMDTagsDao();
		modelPercDao = new ModelPercentileDao();
		initMetrics(context);
	}

	@Override
	public void execute(Tuple input) {
		//System.out.println("IN PARSING BOLT: " + input);
		countMetric.scope("incoming_tuples").incr();
		Map<String, String> variableValueTagsMap = new HashMap<String, String>();
		JsonParser parser = new JsonParser();
		JsonElement jsonElement= null;
		try{
		jsonElement = getParsedJson(input, parser);
		}
		catch(Exception e){
			LOGGER.error("exception in parsing: " + e);
		}
		
		//Fetch l_id from json
		JsonElement lyl_id_no = null;
		if(jsonElement.getAsJsonObject().get("lyl_id_no") != null){
		 lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
		}
		else{
			LOGGER.error("Invalid incoming json");
			return;
		}
		if (lyl_id_no == null || lyl_id_no.getAsString().length()!=16) {
			countMetric.scope("empty_lid").incr();
			outputCollector.ack(input);
			return;
		} 
		String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
		//System.out.println(l_id);
		
		//Get list of tags from json
		StringBuilder tagsString = new StringBuilder();
		JsonArray tags = null;
		tags = getTagsFromInput(jsonElement);
				
		//reset the variableValueMap to 0 before persisting new incoming tags
		resetVariableValuesMap(variableValueTagsMap, l_id);
			
		List<Object> emitToPersist = new ArrayList<Object>();
		persistTagsToMemberTagsColl(l_id, tagsString, tags);
		emitToPersist.add(l_id);
		emitToPersist.add(tagsString.toString());
		this.outputCollector.emit("persist_stream", emitToPersist);
		LOGGER.debug("Scoring for " + l_id);
			
		if (tags != null && tags.size() != 0) {
			for (JsonElement tag : tags) {
				TagMetadata tagMetaData = getTagMetaData(tag);
				if (tagMetaData != null) {//TODO: Add list of tagMetadatas you can process. Egh- ignore unwanted
							
					Map<String, String> tagVariable = getTagVariable(tag);
					String tagVariableValue = getTagVarValue(tagVariable.get(tagVariable.keySet().iterator().next()));
					
					if(tagVariable!=null && tagVariableValue!=null &&!tagVariable.isEmpty() )
					{
						populateVariableValueTagsMap(variableValueTagsMap,
								tagVariableValue, tagVariable.keySet().iterator().next());
						countMetric.scope("tag_variable_added").incr();
					}
					else{
						countMetric.scope("no_tag_variable").incr();
					}
				}
				else{
					countMetric.scope("unwanted_tag_metadata").incr();
				}
			}
		} 
		//Even if there are no new tags and the list is null, we need to process the deletes
		if(variableValueTagsMap!=null && !variableValueTagsMap.isEmpty())
		{
			List<Object> listToEmit = new ArrayList<Object>();
	    	listToEmit.add(l_id);
	    	listToEmit.add(JsonUtils.createJsonFromStringStringMap(variableValueTagsMap));
	    	listToEmit.add("PurchaseOccasion");
	    	countMetric.scope("emitted_to_scoring").incr();
	    	this.outputCollector.emit(listToEmit);
		}
		else{
	    	countMetric.scope("no_variables_affected").incr();			
		}
    	outputCollector.ack(input);
	}

	public JsonElement getParsedJson(Tuple input, JsonParser parser) throws JsonSyntaxException{
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
		String tagVariableValue = modelPercDao.getSingleModelPercentile(modelId);
				
		return tagVariableValue;
	}

	public TagMetadata getTagMetaData(JsonElement tag) {
		TagMetadata tagMetaData = tagMetadataDao.getDetails(tag
				.getAsString());
		return tagMetaData;
	}

	public void persistTagsToMemberTagsColl(String l_id,
			StringBuilder tagsString, JsonArray tags) {
		for(int i=0; i<tags.size(); i++){
			tagsString.append(tags.get(i).getAsString());
			if(i != tags.size()-1)
				tagsString.append(",");
		}
		//return tagsString.toString();
	}

	public void resetVariableValuesMap(
			Map<String, String> variableValueTagsMap, String l_id) {
		//Reset all variables to 0
				List<String> memberTags = memberTagDao.getMemberMDTags(l_id);
				if(memberTags != null){
				List<String> tagVarList= tagVariableDao.getTagVariablesList(memberTags);
				if(!tagVarList.isEmpty()){
					for(String var:tagVarList){
						variableValueTagsMap.put(var, "0");
					}
				}
				}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source"));
		declarer.declareStream("persist_stream", new Fields("l_id", "tags"));
	}

}
