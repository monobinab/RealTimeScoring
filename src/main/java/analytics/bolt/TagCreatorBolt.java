package analytics.bolt;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.SecurityUtils;
import analytics.util.TupleParser;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.ModelScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

public class TagCreatorBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(TagCreatorBolt.class);
	private OutputCollector outputCollector;
	TagVariableDao tagVariableDao;
	MemberMDTags2Dao memberMDTags2Dao;
	Map<Integer, String> modelTagsMap = new HashMap<Integer, String>();

	public TagCreatorBolt(String env) {
		super(env);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;		
		
		tagVariableDao = new TagVariableDao();
		modelTagsMap = tagVariableDao.getModelTags();
		memberMDTags2Dao = new MemberMDTags2Dao();
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("TagCreatorBolt_input_count");
		if(input != null)
		{
			try{
				JsonElement jsonElement = TupleParser.getParsedJson(input);
				LOGGER.info("Input from PurchaseScoreKafkaBolt :" + jsonElement.toString());
				JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("memberId");
				
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
				
				if(lyl_id_no!=null){
					
					TypeToken<List<ModelScore>> token = new TypeToken<List<ModelScore>>(){};
					List<ModelScore> modelScoreList = new Gson().fromJson(jsonElement.getAsJsonObject().get("scoresInfo"), token.getType());
					
					//List<ModelScore> modelScoreList = (List<ModelScore>) jsonElement.getAsJsonObject().get("scoresInfo");
					if(modelScoreList != null && !modelScoreList.isEmpty()){
						List<Object> rtsTagsListToEmit = new ArrayList<Object>();
						List<String> rtsTags = new ArrayList<String>();
						JSONArray jsonArray = new JSONArray();
						JSONObject mainJsonObj = new JSONObject();
						boolean blackListed = false;
						for(ModelScore modelScore :  modelScoreList){	
							
							if(modelScore.getScore()==0.0)
								blackListed = true;
							
							if(modelScore.getPercentile() > 95){
								String rtsTag = createTag(modelScore,l_id,Constants.TOP5PRIORITY);
								rtsTags.add(rtsTag);
							}
						}
						if(rtsTags.size()>0){
							mainJsonObj.put("lyl_id_no", lyl_id_no);
							mainJsonObj.put("tags", rtsTags);
							mainJsonObj.put("tagIdentifier", "RTS");
							rtsTagsListToEmit.add(mainJsonObj.toString());
							this.outputCollector.emit("rtsTags_stream",rtsTagsListToEmit);	
						}
						else if(rtsTags.size()==0 && blackListed){
							List<Object> blackedoutListToEmit = new ArrayList<Object>();
							blackedoutListToEmit.add(lyl_id_no);
							this.outputCollector.emit("blackedout_stream",blackedoutListToEmit);
						}
					}
				}
			} catch (Exception e){
				LOGGER.error("Exception Occured in TagCreatorBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				outputCollector.fail(input);	
			}
				
		} else {
			redisCountIncr("null_lid");			
			outputCollector.fail(input);				
		}
		outputCollector.ack(input);
		
	}
	
	private String createTag(ModelScore modelScore, String l_id , int priority) {
		String tag = modelTagsMap.get(new Integer (modelScore.getModelId()));
		
		String mdTag = null;
		//Check if there is an MDTag already in the collection.
		if (tag != null) {
			tag+=priority;
			mdTag = getMdTagIfExists(tag,l_id);
		}
		
		if(tag != null && mdTag == null)
			mdTag = tag;
		
		return mdTag;
	}
	
	private String getMdTagIfExists(String tag, String l_id){
		
		ArrayList<String> tagList = (ArrayList<String>) memberMDTags2Dao.getMemberMDTags(l_id);
		
		for(String tagFromLst : tagList){
			if(tagFromLst.substring(0, 6).equalsIgnoreCase(tag)){
				return tagFromLst; 
			}
		}
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("rtsTags_stream",new Fields( "rtsTags"));
		declarer.declareStream("blackedout_stream", new Fields("lyl_id_no"));

	}
		

}