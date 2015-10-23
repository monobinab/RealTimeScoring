package analytics.bolt;


import java.math.BigInteger;
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
	private static BigInteger startLoyalty = new BigInteger("7081010000647509"); 
	private static BigInteger lastLoyalty = new BigInteger("7081117556061439");

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

	/**
	 * @return the modelTagsMap
	 */
	public Map<Integer, String> getModelTagsMap() {
		return modelTagsMap;
	}

	/**
	 * @param modelTagsMap the modelTagsMap to set
	 */
	public void setModelTagsMap(Map<Integer, String> modelTagsMap) {
		this.modelTagsMap = modelTagsMap;
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("TagCreatorBolt_input_count");
		//String lyl_id_no = null; 

		countMetric.scope("entering_TagCreator_bolt").incr();			
			
		if(input != null)
		{
			
			
			try{
				JsonElement jsonElement = TupleParser.getParsedJson(input);
				LOGGER.info("Input from TagCreatorBolt :" + jsonElement.toString());
				JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("memberId");
				
				
				/*
				 * topologyName is not needed as of now, but is there in the json emitted and can be used whenever needed
				 */
			//	String topology = jsonElement.getAsJsonObject().get("topology").getAsString();
				
				BigInteger loyaltyID =  new BigInteger(lyl_id_no.getAsString());
				//if (! (loyaltyID.compareTo(startLoyalty) != -1  && loyaltyID.compareTo(lastLoyalty) != 1) ){
				
				if (loyaltyID.compareTo(startLoyalty) == -1  || loyaltyID.compareTo(lastLoyalty) == 1) {
					LOGGER.info("Not creating Tag as lid is out of the percentile range alloted");
					redisCountIncr("OutOf_PO_CPS_PercSplit");	
					outputCollector.ack(input);
					return;
				}
				
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
				
				if(lyl_id_no!=null){
					
					TypeToken<List<ModelScore>> token = new TypeToken<List<ModelScore>>(){};
					List<ModelScore> modelScoreList = new Gson().fromJson(jsonElement.getAsJsonObject().get("scoresInfo"), token.getType());
					
					//List<ModelScore> modelScoreList = (List<ModelScore>) jsonElement.getAsJsonObject().get("scoresInfo");
					process(lyl_id_no, l_id, modelScoreList);
				}
			} catch (Exception e){
				LOGGER.error("PERSIST:Exception Occured in TagCreatorBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
				//outputCollector.fail(input);	
			}
				
		} else {
			redisCountIncr("null_lid");			
			//outputCollector.fail(input);				
		}
		outputCollector.ack(input);
		
	}

	private void process(JsonElement lyl_id_no, String l_id,
			List<ModelScore> modelScoreList) {
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
				LOGGER.info("PERSIST:Tags being sent for loyalty Id : " +lyl_id_no.getAsString()+ " > 95% : " +rtsTags.toString());
				if(blackListed){
					LOGGER.info("PERSIST:Blackedout loyalty Id being sent to CP Processing : " +lyl_id_no.getAsString());
				}
				this.outputCollector.emit("rtsTags_stream",rtsTagsListToEmit);	
			}
			else if(rtsTags.size()==0 && blackListed){
				List<Object> blackedoutListToEmit = new ArrayList<Object>();
				blackedoutListToEmit.add(lyl_id_no.getAsString());
				LOGGER.info("PERSIST:Blackedout loyalty Id being sent to CP Processing : " +lyl_id_no.getAsString());
				this.outputCollector.emit("blackedout_stream",blackedoutListToEmit);
			}
		}
	}
	
	public String createTag(ModelScore modelScore, String l_id , int priority) {
		String tag = modelTagsMap.get(new Integer (modelScore.getModelId()));
		
		return tag+priority;
		
		/*String mdTag = null;
		//Check if there is an MDTag already in the collection.
		if (tag != null) {
			//tag+=priority;
			mdTag = getMdTagIfExists(tag,l_id);
		}
		
		if(tag != null && mdTag == null)
			mdTag = tag + priority;
		
		return mdTag;*/
	}
	
	private String getMdTagIfExists(String tag, String l_id){
		
		ArrayList<String> tagList = (ArrayList<String>) memberMDTags2Dao.getMemberMDTags(l_id);
		
		if(tagList!= null && tagList.size()>0)
			for(String tagFromLst : tagList){
				if(tagFromLst.substring(0, 5).equalsIgnoreCase(tag)){
					return tagFromLst; 
				}
			}
		return null;
	}

	/**
	 * @return the memberMDTags2Dao
	 */
	public MemberMDTags2Dao getMemberMDTags2Dao() {
		return memberMDTags2Dao;
	}

	/**
	 * @param memberMDTags2Dao the memberMDTags2Dao to set
	 */
	public void setMemberMDTags2Dao(MemberMDTags2Dao memberMDTags2Dao) {
		this.memberMDTags2Dao = memberMDTags2Dao;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("rtsTags_stream",new Fields( "rtsTags"));
		declarer.declareStream("blackedout_stream", new Fields("lyl_id_no"));

	}
}