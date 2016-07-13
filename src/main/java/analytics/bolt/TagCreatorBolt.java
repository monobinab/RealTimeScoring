package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.SecurityUtils;
import analytics.util.TupleParser;
import analytics.util.dao.CatgSubcatgModelDAO;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.ModelsDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.Model;
import analytics.util.objects.ModelScore;
import analytics.util.objects.Sweep;
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
	ModelsDao modelsDao;
	Map<Integer, Model> modelsMap = new HashMap<Integer, Model>();
	CatgSubcatgModelDAO catgSubcatgModelDAO;
	List<Sweep> catSubCatData = new ArrayList<Sweep>();
	/*private static BigInteger startLoyalty = new BigInteger("7081010000647509"); 
	private static BigInteger lastLoyalty = new BigInteger("7081216198457607");*/

	public TagCreatorBolt(String env) {
		super(env);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;		
		tagVariableDao = new TagVariableDao();
		modelTagsMap = tagVariableDao.getModelTags();
		memberMDTags2Dao = new MemberMDTags2Dao();
		modelsDao = new ModelsDao();
		modelsMap = modelsDao.getModelNames();
		catgSubcatgModelDAO = new CatgSubcatgModelDAO();
		catSubCatData = catgSubcatgModelDAO.getCatSubCat();
	}

	/**
	 * @return the modelsMap
	 */
	public Map<Integer, Model> getModelsMap() {
		return modelsMap;
	}

	/**
	 * @param modelsMap the modelsMap to set
	 */
	public void setModelsMap(Map<Integer, Model> modelsMap) {
		this.modelsMap = modelsMap;
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("TagCreatorBolt_input_count");
		countMetric.scope("entering_TagCreator_bolt").incr();			
			
		if(input != null){
			try{
				JsonElement jsonElement = TupleParser.getParsedJson(input);
				LOGGER.info("Input to TagCreatorBolt :" + jsonElement.toString());
				if(jsonElement != null){
					Sweep sweep = this.getSweepsInfo(jsonElement);
					if(sweep != null && StringUtils.isNotEmpty(sweep.getMemberId())){
						process(sweep.getJsonMemberId(), sweep.getMemberId(), null, sweep);
					}else{
						JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("memberId");
						if(lyl_id_no != null){
							TypeToken<List<ModelScore>> token = new TypeToken<List<ModelScore>>(){};
							List<ModelScore> modelScoreList = new Gson().fromJson(jsonElement.getAsJsonObject().get("scoresInfo"), token.getType());
							String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
							process(lyl_id_no, l_id, modelScoreList, null);
						}
					}
				}
			} catch (Exception e){
				LOGGER.error("PERSIST:Exception Occured in TagCreatorBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
			}
		} else {
			redisCountIncr("null_lid");			
		}
		outputCollector.ack(input);
	}

	@SuppressWarnings("unchecked")
	private void process(JsonElement lyl_id_no, String l_id, List<ModelScore> modelScoreList, Sweep sweep) {
		List<Object> rtsTagsListToEmit = new ArrayList<Object>();
		List<String> rtsTags = new ArrayList<String>();
		JSONObject mainJsonObj = new JSONObject();
		boolean blackListed = false;
		if(sweep != null){
			List<String> modelIds = sweep.getModelId();
			if(modelIds != null && modelIds.size() > 0){
				for(String modelId : modelIds){
					ModelScore modelScore = new ModelScore();
					modelScore.setModelId(modelId);
					String rtsTag = createTag(modelScore, sweep.getMemberId(), sweep.getPriority());
					if(StringUtils.isNotEmpty(rtsTag)){
						rtsTags.add(rtsTag);
					}
				}
			}
		}else{
			if(modelScoreList != null && !modelScoreList.isEmpty()){
				for(ModelScore modelScore :  modelScoreList){	
					if(modelScore.getScore()==0.0)
						blackListed = true;
					if(modelScore.getPercentile() > 95){
						String rtsTag = createTag(modelScore,l_id,Constants.TOP5PRIORITY);
						if(StringUtils.isNotEmpty(rtsTag)){
							rtsTags.add(rtsTag);
						}
					}
				}
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
	
	public String createTag(ModelScore modelScore, String l_id , String priority) {
		Model model = modelsMap.get(Integer.parseInt(modelScore.getModelId()));
		if(model != null && StringUtils.isNotBlank(model.getModelCode())){
			return model.getModelCode() + priority;
		}
		return null;
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

	private Sweep getSweepsInfo(JsonElement jsonElement){
		Sweep sweep = null;
		JsonElement memIdElement = jsonElement.getAsJsonObject().get("memberId");
		JsonElement prodIdElement = jsonElement.getAsJsonObject().get("productId");
		if(prodIdElement != null && memIdElement != null && StringUtils.isNotEmpty(memIdElement.getAsString())){
			JsonElement categoryElement = jsonElement.getAsJsonObject().get("category");
			JsonElement subCategoryElement = jsonElement.getAsJsonObject().get("subCategory");
			if(categoryElement != null && StringUtils.isNotEmpty(categoryElement.getAsString()) 
					&& subCategoryElement != null && StringUtils.isNotEmpty(subCategoryElement.getAsString())){
				String memberId = memIdElement.getAsString();
				String category = categoryElement.getAsString();
				String subCategory = subCategoryElement.getAsString();
				if(catSubCatData != null && catSubCatData.size() > 0){
					for(Sweep catSubCat : catSubCatData){
						if(catSubCat.getCategory().equalsIgnoreCase(category) 
								&& catSubCat.getSubCategory().equalsIgnoreCase(subCategory)){
							sweep = new Sweep();
							sweep.setMemberId(memberId);
							sweep.setJsonMemberId(memIdElement);
							sweep.setCategory(category);
							sweep.setSubCategory(subCategory);
							sweep.setModelId(catSubCat.getModelId());
							sweep.setPriority(Constants.SWEEPSPRIORITY);
						}
					}
				}
			}
		}
		return sweep;
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