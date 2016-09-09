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
import analytics.util.SywApiCalls;
import analytics.util.TupleParser;
import analytics.util.dao.CatgSubcatgModelDAO;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.ModelsDao;
import analytics.util.objects.Model;
import analytics.util.objects.ModelScore;
import analytics.util.objects.Sweep;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonElement;

public class SweepsTagCreatorBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(TagCreatorBolt.class);
	private OutputCollector outputCollector;
	//TagVariableDao tagVariableDao;
	MemberMDTags2Dao memberMDTags2Dao;
	Map<Integer, String> modelTagsMap = new HashMap<Integer, String>();
	ModelsDao modelsDao;
	Map<Integer, Model> modelsMap = new HashMap<Integer, Model>();
	CatgSubcatgModelDAO catgSubcatgModelDAO;
	List<Sweep> catSubCatData = new ArrayList<Sweep>();
	SywApiCalls sywApiCalls;
	
	public SweepsTagCreatorBolt(String env) {
		super(env);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;		
		//tagVariableDao = new TagVariableDao();
		
		memberMDTags2Dao = new MemberMDTags2Dao();
		modelsDao = new ModelsDao();
		modelsMap = modelsDao.getModelNames();
		modelTagsMap = modelsDao.getModelTags();
		catgSubcatgModelDAO = new CatgSubcatgModelDAO();
		catSubCatData = catgSubcatgModelDAO.getCatSubCat();
		sywApiCalls = new SywApiCalls(); //Used for sweeps
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
		redisCountIncr("SweepsTagCreatorBolt_input_count");
		countMetric.scope("entering_SweepsTagCreator_bolt").incr();			
		if(input != null){
			try{
				JsonElement jsonElement = TupleParser.getParsedJson(input);
				LOGGER.info("Input to TagCreatorBolt :" + jsonElement.toString());
				if(jsonElement != null){
					Sweep sweep = this.getSweepsInfo(jsonElement);
					if(sweep != null && StringUtils.isNotEmpty(sweep.getMemberId())){
						process(sweep.getMemberId(), sweep.getL_id(), sweep);
					}
				}
			} catch (Exception e){
				LOGGER.error("PERSIST:Exception Occured in SweepsTagCreatorBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
			}
		} else {
			redisCountIncr("null_lid");			
		}
		outputCollector.ack(input);
	}

	@SuppressWarnings("unchecked")
	private void process(String lyl_id_no, String l_id, Sweep sweep) {
		List<Object> rtsTagsListToEmit = new ArrayList<Object>();
		List<String> rtsTags = new ArrayList<String>();
		JSONObject mainJsonObj = new JSONObject();
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
		}
		if(rtsTags.size()>0){
			mainJsonObj.put("buSubBu", rtsTags);
			mainJsonObj.put("memberId", lyl_id_no);
			rtsTagsListToEmit.add(mainJsonObj.toString());
			LOGGER.info("PERSIST: Sweeps Tags being sent for loyalty Id : " + lyl_id_no + " Sweeps Priority - Z : " + rtsTags.toString());
			this.outputCollector.emit("kafka_stream",rtsTagsListToEmit);	
		}
	}
	
	public String createTag(ModelScore modelScore, String l_id , String priority) {
		Model model = modelsMap.get(Integer.parseInt(modelScore.getModelId()));
		if(model != null && StringUtils.isNotBlank(model.getModelCode())){
			return model.getModelCode() + priority;
		}
		return null;
	}

	private Sweep getSweepsInfo(JsonElement jsonElement){
		Sweep sweep = null;
		if(jsonElement.getAsJsonObject().get("UserId") != null){
			String userId = jsonElement.getAsJsonObject().get("UserId").getAsString();
			String memberId = sywApiCalls.getLoyaltyId(userId);
			System.out.println("UserId : " +  userId + " --> MemberId : " + memberId);
			JsonElement categoryElement = jsonElement.getAsJsonObject().get("Category");
			if(StringUtils.isNotEmpty(memberId) && categoryElement != null){
				String l_id = SecurityUtils.hashLoyaltyId(memberId);
				if(categoryElement != null && StringUtils.isNotEmpty(categoryElement.getAsString())){
					JsonElement subCategoryElement = jsonElement.getAsJsonObject().get("SubCategory");
					String category = categoryElement.getAsString();
					String subCategory = StringUtils.EMPTY;
					if(subCategoryElement != null && StringUtils.isNotEmpty(subCategoryElement.getAsString())){
						subCategory = subCategoryElement.getAsString();
					}
					if(catSubCatData != null && catSubCatData.size() > 0){
						for(Sweep catSubCat : catSubCatData){
							if(StringUtils.isNotEmpty(category)){
								if(StringUtils.isNotEmpty(subCategory)){
									if(catSubCat.getCategory().equalsIgnoreCase(category) 
											&& catSubCat.getSubCategory().equalsIgnoreCase(subCategory)){
										sweep = new Sweep();
										sweep.setMemberId(memberId);
										sweep.setL_id(l_id);
										sweep.setCategory(category);
										sweep.setSubCategory(subCategory);
										sweep.setModelId(catSubCat.getModelId());
										sweep.setPriority(Constants.SWEEPSPRIORITY);
									}
								}else if(catSubCat.getCategory().equalsIgnoreCase(category)){
									sweep = new Sweep();
									sweep.setMemberId(memberId);
									sweep.setL_id(l_id);
									sweep.setCategory(category);
									sweep.setSubCategory(subCategory);
									sweep.setModelId(catSubCat.getModelId());
									sweep.setPriority(Constants.SWEEPSPRIORITY);
								}
							}
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
		declarer.declareStream("kafka_stream", new Fields("message"));
	}
}