package analytics.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagResponsysActiveDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.Responsys;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.json.JSONException;

public class ResponsysUnknownCallsBolt  extends EnvironmentBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponsysUnknownCallsBolt.class);
	
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	private ResponsysUtil responsysUtil;
	private TagResponsysActiveDao tagResponsysActiveDao;
	private TagVariableDao tagVariableDao;
	private TagMetadataDao tagMetadataDao;
	private MemberInfoDao memberInfoDao;
	private String topologyName;
	
	public ResponsysUnknownCallsBolt(String systemProperty) {
		super(systemProperty);
	}
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		responsysUtil = new ResponsysUtil();
		tagResponsysActiveDao =  new TagResponsysActiveDao();
		tagVariableDao = new TagVariableDao();
		tagMetadataDao = new TagMetadataDao();
		memberInfoDao = new MemberInfoDao();
		topologyName = (String) stormConf.get("metrics_topology");
		initMetrics(context);
  }

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		String lyl_id_no = null; 
		Responsys responsysObj = new Responsys();
		try {
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
					
				//get the list of models
				List<String> activeTags = tagResponsysActiveDao.tagsResponsysList();
				Map<Integer, String> tagModelsMap = tagVariableDao.getTagModelIds(activeTags);
				
				//get the top jsonObject from api satisfying the condition
				org.json.JSONObject o = new org.json.JSONObject(scoreInfoJsonString);
				org.json.JSONObject objToSend = null;
				objToSend = getJsonForResponsys(tagModelsMap, o, objToSend);
				if(objToSend == null){
					countMetric.scope("no_data_to_responsys").incr();
					return;
				}
				
				//get tagMetadata information
				TagMetadata tagMetadata = null;
				String tag = tagModelsMap.get(Integer.parseInt((String) objToSend.get("modelId")));
				tagMetadata = tagMetadataDao.getBuSubBu(tag);
				if(!objToSend.has("occassion") ){
					tagMetadata = new TagMetadata();
					tagMetadata.setMdTags(tag+"0000000000000");
				}
				
				//preparing the jsonObject with only first model, which satisfied the above conditions
				o.remove("scoresInfo");
				o.append("scoresInfo", objToSend);
				String eid = memberInfoDao.getMemberInfoEId(l_id);
				
				//set the responsys object
				responsysObj.setLyl_id_no(lyl_id_no);
				responsysObj.setL_id(l_id);
				responsysObj.setJsonObj(o);
				responsysObj.setTagMetadata(tagMetadata);
				responsysObj.setEid(eid);
				responsysObj.setCustomEventName("RTS_Unknown");
				responsysObj.setTopologyName(topologyName);
				
				responsysUtil.getResponseUnknownServiceResult(responsysObj);
			    countMetric.scope("responses").incr();
		}
			outputCollector.ack(input);
	} catch (Exception e) {
			LOGGER.error("Exception in ResponsysUnknownCallsBolt", e);
			countMetric.scope("responses_failed").incr();
		}
	}
	private org.json.JSONObject getJsonForResponsys(
			Map<Integer, String> tagModelsMap, org.json.JSONObject o,
			org.json.JSONObject objToSend) throws JSONException {
		org.json.JSONArray arr = o.getJSONArray("scoresInfo");
		for(int i=0; i<arr.length(); i++){
			String modelId = ((org.json.JSONObject)arr.get(i)).getString("modelId");
			Double percentile = Double.valueOf(((org.json.JSONObject)arr.get(i)).getString("percentile"));
			for(Map.Entry<Integer, String> entry : tagModelsMap.entrySet()){
				if((entry.getKey() +"").equals(modelId) && percentile >= 95){
					if(((org.json.JSONObject) arr.get(i)).has("occassion") ){
						if(((org.json.JSONObject) arr.get(i)).getString("occassion").equalsIgnoreCase("Unknown")){
						objToSend = (org.json.JSONObject)arr.get(i);
						return objToSend;
						}
					}
					else{
						objToSend = (org.json.JSONObject)arr.get(i);
						return objToSend;
					}
				}
			}
		}
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
