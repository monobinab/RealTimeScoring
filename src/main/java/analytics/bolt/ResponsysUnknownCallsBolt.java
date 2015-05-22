package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.ResponsysPayload;
import analytics.util.objects.TagMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;

public class ResponsysUnknownCallsBolt  extends EnvironmentBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponsysUnknownCallsBolt.class);
	
	private OutputCollector outputCollector;
	private ResponsysUtil responsysUtil;
	private TagMetadataDao tagMetadataDao;
	private MemberInfoDao memberInfoDao;
	private String topologyName;
	private Map<Integer, String> tagModelsMap;
	
	public ResponsysUnknownCallsBolt(String systemProperty) {
		super(systemProperty);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		LOGGER.info("PREPARING RESPONSYSUNKNOWNCALLS BOLT");	
		this.outputCollector = collector;
		responsysUtil = new ResponsysUtil();
		topologyName = (String) stormConf.get("metrics_topology");
		tagModelsMap = responsysUtil.getTagModelsMap();
		tagMetadataDao = responsysUtil.getTagMetadataDao();
		memberInfoDao = responsysUtil.getMemberInfoDao();
	 }

	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		String lyl_id_no = null; 
		ResponsysPayload responsysObj = new ResponsysPayload();
		try {
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
				
				if(StringUtils.isEmpty(scoreInfoJsonString) || !scoreInfoJsonString.startsWith("{")){
					LOGGER.error("empty string from api");
					outputCollector.ack(input);
					return;
				}
		
				//get the top jsonObject from api satisfying the condition
				org.json.JSONObject o = new org.json.JSONObject(scoreInfoJsonString);
				org.json.JSONObject objToSend = null;
				objToSend = getJsonForResponsys(tagModelsMap, o, objToSend);
				if(objToSend == null){
					redisCountIncr("no_data_to_responsys");
					outputCollector.ack(input);
					return;
				}
				
				//get tagMetadata information and set the mdTag with zeros
				TagMetadata tagMetadata = null;
				String tag = tagModelsMap.get(Integer.parseInt((String) objToSend.get("modelId")));
				tagMetadata = tagMetadataDao.getBuSubBu(tag);
				tagMetadata.setMdTags(tag+"8000000000000");
				
				
				//preparing the jsonObject with only first model, which satisfied the above conditions
				o.remove("scoresInfo");
				o.append("scoresInfo", objToSend);
				MemberInfo memberInfo = memberInfoDao.getMemberInfo(l_id);
			    String eid = memberInfo.getEid();
			 
				//set the responsys object
				responsysObj.setLyl_id_no(lyl_id_no);
				responsysObj.setL_id(l_id);
				responsysObj.setJsonObj(o);
				responsysObj.setTagMetadata(tagMetadata);
				responsysObj.setEid(eid);
				responsysObj.setCustomEventName("RTS_Unknown");
				responsysObj.setTopologyName(topologyName);
				
				responsysUtil.getResponseUnknownServiceResult(responsysObj);
			    redisCountIncr("data_to_responsys");
				 
			}
			else{
				redisCountIncr("no_lyl_id_no");
			}
			outputCollector.ack(input);
	} catch (Exception e) {
			LOGGER.error("Exception in ResponsysUnknownCallsBolt", e);
		}
	}
		

	private org.json.JSONObject getJsonForResponsys(
			Map<Integer, String> tagModelsMap, org.json.JSONObject o,
			org.json.JSONObject objToSend) throws JSONException {
		org.json.JSONArray arr = null;
		if(o.has("org.json.JSONArray arr"))
			arr = o.getJSONArray("scoresInfo");
		else
			return null;
		
		if(((org.json.JSONObject) arr.get(0)).has("mdTag"))
			return null;
		
		for(int i=0; i<arr.length(); i++){
			if(!((org.json.JSONObject) arr.get(i)).has("mdTag") ){
				String modelId = ((org.json.JSONObject)arr.get(i)).getString("modelId");
				Double percentile = Double.valueOf(((org.json.JSONObject)arr.get(i)).getString("percentile"));
				for(Map.Entry<Integer, String> entry : tagModelsMap.entrySet()){
					if((entry.getKey() +"").equals(modelId) && percentile >= 95){
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
