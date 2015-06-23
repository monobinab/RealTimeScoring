package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
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
		this.outputCollector = collector;
		responsysUtil = new ResponsysUtil();
		topologyName = (String) stormConf.get("metrics_topology");
		LOGGER.info("PREPARING "+topologyName+ " BOLT");
		tagModelsMap = responsysUtil.getTagModelsMap();
		tagMetadataDao = responsysUtil.getTagMetadataDao();
		memberInfoDao = responsysUtil.getMemberInfoDao();
	 }

	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		String lyl_id_no = null;
		String value = null;
		ResponsysPayload responsysObj = new ResponsysPayload();
		try {
				
			org.json.JSONObject o = null;
			org.json.JSONObject objToSend = null;
			TagMetadata tagMetadata = null;
			String custEventNm = null;
			
			lyl_id_no = input.getString(0);
			value = input.getString(1);
			
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
			
			//responsys need eid in its xml, so checked for its nullness before rtsapi call
			MemberInfo memberInfo = memberInfoDao.getMemberInfo(l_id);
		    String eid = memberInfo.getEid();
		    if(eid == null){
		    	LOGGER.info("PERSIST: No Eid found for loyalty id : "+lyl_id_no);
		    	outputCollector.ack(input);
				return;
		    }
			
			if(topologyName.equalsIgnoreCase(Constants.UNKNOWN_OCCASION)){
		    
		    	String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
				
				if(StringUtils.isEmpty(scoreInfoJsonString) ||  !scoreInfoJsonString.startsWith("{")){
					LOGGER.error("Exception occured in api " + scoreInfoJsonString);
					outputCollector.fail(input);
					return;
				}
		
				//get the top jsonObject from api which are in ready tags and satisfying >95 percentile
				o = new org.json.JSONObject(scoreInfoJsonString);
				objToSend =  getJsonForResponsys(tagModelsMap, o);
				if(objToSend == null){
					redisCountIncr("no_data_to_responsys");
					LOGGER.info("PERSIST: No Data to send for loyalty id : "+lyl_id_no);
					outputCollector.ack(input);
					return;
				}
				//preparing the jsonObject with only first model, which satisfied the above conditions
				o.remove("scoresInfo");
				o.append("scoresInfo", objToSend);
				
				tagMetadata = responsysUtil.getTagMetadata(objToSend);
				custEventNm = "RTS_Unknown";
			}
			else if(topologyName.equalsIgnoreCase(Constants.POS_PURCHASE)){
				
				String[] divLine = value.split("~");
				tagMetadata = responsysUtil.getTagMetadata(divLine[0],divLine[1]);
				custEventNm = "RTS_Purchase";
			}
			
			setResponsysObj(lyl_id_no, responsysObj, l_id, eid, o, tagMetadata, value, custEventNm);
			
			responsysUtil.getResponsysServiceResult(responsysObj);
		    redisCountIncr("data_to_responsys");
		
			outputCollector.ack(input);
	} catch (Exception e) {
			LOGGER.error("Exception in ResponsysUnknownCallsBolt for lid " + lyl_id_no, e);
		}
	}

	private void setResponsysObj(String lyl_id_no,
			ResponsysPayload responsysObj, String l_id, String eid,
			org.json.JSONObject o, TagMetadata tagMetadata, 
			String value, String custEventNm) {
		//set the responsys object
		responsysObj.setLyl_id_no(lyl_id_no);
		responsysObj.setL_id(l_id);
		responsysObj.setJsonObj(o);
		responsysObj.setTagMetadata(tagMetadata);
		responsysObj.setEid(eid);
		responsysObj.setCustomEventName(custEventNm);
		responsysObj.setTopologyName(topologyName);
		responsysObj.setValue(value);
	}

	
		

	private org.json.JSONObject getJsonForResponsys(
			Map<Integer, String> tagModelsMap, org.json.JSONObject o
			) throws JSONException {
		org.json.JSONArray arr = null;
		if(o.has("scoresInfo"))
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
					if((String.valueOf(entry.getKey())).equals(modelId) && percentile > 95){
						return (org.json.JSONObject)arr.get(i);
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
