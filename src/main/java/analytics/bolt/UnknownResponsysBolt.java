package analytics.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
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

public class UnknownResponsysBolt  extends ResponsysBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(UnknownResponsysBolt.class);
	
	
	public UnknownResponsysBolt(String systemProperty) {
		super(systemProperty);
	}

	
	public String process (String lyl_id_no, ResponsysPayload responsysObj, String l_id, MemberInfo memberInfo, String value, String topologyName ){
		
		org.json.JSONObject o = null;
		org.json.JSONObject objToSend = null;
		TagMetadata tagMetadata = null;
		String custEventNm = null;   
		String retString = null;
		
	    try{
			String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no, topologyName);
			
			if(StringUtils.isEmpty(scoreInfoJsonString) ||  !scoreInfoJsonString.startsWith("{")){
				LOGGER.error("Exception occured in api " + scoreInfoJsonString);
				return "F";
			}
	
			//get the top jsonObject from api which are in ready tags and satisfying >95 percentile
			o = new org.json.JSONObject(scoreInfoJsonString);
			objToSend =  getJsonForResponsys(tagModelsMap, o);
			if(objToSend == null){
				redisCountIncr("no_data_to_responsys");
				LOGGER.info("PERSIST: No Data to send for loyalty id : "+lyl_id_no);
				return "P";
			}
			//preparing the jsonObject with only first model, which satisfied the above conditions
			o.remove("scoresInfo");
			o.append("scoresInfo", objToSend);
			
			tagMetadata = responsysUtil.getTagMetadata(objToSend);
			custEventNm = "RTS_Unknown";
		
			setResponsysObj(lyl_id_no, responsysObj, l_id, memberInfo, o, tagMetadata, value, custEventNm, topologyName);
	    }catch(Exception e){
	    	LOGGER.error(" Error Occured processing RTS purchase for member : " + lyl_id_no);
	    	e.printStackTrace();
	    }
	
			return retString;
	}
	
	

	private void setResponsysObj(String lyl_id_no,
			ResponsysPayload responsysObj, String l_id, MemberInfo memberInfo,
			org.json.JSONObject o, TagMetadata tagMetadata, 
			String value, String custEventNm, String topologyName) {
		//set the responsys object
		responsysObj.setLyl_id_no(lyl_id_no);
		responsysObj.setL_id(l_id);
		responsysObj.setJsonObj(o);
		responsysObj.setTagMetadata(tagMetadata);
		responsysObj.setMemberInfo(memberInfo);
		responsysObj.setCustomEventName(custEventNm);
		responsysObj.setTopologyName(topologyName);
		responsysObj.setValue(value);
	}

	
	public void addRtsMemberTag(String l_id, String rtsTag){
		ArrayList<String> rtsTagLst = new ArrayList<String>();
		rtsTagLst.add(rtsTag);
		responsysUtil.getMemberMDTags2Dao().addRtsMemberTags(l_id, rtsTagLst);
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
