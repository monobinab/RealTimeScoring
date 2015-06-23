package analytics.bolt;

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

public class POSPurchaseBolt  extends ResponsysBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(POSPurchaseBolt.class);
	

	public POSPurchaseBolt(String systemProperty) {
		super(systemProperty);
	}

	
	@Override
	public String process (String lyl_id_no, ResponsysPayload responsysObj, String l_id, String eid, String value,String topologyName ){

		TagMetadata tagMetadata = null;
		String custEventNm = null;   
		String retString = null;
		org.json.JSONObject o = null;
		
	    try{
			
	    	String[] divLineArr = value.split("~");
			tagMetadata = new TagMetadata();
			tagMetadata.setPurchaseOccassion("Purchase");
			custEventNm = "RTS_Purchase";
			
			for(int i=0; i <divLineArr.length ; i++){
				tagMetadata.setDivLine(tagMetadata.getDivLine()!=null ? tagMetadata.getDivLine()+"," +divLineArr[i]: divLineArr[i]);

				responsysUtil.getTagMetadata(tagMetadata,divLineArr[i]);
			}
			
			setResponsysObj(lyl_id_no, responsysObj, l_id, eid, o, tagMetadata, value, custEventNm, topologyName);
	    }catch(Exception e){
	    	LOGGER.error(" Error Occured processing Unknown Responsys for member : " + lyl_id_no);
	    }
	    	
			return retString;
	}
	
	

	private void setResponsysObj(String lyl_id_no,
			ResponsysPayload responsysObj, String l_id, String eid,
			org.json.JSONObject o, TagMetadata tagMetadata, 
			String value, String custEventNm, String topologyName) {
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
