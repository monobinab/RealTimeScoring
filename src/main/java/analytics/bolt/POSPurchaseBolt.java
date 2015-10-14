package analytics.bolt;

import java.text.ParseException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.objects.MemberInfo;
import analytics.util.objects.ResponsysPayload;
import analytics.util.objects.TagMetadata;
import backtype.storm.topology.OutputFieldsDeclarer;

public class POSPurchaseBolt  extends ResponsysBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(POSPurchaseBolt.class);
	

	public POSPurchaseBolt(String systemProperty) {
		super(systemProperty);
	}

	
	@Override
	public String process (String lyl_id_no, ResponsysPayload responsysObj, String l_id, MemberInfo memberInfo, String value,String topologyName ){

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
				//tagMetadata.setDivLine(tagMetadata.getDivLine()!=null ? tagMetadata.getDivLine()+"," +divLineArr[i]: divLineArr[i]);
				responsysUtil.getTagMetadata(tagMetadata,divLineArr[i]);
				if (tagMetadata.getDivLine() == null || tagMetadata.getDivLine().equals("")||tagMetadata.getDivLine().isEmpty())
					return "P";
			}
			
			setResponsysObj(lyl_id_no, responsysObj, l_id, memberInfo, o, tagMetadata, value, custEventNm, topologyName);
	    }catch(Exception e){
	    	LOGGER.error(" Error Occured processing Unknown Responsys for member : " + lyl_id_no);
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
	
	public void addRtsMemberTag(String l_id, String rtsTag, HashMap<String, String> cpsOccasionDurationMap, 
			HashMap<String, String> cpsOccasionPriorityMap) throws ParseException{}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
