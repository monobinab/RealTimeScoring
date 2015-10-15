package analytics.bolt;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.ResponsysPayload;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public abstract class ResponsysBolt  extends EnvironmentBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponsysBolt.class);
	
	private OutputCollector outputCollector;
	protected ResponsysUtil responsysUtil;
	private TagMetadataDao tagMetadataDao;
	private MemberInfoDao memberInfoDao;
	private String topologyName;
	protected Map<Integer, String> tagModelsMap;
	private CpsOccasionsDao cpsOccasion;
	private HashMap<String, String> cpsOccasionPriorityMap;
	private HashMap<String, String> cpsOccasionDurationMap;
	
	public ResponsysBolt(String systemProperty) {
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
		cpsOccasion = new CpsOccasionsDao();
		cpsOccasionPriorityMap = cpsOccasion.getcpsOccasionPriority();
		cpsOccasionDurationMap = cpsOccasion.getcpsOccasionDurations();
	 }

	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		String lyl_id_no = null;
		String value = null;
		ResponsysPayload responsysObj = new ResponsysPayload();
		try {
			
			lyl_id_no = input.getString(0);
			value = input.getString(1);
			
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
			
			//responsys need eid in its xml, so checked for its nullness before rtsapi call
			MemberInfo memberInfo = memberInfoDao.getMemberInfo(l_id);
		    String eid = (memberInfo !=null ? memberInfo.getEid() : null);
		    if(eid == null){
		    	LOGGER.info("PERSIST: No Eid found for loyalty id : "+lyl_id_no);
		    	outputCollector.ack(input);
				return;
		    }
			

		    String successFlag = process(lyl_id_no,responsysObj,l_id,memberInfo,value, topologyName);
		    
		    //Success Flags would be either null or F or P indicating as follows
		    //null - no action needed, continue further processing
		    //F - Failure occurred, fail the message
		    //P - Passed, but there is nothing to processing further, so just ack and return
		    if(successFlag!= null && successFlag.equalsIgnoreCase("F")){
		    	LOGGER.info("Failing the tuple for Loyalty id : "+lyl_id_no + " from Topology : " + topologyName);
		    	outputCollector.fail(input);
		    	return;
		    }else if(successFlag!= null && successFlag.equalsIgnoreCase("P")){
		    	LOGGER.info("No further processing needed for Loyalty id : "+lyl_id_no + " from Topology : " + topologyName);
		    	outputCollector.ack(input);
		    	return;
		    }

			responsysUtil.getResponsysServiceResult(responsysObj);
			
			addRtsMemberTag(l_id, responsysObj.getTagMetadata().getMdTag(),cpsOccasionDurationMap, cpsOccasionPriorityMap);
			
		    redisCountIncr("data_to_responsys");
		
			outputCollector.ack(input);
	} catch (Exception e) {
			LOGGER.error("Exception in Responsys Bolt for lid " + lyl_id_no, e);
		}
	}

	
	
	protected abstract String process(String lyl_id_no, ResponsysPayload responsysObj, String l_id, MemberInfo memberInfo, String value, String topologyName ) ;
	
	protected abstract void addRtsMemberTag(String l_id, String rtsTag, HashMap<String, String> cpsOccasionDurationMap, 
			HashMap<String, String> cpsOccasionPriorityMap) throws ParseException;
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
