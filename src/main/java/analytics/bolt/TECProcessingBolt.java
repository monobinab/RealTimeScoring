package analytics.bolt;

import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.RTSAPICaller;
import analytics.util.SecurityUtils;
import analytics.util.ResponsysUtil;
import analytics.util.TECPostClient;
import analytics.util.dao.ClientApiKeysDAO;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TECProcessingBolt extends EnvironmentBolt {
	
	private static final long serialVersionUID = 1L;
	private static final String api_Key_Param="RTS_TEC";
	private String api_key;
	private static final Logger LOGGER = LoggerFactory.getLogger(TECProcessingBolt.class);
	private OutputCollector outputCollector;
	private RTSAPICaller rtsApiCaller;
	private TECPostClient tecPostClient;

	public TECProcessingBolt(String env) {
		super(env);
	}
	
	public void redisCountIncr(String scope){
		countMetric.scope(scope).incr();
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		LOGGER.info("Preparing TECProcessingBolt");
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		try {
			rtsApiCaller = RTSAPICaller.getInstance();
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage() +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
		}
		tecPostClient = TECPostClient.getInstance();
		api_key=new ClientApiKeysDAO().findkey(api_Key_Param);
		
	}

	@Override
	public void execute(Tuple input) {
		
		redisCountIncr("incoming_tuples");
		String message  = (String) input.getValueByField("str");
		String l_id = message.substring(0,16);		
		LOGGER.info("tec input contains message : " + l_id );
			
			if (l_id != null && !l_id.isEmpty()) {	
				try{
					//call rts api and get response for this l_id 
					//16 - level, rtsTOtec is the apikey for internal calls to RTS from topologies
					String scoreInfoSearsJsonString = rtsApiCaller.getRTSAPIResponse(l_id, "16", api_key, "sears", Boolean.FALSE, "");
					String scoreInfoKmartJsonString = rtsApiCaller.getRTSAPIResponse(l_id, "16", api_key, "kmart", Boolean.FALSE, "");
					//send the response for both sears and kmart format to TEC end point.
					LOGGER.info("sears message sent to TEC for memeber - " + l_id + " is -- " + scoreInfoSearsJsonString);
					TECPostClient.postToTEC(scoreInfoSearsJsonString, l_id);
					LOGGER.info("kmart message sent to TEC for memeber - " + l_id + " is -- " + scoreInfoKmartJsonString);
					TECPostClient.postToTEC(scoreInfoKmartJsonString, l_id);	
					redisCountIncr("sent_to_tec_process");						
				} catch (Exception e){
					LOGGER.error("Exception Occured in TECProcessingBolt :: ", e);
					outputCollector.fail(input);					
				}				
				
			} else {
				redisCountIncr("null_lid");			
				outputCollector.fail(input);				
			}
		
		
		if (input.contains("topology_id")) 	{	
			String topology_id = input.getStringByField("topology_id");
			//what to do with this toplogy id
			
		}
		
		outputCollector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {		

	}	

}
