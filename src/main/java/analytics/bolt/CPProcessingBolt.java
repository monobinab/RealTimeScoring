package analytics.bolt;


//import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

//import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

//import scala.math.BigInt;
//import analytics.exception.RealTimeScoringException;
import analytics.util.CPSFiler;
//import analytics.util.MongoNameConstants;
import analytics.util.RTSAPICaller;
import analytics.util.SecurityUtils;
import analytics.util.SingletonJsonParser;
import analytics.util.VibesUtil;
import analytics.util.dao.ClientApiKeysDAO;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.TagMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class CPProcessingBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;
	private static String cps_api_key;
	private static final Logger LOGGER = LoggerFactory.getLogger(CPProcessingBolt.class);
	private OutputCollector outputCollector;
	private RTSAPICaller rtsApiCaller;
	private static final String cps_api_key_param = "CPS";
	private CPSFiler cpsFiler;
	
	private VibesUtil vibesUtil;
	private String host;
	private int port;
	
	public CPProcessingBolt(String env) {
		super(env);
	}
	
	public CPProcessingBolt(String env, String host, int port) {
		super(env);
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;		
		try {
			rtsApiCaller = RTSAPICaller.getInstance();			
			cpsFiler = new CPSFiler();
			cpsFiler.initDAO();	
			cps_api_key = new ClientApiKeysDAO().findkey(cps_api_key_param);
			
			vibesUtil = VibesUtil.getInstance();
		} catch (Exception e) {
			LOGGER.error("PERSIST: "+e.getClass() + ": " + ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("input_count");
		String lyl_id_no = null; 
		String l_id = null; 
			
		if(input != null && input.contains("lyl_id_no"))
		{
			lyl_id_no = input.getStringByField("lyl_id_no");
			l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
			
			try{
				//call rts api and get response for this l_id 
				//20 - level, rtsTOtec is the apikey for internal calls to RTS API from topologies
				String rtsAPIResponse = rtsApiCaller.getRTSAPIResponse(lyl_id_no, "20", cps_api_key, "sears", Boolean.FALSE, "");
				
				//Using the RTSApi response fetch the rank 1 element and check if Vibes is ready. If yes, set Redis with necessary info.
				try{
					MemberInfo memberInfo = cpsFiler.getMemberInfoObj(l_id);
					SingletonJsonParser singletonJsonParser = SingletonJsonParser.getInstance();
					if(rtsAPIResponse!=null && singletonJsonParser != null){
						Long time = System.currentTimeMillis();
						JsonObject obj = singletonJsonParser.getGsonInstance().fromJson (rtsAPIResponse, JsonElement.class).getAsJsonObject();
					
						TagMetadata tagMetaData = vibesUtil.getTagMetaDataInfo(obj);
						vibesUtil.getTextInfo(memberInfo,tagMetaData );
						
						if(tagMetaData != null && vibesUtil.publishVibesToRedis(tagMetaData,host, port, lyl_id_no)){
							countMetric.scope("adding_to_vibes_call").incr();
							LOGGER.info("Time taken to process Vibes : " + (System.currentTimeMillis()- time));
						}
					}
				}catch(Exception e){
					LOGGER.error("PERSIST: Exception in CPProcessingBolt Vibes for memberId :: "+ lyl_id_no + " : "+ ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) + "  SATCKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
					redisCountIncr("exception_count");	
				}
				
				//End
				
				List<EmailPackage> emailPackages = cpsFiler.prepareEmailPackages(rtsAPIResponse,lyl_id_no,l_id );
				LOGGER.info("PERSIST: MemberId : " + lyl_id_no+ " | Tags to be queued : ["+getLogMsg(emailPackages) + "]");
				
				if(emailPackages!= null && emailPackages.size()>0){
					List<EmailPackage> queuedEmailPackages = cpsFiler.fileEmailPackages(emailPackages);
					if(queuedEmailPackages!= null && queuedEmailPackages.size()>0){
						LOGGER.info("PERSIST: MemberId : " + lyl_id_no + " | CPS STATUS : QUEUED " + " | TAGS: "+  getLogMsg(queuedEmailPackages));
						redisCountIncr("queued_tags_count");											
					}else{
						LOGGER.info("PERSIST: MemberId : " + lyl_id_no + " | CPS STATUS : NOT QUEUED****");	
						redisCountIncr("not_queued_tags_count");
					}
					
				}			
			} catch (SQLException e){
				LOGGER.error("PERSIST: SQLException Occured in CPProcessingBolt for memberId :: "+ lyl_id_no + " : "+ ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) + "  SATCKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("SQLException_count");	
				e.printStackTrace();									
			} catch (Exception e){
				LOGGER.error("PERSIST: Exception Occured in CPProcessingBolt for memberId:: " + lyl_id_no + " : "+  ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) + "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
				e.printStackTrace();
			}
				
		} else {
			redisCountIncr("null_lid");			
			//outputCollector.fail(input);				
		}
		redisCountIncr("output_count");	
		outputCollector.ack(input);
	}

	private String getLogMsg(List<EmailPackage> emailPackages) {
		String logMsg = "  ";
		if(emailPackages!=null && emailPackages.size()>0){
				for(EmailPackage emailPackage : emailPackages)
				{
					logMsg = logMsg.concat(emailPackage.getMdTagMetaData().getMdTag()).concat("  "); 
				}				
						
		}	
		
		return logMsg;
	}
	
	
}
