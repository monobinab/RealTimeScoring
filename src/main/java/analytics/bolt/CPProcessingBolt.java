package analytics.bolt;


import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.math.BigInt;
import analytics.exception.RealTimeScoringException;
import analytics.util.CPSFiler;
import analytics.util.MongoNameConstants;
import analytics.util.RTSAPICaller;
import analytics.util.SecurityUtils;
import analytics.util.dao.ClientApiKeysDAO;
import analytics.util.objects.EmailPackage;
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
	
	private static BigInteger startLoyalty = new BigInteger("7081010000647509"); 
	private static BigInteger lastLoyalty = new BigInteger("7081021610457114");
	
	public CPProcessingBolt(String env) {
		super(env);
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
				List<EmailPackage> emailPackages = cpsFiler.prepareEmailPackages(rtsAPIResponse,lyl_id_no,l_id);
				LOGGER.info("PERSIST:Tags to be queued to outbox for lyl_id_no " + lyl_id_no+ " : ["+getLogMsg(emailPackages) + "]");
				
				if(emailPackages!= null && emailPackages.size()>0)
				{
					cpsFiler.fileEmailPackages(emailPackages);
					LOGGER.info("PERSIST: Queued Tags in CPS Outbox for memberId " + lyl_id_no+ " : "+getLogMsg(emailPackages));
					redisCountIncr("queued_tags_count");
					
				}					
			} catch (SQLException e){
				LOGGER.error("PERSIST: SQLException Occured in CPProcessingBolt for memberId :: "+ lyl_id_no + " : "+ ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) + "  SATCKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("SQLException_count");	
				e.printStackTrace();
				//outputCollector.fail(input);					
			} catch (Exception e){
				LOGGER.error("PERSIST: Exception Occured in CPProcessingBolt for memberId:: " + lyl_id_no + " : "+  ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) + "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
				e.printStackTrace();
				//outputCollector.fail(input);	
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
		for(EmailPackage emailPackage : emailPackages)
		{
			logMsg = logMsg.concat(emailPackage.getMdTagMetaData().getMdTag()).concat("  "); 
		}
		return logMsg;
	}
	
}
