package analytics.bolt;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.SecurityUtils;
import analytics.util.dao.EventsVibesActiveDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VibesProcessingBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(VibesProcessingBolt.class);
	private OutputCollector outputCollector;

	HashMap<String, HashMap<String, String>> eventVibesActiveMap = new HashMap<String, HashMap<String, String>>();
	private EventsVibesActiveDao eventsVibesActiveDao;
	private String host;
	private int port;

	public VibesProcessingBolt(String env, String host, int port) {
		super(env);
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;		
		
		eventsVibesActiveDao = new EventsVibesActiveDao();
		eventVibesActiveMap = eventsVibesActiveDao.getVibesActiveEventsList();
		
	}


	@Override
	public void execute(Tuple input) {
		redisCountIncr("VibesProcessingBolt_input_count");
		Jedis jedis = null;

		countMetric.scope("entering_VibesProcessingBolt").incr();			
			
		if(input != null)
		{
			try{
				String loyalty_id = (String) input.getValueByField("lyl_id_no");
				String tag = (String) input.getValueByField("tag");
				String occasion = (String) input.getValueByField("occasion");
				LOGGER.info("Input received to VibesProcessingBolt :" + input);				
				
				if(loyalty_id!=null){
					StringBuilder custVibesEvent = new StringBuilder();
					if(isVibesActiveWithEvent(occasion, tag.substring(0,5), custVibesEvent)){
						jedis = new Jedis(host, port, 1800);
						jedis.connect();
						jedis.set("Vibes:"+loyalty_id, custVibesEvent.toString());
						jedis.disconnect();
						countMetric.scope("adding_to_vibes_call").incr();
						custVibesEvent = null;
					}
				}
			} catch (Exception e){
				LOGGER.error("PERSIST:Exception Occured in VibesProcessingBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
				//outputCollector.fail(input);	
			}
				
		} else {
			redisCountIncr("null_lid");			
			//outputCollector.fail(input);				
		}
		outputCollector.ack(input);
		
	}
	

	public boolean isVibesActiveWithEvent(String occasion, String bussUnit, StringBuilder custVibesEvent){
		
		if(eventVibesActiveMap.get(occasion)!= null){
			if(eventVibesActiveMap.get(occasion).get(bussUnit)!=null)
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(bussUnit));
			else
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(null));
		}
		
		//Log the info incase Vibes isn;t ready with the occasion and BU
		if(custVibesEvent.toString().isEmpty() || custVibesEvent.toString().equals("null"))
			LOGGER.info("Vibes is not ready for Occasion "+occasion+ " for BU "+bussUnit);
		
		return (!custVibesEvent.toString().equals("null") && !custVibesEvent.toString().isEmpty());
	}

}