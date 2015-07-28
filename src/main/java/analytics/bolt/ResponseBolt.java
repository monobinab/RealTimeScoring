package analytics.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import analytics.util.dao.EventsVibesActiveDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ResponseBolt extends EnvironmentBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponseBolt.class);
	private OutputCollector outputCollector;
	private String host;
	private int port;
	//private JedisPool jedisPool;
	private ResponsysUtil responsysUtil;
	private EventsVibesActiveDao eventsVibesActiveDao;
	HashMap<String, HashMap<String, String>> eventVibesActiveMap = new HashMap<String, HashMap<String, String>>();
	
	public ResponseBolt(String systemProperty, String host, int port) {
		super(systemProperty);
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		responsysUtil = new ResponsysUtil();
		this.outputCollector = collector;
		eventsVibesActiveDao = new EventsVibesActiveDao();
		eventVibesActiveMap = eventsVibesActiveDao.getVibesActiveEventsList();
		
		//JedisPoolConfig poolConfig = new JedisPoolConfig();
        //poolConfig.setMaxActive(100);
        //jedisPool = new JedisPool(poolConfig,host, port, 100);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		Long startTime = System.currentTimeMillis();
		redisCountIncr("ResponseBolt_input_count");
		String lyl_id_no = null; 
		Jedis jedis = null;
		countMetric.scope("entering_responsys_bolt").incr();
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
			}
			LOGGER.debug("TIME:" + messageID + "-Entering Response bolt-" + System.currentTimeMillis());
			
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				
				LOGGER.debug("TIME:" + messageID + "-Calling API complete-" + System.currentTimeMillis());
				
				//4-2-2015.Recent update to send responses only for 1 tag irrespective of 
				//how many tags we receive in the difference. This occasion tag 
				//for which the response has to be sent is taken from the 1st ranks occasion tags from the API call
				
				//Get the Difference Tags from Redis for an lid
				/*jedis = new Jedis(host, port, 1800);
				jedis.connect();
				String diffTags = null;
				if(jedis.exists("Responses:"+l_id)){
					diffTags = jedis.get("Responses:"+l_id).toString() ;
					jedis.del("Responses:"+l_id);
				}
				else{
					LOGGER.info("PERSIST: No Tags found for lyl_id_no " + lyl_id_no);
				}*/
				String diffTags = input.getStringByField("tags");
				LOGGER.info("PERSIST: Input Tags for lyl_id_no " + lyl_id_no+ " : "+diffTags);
				
				//jedis.disconnect();

				if(diffTags!=null && !"".equals(diffTags)){
					countMetric.scope("making_responsys_call").incr();
					//Get the metadata info for all the tags
					//ArrayList<TagMetadata> list = responsysUtil.getTagMetaDataList(diffTags);
					
					LOGGER.debug("TIME:" + messageID + "-Making responsys call-" + System.currentTimeMillis());
					//if( readyToProcessTags.size()>0){
					
						
						TagMetadata tagMetadata = responsysUtil.getResponseServiceResult(scoreInfoJsonString,lyl_id_no,l_id, messageID, countMetric);

						LOGGER.info(" Time Taken for ResponsysCall & Processing = " + (System.currentTimeMillis() - startTime));
						
						LOGGER.debug("TIME:" + messageID + "-Completed responsys call-" + System.currentTimeMillis());
						StringBuilder custVibesEvent = new StringBuilder();

						/*if(tagMetadata!=null && tagMetadata.getPurchaseOccasion()!=null && 
								tagMetadata.getEmailOptIn()!=null && tagMetadata.getEmailOptIn().equals("N") && 
								isVibesActiveWithEvent(tagMetadata.getPurchaseOccasion(),tagMetadata.getFirst5CharMdTag(),custVibesEvent)){
							Long time = System.currentTimeMillis();
							jedis = new Jedis(host, port, 1800);
							jedis.connect();
							jedis.set("Vibes:"+lyl_id_no, custVibesEvent.toString());
							jedis.disconnect();
							//jedisPool.returnResource(jedis);
							countMetric.scope("adding_to_vibes_call").incr();
							custVibesEvent = null;
							LOGGER.info("Time taken to process Vibes : " + (System.currentTimeMillis()- time));
						}*/
						countMetric.scope("responsys_call_completed").incr();
				}
				else{
					countMetric.scope("no_diff_tags").incr();
				}

			}
			else{
				countMetric.scope("no_lid").incr();
			}
			LOGGER.info(" Time Taken Complete = " + (System.currentTimeMillis() - startTime));
			LOGGER.debug("TIME:" + messageID + "-Completed Response bolt-" + System.currentTimeMillis());
			redisCountIncr("ResponseBolt_output_count");
			outputCollector.ack(input);
			
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("responses_failed").incr();
		}finally{
			if(jedis!=null)
				jedis.disconnect();
		}
	}

	private boolean isVibesActiveWithEvent(String occasion, String bussUnit, StringBuilder custVibesEvent){
		
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
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
