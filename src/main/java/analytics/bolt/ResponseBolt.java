package analytics.bolt;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import analytics.util.dao.OccasionResponsesDao;
import analytics.util.dao.OccationCustomeEventDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagResponsysActiveDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;
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
	private JedisPool jedisPool;
	private ResponsysUtil responsysUtil;
	
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
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        jedisPool = new JedisPool(poolConfig,host, port, 100);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String lyl_id_no = null; 
		
		try {
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
			}
			LOGGER.info("TIME:" + messageID + "-Entering Response bolt-" + System.currentTimeMillis());
			
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				
				LOGGER.info("TIME:" + messageID + "-Calling API complete-" + System.currentTimeMillis());
				
				//4-2-2015.Recent update to send responses only for 1 tag irrespective of 
				//how many tags we receive in the difference. This occasion tag 
				//for which the response has to be sent is taken from the 1st ranks occasion tags from the API call
				
				//Get the Difference Tags from Redis for an lid
				Jedis jedis = jedisPool.getResource();
				String diffTags = null;
				if(jedis.exists("Responses:"+l_id))
					diffTags = jedis.get("Responses:"+l_id).toString() ;
				jedisPool.returnResource(jedis);
				
				/*if(diffTags!=null && !"".equals(diffTags)){
					String[] tags = diffTags.split(",");
					//Send response for every new tag scored
					for(int i=0 ;i<tags.length ;i++){
						String tag = tags[i];
						getResponseServiceResult(scoreInfoJsonString,lyl_id_no,tag);
						countMetric.scope("responses").incr();
					}
				}*/
				
				if(diffTags!=null && !"".equals(diffTags)){
					//Get the metadata info for all the tags
					ArrayList<TagMetadata> list = responsysUtil.getTagMetaDataList(diffTags);
					
					LOGGER.info("TIME:" + messageID + "-Making responsys call-" + System.currentTimeMillis());
					//if( readyToProcessTags.size()>0){
						TagMetadata tagMetadata = responsysUtil.getResponseServiceResult(scoreInfoJsonString,lyl_id_no,list,l_id, messageID);
						LOGGER.info("TIME:" + messageID + "-Completed responsys call-" + System.currentTimeMillis());
						if(tagMetadata!=null && tagMetadata.getPurchaseOccasion()!=null && tagMetadata.getEmailOptIn()!=null && tagMetadata.getEmailOptIn().equals("N")){
								jedis = jedisPool.getResource();
								//TODO: Should we just do a put??
								jedis.append("Vibes:"+lyl_id_no, tagMetadata.getPurchaseOccasion());
								jedisPool.returnResource(jedis);
							
						}
							countMetric.scope("responses").incr();
				}

			}
			LOGGER.info("TIME:" + messageID + "-Completed Response bolt-" + System.currentTimeMillis());
			outputCollector.ack(input);
			
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("responses_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	

}
