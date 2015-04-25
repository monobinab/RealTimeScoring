package analytics.bolt;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.dao.MemberScoreDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Flume bolt
 * 
 * @author smehta2
 */
public class FlumeRPCBolt extends EnvironmentBolt {


	private static final Logger LOGGER = LoggerFactory.getLogger(FlumeRPCBolt.class);
	
	private static final long serialVersionUID = -4261488068349296727L;
	private static final int MAX_BACKOFF = 10000;

	private OutputCollector collector;
	private RpcClient client;
	private MemberScoreDao memberScoreDao;
	
	public FlumeRPCBolt(String systemProperty){
		 super(systemProperty);
		 }
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
			String isProd = System.getProperty(MongoNameConstants.IS_PROD);
	
		memberScoreDao = new MemberScoreDao();
		String[] hostArray = new String[1];
		this.collector = collector;
		final Properties sinkProperties = new Properties();
		Properties prop = new Properties();
	    //load a properties file from class path, inside static method
			try {
				prop.load(RedisConnection.class.getClassLoader().getResourceAsStream("resources/flume_agent.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		if(prop!=null && prop.containsKey("server") && prop.containsKey("port")){
			hostArray[0] = prop.getProperty("server") + ":" + prop.getProperty("port");
		}
		
		final StringBuilder hostsStr = new StringBuilder();
		int hostCount = 0;
		//hostArray.length;
		for (int i = 0; i < hostArray.length; i++) {
			sinkProperties.put("hosts.h" + hostCount, hostArray[i]);
			hostsStr.append("h" + hostCount);
			hostCount++;
			if (i != hostArray.length) {
				hostsStr.append(" ");
			}
		}
		// sinkProperties.put(NPOSConstant.FLUME_PROP_CLIENT_TYPE,
		sinkProperties.put("hosts", hostsStr.toString().trim());
		sinkProperties.put("backoff", Boolean.TRUE.toString());
		sinkProperties.put("maxBackoff", String.valueOf(MAX_BACKOFF));
		
		this.client = RpcClientFactory.getInstance(sinkProperties);
		
	}

	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		// System.out.println(" %%% scorepublishbolt :" + input);
		String l_id = input.getStringByField("l_id");
		String modelId = input.getStringByField("model");
		String oldScore = memberScoreDao.getMemberScores(l_id).get(modelId);
		String source = input.getStringByField("source");
		Double newScore = input.getDoubleByField("newScore");
		String minExpiry = input.getStringByField("minExpiry");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("MessageID", "LogMessage");
		LOGGER.info("TIME:" + messageID + "-Entering logging bolt-" + System.currentTimeMillis());
		String logMessage = "PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+": source: " + source;
		Event event = EventBuilder.withBody(logMessage.getBytes(), headers);
		try {
			LOGGER.error("Received message");
			if(client==null){
				LOGGER.error("client is null");
			}
			if(event == null){
				LOGGER.error("event is null");
			}
			client.append(event);
		} catch (EventDeliveryException e) {
			e.printStackTrace();
		} catch(NullPointerException e){
			e.printStackTrace();
		}
		countMetric.scope("score_logged").incr();
		this.collector.ack(input);	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		if (client != null) {
			try {
				client.close();
			} catch (Exception e) {
			}
			client = null;
		}
	}
}
