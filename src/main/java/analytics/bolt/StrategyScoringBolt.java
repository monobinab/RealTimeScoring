package analytics.bolt;

import analytics.util.JsonUtils;
import analytics.util.ScoringSingleton;
import analytics.util.dao.MemberVariablesDao;
import analytics.util.jedis.JedisFactoryImpl;
import analytics.util.jedis.JedisFactory;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.MemberRTSChanges;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.*;

/**
 * 
 * @author dnairsy Bolt to find the models affected by a list of variables,
 *         apply strategy on each model and rescore each model
 *
 */
public class StrategyScoringBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyScoringBolt.class);
	
	private String host;
	private int port;
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private String topologyName;
	ScoringSingleton scoringSingleton;
	MemberVariablesDao memDao;
	DB db;
	DBCollection coll;
	
	private String respHost;
	private int respPort;
	JedisFactory jedisInterface;
	
	public JedisFactory getJedisInterface() {
		return jedisInterface;
	}

	public void setJedisInterface(JedisFactory jedisInterface) {
		this.jedisInterface = jedisInterface;
	}

	public StrategyScoringBolt(String systemProperty, String host, int port, String respHost, int respPort) {
		super(systemProperty);
		this.host = host;
		this.port = port;
		this.respHost = respHost;
		this.respPort = respPort;
	}
	
	 public StrategyScoringBolt(String systemProperty){
		 super(systemProperty);
	 }


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		LOGGER.info("PREPARING STRATEGY SCORING BOLT");	
	  	this.outputCollector = collector;
	  	
	  	if(getJedisInterface() == null){
	  		jedisInterface = new JedisFactoryImpl();
	  		setJedisInterface(jedisInterface);
	  	}
	  
	  	topologyName = (String) stormConf.get("metrics_topology");
	  	scoringSingleton = ScoringSingleton.getInstance();
	  }
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
					
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("The time it enters inside Strategy Bolt execute method "	+ System.currentTimeMillis());
		}
		Jedis jedis = null;

		//PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		redisCountIncr("incoming_tuples");
		String lId = input.getStringByField("l_id");

		String source = "";
		String lyl_id_no = "";
		
		LOGGER.info("Incoming Message to StrategyScoringBolt " + input.toString());
		
		try{
			if (input.contains("lyl_id_no")) {
				lyl_id_no = input.getStringByField("lyl_id_no");
			}
					
			if(input.contains("source")){
				source = input.getStringByField("source");
			}
			else
				source = topologyName;
			
			String messageID = "";
			if (input.contains("messageID")) {
				messageID = input.getStringByField("messageID");
			}
	
			LOGGER.debug("TIME:" + messageID + "-Entering scoring bolt-" + System.currentTimeMillis());
			
			//Create map of new changes from the input
			Map<String, String> newChangesVarValueMap = JsonUtils.restoreVariableListFromJson(input.getString(1));
			
			if(newChangesVarValueMap == null || newChangesVarValueMap.isEmpty()){
				outputCollector.ack(input);
				return;
			}
	
			MemberRTSChanges memberRTSChanges = scoringSingleton.calcRTSChanges(lId, newChangesVarValueMap, null, source);

			if(memberRTSChanges == null  || memberRTSChanges.getChangedMemberScoreList() == null || memberRTSChanges.getChangedMemberScoreList().isEmpty()){
				redisCountIncr(memberRTSChanges.getMetricsString());
				outputCollector.ack(input);
				return;
			}
			
			Map<String, String> modelIdScoreStringMap = new HashMap<String, String>();
			List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
			
			//for TI_POS, score-value map set in redis for the specific member
			for(ChangedMemberScore changedMemberScore : changedMemberScoresList){
				modelIdScoreStringMap.put(""+changedMemberScore.getModelId(), ""+BigDecimal.valueOf(changedMemberScore.getScore()).toPlainString());
			}
		
			//Persisting to Redis to be retrieved quicker than getting from Mongo.
			//Perform the below operation only when the Redis is configured
			//Long timeBefore = System.currentTimeMillis();
			if(host != null ){
				jedis = getJedisInterface().createJedis(host, port);
				jedis.connect();
				jedis.hmset("RTS:Telluride:"+lId, modelIdScoreStringMap);
				/*if(testMode){
					setFakeRedis(jedis);
				}*/
				
				jedis.expire("RTS:Telluride:"+lId, 600);
				jedis.disconnect();
			}
		
		
			//Write changedMemberVariableswith expiry
			if(memberRTSChanges.getAllChangesMap() != null && !memberRTSChanges.getAllChangesMap().isEmpty() )
				scoringSingleton.updateChangedMemberVariables(lId, memberRTSChanges.getAllChangesMap());
	    
	    	//Write changedMemberScores with min max expiry
			if(changedMemberScoresList != null && !changedMemberScoresList.isEmpty())
				scoringSingleton.updateChangedMemberScore(lId, changedMemberScoresList, source);
	      	
	      	//emitting to logging bolt
			
			//null check
	      	for(ChangedMemberScore changedMemberScore : changedMemberScoresList){
	      		if(StringUtils.isEmpty(changedMemberScore.getSource())){
	      			changedMemberScore.setSource(source);
	      		}
	      		changedMemberScore.setlId(lId);
	      		changedMemberScore.setMessageID(messageID);
			}
	      	List<Object> listToEmitMemberScoreList = new ArrayList<Object>();
	      	listToEmitMemberScoreList.add(changedMemberScoresList);
	      	this.outputCollector.emit("score_stream", listToEmitMemberScoreList);
	      	
			//persisting the loyalty id to redis for UnknownOccasionsTopology to pick up the loyalty id
			if(respHost != null){
					jedis = getJedisInterface().createJedis(respHost, respPort);
					jedis.connect();
					jedis.set("Unknown:"+lyl_id_no,"");
					jedis.disconnect();
			}
			
			//Adding logic to set up a Stream that the KafkaBolt can listen to...
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lyl_id_no+"~"+topologyName);
			this.outputCollector.emit("kafka_stream", listToEmit);
		
			redisCountIncr("member_scored_successfully");
			this.outputCollector.ack(input);
		}catch(Exception e){
			e.printStackTrace();
			LOGGER.info("Exception scoring lId " +lId +" "+ e.getCause());

		}finally{
			if(jedis!=null)
				jedis.disconnect();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream",new Fields("changedMemberScoresList"));
		declarer.declareStream("kafka_stream", new Fields("message"));
		
	}
}


