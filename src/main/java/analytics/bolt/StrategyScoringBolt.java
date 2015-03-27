package analytics.bolt;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.exception.RealTimeScoringException;
import analytics.util.Constants;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.ScoringSingleton;
import analytics.util.dao.MemberScoreDao;
import analytics.util.objects.Change;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

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
	private JedisPool jedisPool;
	private Jedis jedis;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private MultiCountMetric countMetric;
	private String environment;
	
	public StrategyScoringBolt(String systemProperty, String host, int port) {
		super(systemProperty);
		environment = systemProperty;
		this.host = host;
		this.port = port;
		
	}
	
	 public StrategyScoringBolt(String systemProperty){
		 super(systemProperty);
		 environment = systemProperty;
 }


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.setProperty(MongoNameConstants.IS_PROD, environment);
	//	super.prepare(stormConf, context, collector);
		LOGGER.info("PREPARING STRATEGY SCORING BOLT");	
	     initMetrics(context);
	  	this.outputCollector = collector;
		//Configure the Redis Server.
		if(host!=null && !host.equalsIgnoreCase("")){
			JedisPoolConfig poolConfig = new JedisPoolConfig();
	        poolConfig.setMaxActive(100);
	        jedisPool = new JedisPool(poolConfig,host, port, 100);
	    //    jedis = jedisPool.getResource();
		}
		
	}
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, Constants.METRICS_INTERVAL);
	    }

	@Override
	public void execute(Tuple input) {
		if(LOGGER.isDebugEnabled()){
		LOGGER.debug("The time it enters inside Strategy Bolt execute method "
				+ System.currentTimeMillis());
		}
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		// 2) Create map of new changes from the input
		// 3) Find all models affected by the changes
		// 4) Find all variables for models
		// 5) Create a map of variable values, fetched from from memberVariables
		// 6) For each variable in new changes, execute strategy
		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch
		// all for the member since upsert needs it too
		// 8) Rescore - arbitrate between new changes, changedMemberVariables
		// and memberVariables
		// 9) Emit the score
		// 10) Write changedMemberScores with expiry
		// 11) Write changedMemberVariables with expiry

		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		countMetric.scope("incoming_tuples").incr();
		String lId = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering scoring bolt-" + System.currentTimeMillis());
		// 2) Create map of new changes from the input
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));

		// 3) Find all models affected by the changes
		Set<Integer> modelIdList = ScoringSingleton.getInstance().getModelIdList(newChangesVarValueMap);
		if(modelIdList==null||modelIdList.isEmpty()){
			LOGGER.info("No models affected");
			countMetric.scope("no_models_affected").incr();
			outputCollector.ack(input);
			return;
		}
		// 4) Find all variables for models

		// 5) Create a map of variable values, fetched from from memberVariables
		Map<String, Object> memberVariablesMap = new HashMap<String, Object>();
		try {
			memberVariablesMap = ScoringSingleton.getInstance().createMemberVariableValueMap(lId, modelIdList);
		} catch (RealTimeScoringException e1) {
			LOGGER.error("Can not create member variable map", e1);
		}
		if(memberVariablesMap==null){
			LOGGER.warn("Unable to find member variables for " + lId);
			countMetric.scope("no_member_variables").incr();
			outputCollector.ack(input);
			return;
		}
		
		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch
		// all for the member since upsert needs it too
		
		// 6.1) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE
		// (CHANGE CLASS) and store in allChanges
		Map<String, Change> changedMemberVariables = ScoringSingleton.getInstance().createChangedVariablesMap(lId);
		
		
		// 6) For each variable in new changes, execute strategy and store in
		// allChanges
		// allChanges will contain newChanges and the changedMemberVariables
		Map<String, Change> allChanges = ScoringSingleton.getInstance().executeStrategy(changedMemberVariables, newChangesVarValueMap, memberVariablesMap);
		
		// 8) Rescore - arbitrate between all changes and memberVariables
		// Score each model in a loop
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		Map<String, String> modelIdScoreStringMap = new HashMap<String, String>();
		Map<Integer,Map<String,Date>> modelIdToExpiryMap = new HashMap<Integer, Map<String,Date>>();
		
		for (Integer modelId : modelIdList) {// Score and emit for all modelIds
												// before mongo inserts
			// recalculate score for model
			double newScore;
			try {
				newScore = ScoringSingleton.getInstance().calcScore(memberVariablesMap, allChanges,
						modelId);

			LOGGER.debug("new score before boost var: " + newScore);

			newScore = newScore + ScoringSingleton.getInstance().getBoostScore(allChanges, modelId );
			
			// 9) Emit the new score
			Map<String, Date> minMaxMap = ScoringSingleton.getInstance().getMinMaxExpiry(modelId, allChanges);
			modelIdToExpiryMap.put(modelId, minMaxMap);
			Date minExpiryDate = minMaxMap.get("minExpiry");
			Date maxExpiryDate = minMaxMap.get("maxExpiry");
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
			String minExpiry = null;
			String maxExpiry = null;
			if(minExpiryDate != null){
				minExpiry = format.format(minExpiryDate);
			}
			if(maxExpiryDate != null){
				maxExpiry = format.format(maxExpiryDate);
			}
			// TODO: change oldScore to a timestamp of the change to persist to changedMemberScore
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lId);
			listToEmit.add(newScore);
			listToEmit.add(modelId.toString());
			listToEmit.add(source);
			listToEmit.add(messageID);
			listToEmit.add(minExpiry);
			listToEmit.add(maxExpiry);
			modelIdScoreMap.put(modelId, newScore);
			
			//String Map to set to redis...
			modelIdScoreStringMap.put(""+modelId, ""+BigDecimal.valueOf(newScore).toPlainString());
			LOGGER.debug(" ### SCORING BOLT EMITTING: " + listToEmit);
			if(LOGGER.isDebugEnabled())
			LOGGER.debug("The time spent for creating scores..... "
					+ System.currentTimeMillis() + " and the message ID is ..."
					+ messageID);
			this.outputCollector.emit("score_stream",listToEmit);
			countMetric.scope("model_scored").incr();
			} catch (RealTimeScoringException e) {
				e.printStackTrace();
			}
		}
		
		//Persisting to Redis to be retrieved quicker than getting from Mongo.
		//Perform the below operation only when the Redis is configured
		Long timeBefore = System.currentTimeMillis();
		if(jedisPool!=null){
			Jedis jedis = jedisPool.getResource();
			jedis.hmset("RTS:Telluride:"+lId, modelIdScoreStringMap);
			//jedis.hset("RTS:Telluride:"+lId, ""+modelId, ""+BigDecimal.valueOf(newScore).toPlainString());
			
			jedis.expire("RTS:Telluride:"+lId, 1800);
			jedisPool.returnResource(jedis);
			
		}
		Long timeAfter = System.currentTimeMillis();
		LOGGER.info("~~~~TIME TAKEN FOR REDIS~~~: " +  (timeAfter - timeBefore));
	//	System.out.println("time for redis:" + (timeAfter - timeBefore));
		
		
		// 10) Write changedMemberVariableswith expiry
		ScoringSingleton.getInstance().updateChangedVariables(lId, allChanges);
		LOGGER.info("TIME:" + messageID + "-Score updates complete-" + System.currentTimeMillis());
		
		timeBefore = System.currentTimeMillis();
		// Write changes to changedMemberScores
		ScoringSingleton.getInstance().updateChangedMemberScore(lId, modelIdList, modelIdToExpiryMap, modelIdScoreMap,source);
		timeAfter = System.currentTimeMillis();
		LOGGER.info("~~~~~TIME TAKED FOR MONGO~~~: " + (timeAfter - timeBefore) );
	//	System.out.println("time for mongo: " + (timeAfter - timeBefore));
		LOGGER.info("TIME:" + messageID + "- Scoring complete-" + System.currentTimeMillis());
		
		List<Object> listToEmit = new ArrayList<Object>();
		listToEmit.add(lId);
		listToEmit.add(source);
		listToEmit.add(messageID);
		this.outputCollector.emit("member_stream", listToEmit);
		countMetric.scope("member_scored_successfully").incr();
		this.outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream",new Fields("l_id", "newScore", "model","source", "messageID", "minExpiry", "maxExpiry"));
		declarer.declareStream("member_stream", new Fields("l_id", "source","messageID"));

	}

}
