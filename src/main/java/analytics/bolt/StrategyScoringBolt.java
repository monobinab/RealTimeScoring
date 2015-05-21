package analytics.bolt;

import analytics.exception.RealTimeScoringException;
import analytics.util.JsonUtils;
import analytics.util.ScoringSingleton;
import analytics.util.dao.MemberInfoDao;
import analytics.util.objects.Change;
import analytics.util.objects.MemberInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
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
	private MemberInfoDao memberInfoDao;
	
	private String respHost;
	private int respPort;
	
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
	  	topologyName = (String) stormConf.get("metrics_topology");
	  	memberInfoDao = new MemberInfoDao();
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
		
		Jedis jedis = null;

		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		redisCountIncr("incoming_tuples");
		String lId = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String lyl_id_no = "";
		try{
		if (input.contains("lyl_id_no")) {
			lyl_id_no = input.getStringByField("lyl_id_no");
		}
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.debug("TIME:" + messageID + "-Entering scoring bolt-" + System.currentTimeMillis());
		// 2) Create map of new changes from the input
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));

		// 3) Find all models affected by the changes
		Set<Integer> modelIdList = ScoringSingleton.getInstance().getModelIdList(newChangesVarValueMap);
		if(modelIdList==null||modelIdList.isEmpty()){
			LOGGER.info("No models affected for " +lyl_id_no);
			redisCountIncr("no_models_affected");
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
			LOGGER.info("Unable to find member variables for " + lId);
			redisCountIncr("no_member_variables");
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
		
		//get the state for the memberId to get the regionalFactor for scoring
		MemberInfo memberIfo = memberInfoDao.getMemberInfo(lId);
		String state = memberIfo.getState();
		
		for (Integer modelId : modelIdList) {// Score and emit for all modelIds
												// before mongo inserts
			// recalculate score for model
			double newScore;
			double regionalFactor = 1.0;
			try {
				newScore = ScoringSingleton.getInstance().calcScore(memberVariablesMap, allChanges,
						modelId);

			LOGGER.debug("new score before boost var: " + newScore);

			newScore = newScore + ScoringSingleton.getInstance().getBoostScore(allChanges, modelId );
			
			//get the score weighed with regionalFactor only if the member has state
			if(StringUtils.isNotEmpty(state)){
				regionalFactor = ScoringSingleton.getInstance().calcRegionalFactor(modelId, state);
			}
			newScore = newScore * regionalFactor;
			if(newScore > 1.0)
				newScore = 1.0;
			LOGGER.info("new score after regional factor with regional factor " + regionalFactor + " "+ newScore + " " + topologyName);

			//persisting the loyalty id to redis for UnknownResponsys topology to pick up the loyalty id
			if(host != null){
				jedis = new Jedis(respHost, respPort, 1800);
				jedis.connect();
				jedis.set("Unknown:"+lyl_id_no,"");
				jedis.disconnect();
			}
			
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
			redisCountIncr("model_scored");
			} catch (RealTimeScoringException e) {
				e.printStackTrace();
			}
		}
		
		//Persisting to Redis to be retrieved quicker than getting from Mongo.
		//Perform the below operation only when the Redis is configured
		//Long timeBefore = System.currentTimeMillis();
		//if(jedisPool!=null){
		if(host!=null){
			jedis = new Jedis(host, port, 1800);
			jedis.connect();
			jedis.hmset("RTS:Telluride:"+lId, modelIdScoreStringMap);
			//jedis.hset("RTS:Telluride:"+lId, ""+modelId, ""+BigDecimal.valueOf(newScore).toPlainString());
			
			jedis.expire("RTS:Telluride:"+lId, 600);
			jedis.disconnect();
			//jedisPool.returnResource(jedis);
			
		}
				
		// 10) Write changedMemberVariableswith expiry
    	ScoringSingleton.getInstance().updateChangedVariables(lId, allChanges);
		LOGGER.debug("TIME:" + messageID + "-Score updates complete-" + System.currentTimeMillis());
	
		ScoringSingleton.getInstance().updateChangedMemberScore(lId, modelIdList, modelIdToExpiryMap, modelIdScoreMap,source);
		
		LOGGER.debug("TIME:" + messageID + "- Scoring complete-" + System.currentTimeMillis());
		
		/*List<Object> listToEmit = new ArrayList<Object>();
		//member_stream is commented as MemberPublish bolt to redis is not in use now
		listToEmit.add(lId);
		listToEmit.add(source);
		listToEmit.add(messageID);
		this.outputCollector.emit("member_stream", listToEmit);*/
		redisCountIncr("member_scored_successfully");
		if(lyl_id_no!=null){
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lyl_id_no);
			listToEmit.add(messageID);
			this.outputCollector.emit("response_stream", listToEmit);//response_stream_unknown
		}
		
		this.outputCollector.ack(input);
		}catch(Exception e){
			e.printStackTrace();
			LOGGER.info("Exception scoring lId " +lId );
		}finally{
			if(jedis!=null)
				jedis.disconnect();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream",new Fields("l_id", "newScore", "model","source", "messageID", "minExpiry", "maxExpiry"));
	//	declarer.declareStream("member_stream", new Fields("l_id", "source","messageID"));
		declarer.declareStream("response_stream", new Fields("lyl_id_no","messageID"));
	}

}


