package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.ScoringSingleton;
import analytics.util.objects.Change;
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
public class StrategyScoringBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StrategyScoringBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		LOGGER.info("PREPARING STRATEGY SCORING BOLT");	
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		this.outputCollector = collector;
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
		String lId = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}

		// 2) Create map of new changes from the input
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));

		// 3) Find all models affected by the changes
		Set<Integer> modelIdList = ScoringSingleton.getInstance().getModelIdList(newChangesVarValueMap);
		if(modelIdList==null||modelIdList.isEmpty()){
			LOGGER.info("No models affected");
			this.outputCollector.ack(input);
			return;
		}
		// 4) Find all variables for models

		// 5) Create a map of variable values, fetched from from memberVariables
		Map<String, Object> memberVariablesMap = ScoringSingleton.getInstance().createVariableValueMap(lId, modelIdList);
		if(memberVariablesMap==null){
			LOGGER.warn("Unable to find member variables for" + lId);
			this.outputCollector.fail(input);
			return;
		}
		
		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch
		// all for the member since upsert needs it too
		
		// 6.1) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE
		// (CHANGE CLASS) and store in allChanges
		Map<String, Change> allChanges = ScoringSingleton.getInstance().createChangedVariablesMap(lId);
		
		
		// 6) For each variable in new changes, execute strategy and store in
		// allChanges
		// allChanges will contain newChanges and the changedMemberVariables
		allChanges = ScoringSingleton.getInstance().executeStrategy(allChanges, newChangesVarValueMap, memberVariablesMap);
		
		// newChanges has both changedMemberVariables and changes

		// 8) Rescore - arbitrate between all changes and memberVariables
		// Score each model in a loop
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		for (Integer modelId : modelIdList) {// Score and emit for all modelIds
												// before mongo inserts
			// recalculate score for model
			double newScore = ScoringSingleton.getInstance().calcScore(memberVariablesMap, allChanges,
					modelId);

			LOGGER.debug("new score before boost var: " + newScore);

			newScore = newScore + ScoringSingleton.getInstance().getBoostScore(allChanges, modelId );

			// 9) Emit the new score
			double oldScore = 0;
			// TODO: Why are we even emiting oldScore if its always 0
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lId);
			listToEmit.add(oldScore);
			listToEmit.add(newScore);
			listToEmit.add(modelId.toString());
			listToEmit.add(source);
			listToEmit.add(messageID);
			modelIdScoreMap.put(modelId, newScore);
			LOGGER.debug(" ### SCORING BOLT EMITTING: " + listToEmit);
			if(LOGGER.isDebugEnabled())
			LOGGER.debug("The time spent for creating scores..... "
					+ System.currentTimeMillis() + " and the message ID is ..."
					+ messageID);
			this.outputCollector.emit("score_stream",listToEmit);
		}


		// 10) Write changedMemberScores with expiry
		for (Integer modelId : modelIdList) {
			ScoringSingleton.getInstance().updateChangedVariables(lId, modelId, allChanges);
		}
		ScoringSingleton.getInstance().updateChangedMemberScore(lId, modelIdList, allChanges, modelIdScoreMap);
		List<Object> listToEmit = new ArrayList<Object>();
		listToEmit.add(lId);
		listToEmit.add(source);
		this.outputCollector.emit("member_stream", listToEmit);
		this.outputCollector.ack(input);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream",new Fields("l_id", "oldScore", "newScore", "model","source", "messageID"));
		declarer.declareStream("member_stream", new Fields("l_id", "source"));

	}

}
