package analytics.bolt;

import analytics.util.Constants;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.ChangedMemberVariablesDao;
import analytics.util.dao.DCDao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.ModelSywBoostDao;
import analytics.util.dao.SourcesDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.Variable;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class SywScoringBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(SywScoringBolt.class);
	private ChangedMemberScoresDao changedMemberScoresDao;
	private ChangedMemberVariablesDao changedMemberVariablesDao;
	private MemberScoreDao memberScoreDao;
	private ModelSywBoostDao modelBoostDao;
	private ModelPercentileDao modelPercentileDao;

	private Map sywBoostModelMap;
	private Map dcBoostModelMap;
	//TODO: Change this after refactoring
	private Map boostModelMap;
	private Map<Integer, Map<Integer, Double>> modelPercentileMap;
	private SimpleDateFormat simpleDateFormat;
	private OutputCollector outputCollector;
	private List<Integer> monthlyModelsMap;
	private Map<String, String> sourcesMap;
	private MultiCountMetric countMetric;
	private Map<String,String> variableNameToVidMap;
	private VariableDao variableDao;
	

	void initMetrics(TopologyContext context) {
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, Constants.METRICS_INTERVAL);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		initMetrics(context);
		System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		outputCollector = collector;
		variableDao = new VariableDao();
		// populate the variableVidToNameMap
		modelPercentileDao = new ModelPercentileDao();
		memberScoreDao = new MemberScoreDao();
		changedMemberScoresDao = new ChangedMemberScoresDao();
		changedMemberVariablesDao = new ChangedMemberVariablesDao();
		modelBoostDao = new ModelSywBoostDao();
		sywBoostModelMap = modelBoostDao.getVarModelMap();
		dcBoostModelMap = new DCDao().getDCModelMap();
		modelPercentileMap = modelPercentileDao.getModelPercentiles();
		
		variableNameToVidMap = new HashMap<String, String>();
		List<Variable> variables = variableDao.getVariables();
		for(Variable variable:variables){
			if (variable.getName() != null && variable.getVid()!= null) {
				variableNameToVidMap.put(variable.getName(), variable.getVid());
			}
		}
		
		simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		// TODO: Dont hard code this
		monthlyModelsMap = new ArrayList<Integer>();
		monthlyModelsMap.add(20);
		monthlyModelsMap.add(27);
		monthlyModelsMap.add(30);
		monthlyModelsMap.add(59);
		

	}

	@Override
	public void execute(Tuple input) {
		// l_id="jgjh" , source="SYW_LIKE/OWN/WANT
		// newChangesVarValueMap - similar to strategy bolt
		// current-pid, 2014-09-25-[6],...
		countMetric.scope("incoming_record").incr();
		String lId = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		if(source == "DC"){
			boostModelMap = dcBoostModelMap;
		}
		else{
			boostModelMap = sywBoostModelMap;
		}
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}

		// TODO: Reuse this as a function. AAM ATC uses a very similar piece of
		// code
		// 2) Create map of new changes from the input
		Map<String, Integer> varToCountMap = getVarToCount(input);
		Map<String, Change> varChanges = new HashMap<String, Change>();
		Date today = new Date();
		Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 7);
        Date expiry = cal.getTime();  
		for(Entry<String, Integer> varValue: varToCountMap.entrySet()){
			varChanges.put(varValue.getKey(), new Change( varValue.getKey(), varValue.getValue(), expiry, today));
		}
		LOGGER.trace(" varToCountMap: " + varToCountMap + " lid: " + lId);
		// boost_syw... hand_tools_tcount
		// boost_syw... tools_tcount
		// var to Count map -
		// varname, totalcount across all days

		Map<Integer, String> modelIdToScore = fillModelIdToScore(lId, varToCountMap);
		LOGGER.trace(" modelIdToScore map first: " + modelIdToScore + " lid: " + lId);

		// TODO: Loop through the modelIdToScore map
		// Add scoring logic here
		// varToCount map has the total count for each variable
		modelIdToScore = updateModelIdToScore(lId, source, messageID, varToCountMap, modelIdToScore);
		//outputCollector.ack(input);

		LOGGER.trace(" modelIdToScore map second: " + modelIdToScore + " lid: " + lId);
		changedMemberVariablesDao.upsertUpdateChangedVariables(lId, varChanges, variableNameToVidMap);
		LOGGER.info("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + lId + ": scores: " + modelIdToScore);
		
		updateChangedMemberScore(lId, modelIdToScore,source);
		List<Object> listToEmit = new ArrayList<Object>();
		listToEmit.add(lId);
		listToEmit.add(source);
		countMetric.scope("member_scored").incr();
		this.outputCollector.emit("member_stream", listToEmit);
		this.outputCollector.ack(input);
	}

	private Map<Integer, String> updateModelIdToScore(String lId, String source, String messageID, Map<String, Integer> varToCountMap, Map<Integer, String> modelIdToScore) {
		for (String v : varToCountMap.keySet()) {
			String oldScore = new String();
			int boostPercetages = 0;
			if (boostModelMap.get(v) instanceof Integer) {
				int modelId = (Integer) boostModelMap.get(v);
				modelIdToScore = insertModelToScoreUpdate(lId, source, messageID, varToCountMap, modelIdToScore, v, oldScore, boostPercetages, modelId);
			} else if (boostModelMap.get(v) instanceof List) {
				List<Integer> modelIds = (List<Integer>) boostModelMap.get(v);
				for (Integer modelId : modelIds) {
					modelIdToScore = insertModelToScoreUpdate(lId, source, messageID, varToCountMap, modelIdToScore, v, oldScore, boostPercetages, modelId);
				}
			}
		}
		return modelIdToScore;
	}

	private Map<Integer, String> insertModelToScoreUpdate(String lId, String source, String messageID, Map<String, Integer> varToCountMap, Map<Integer, String> modelIdToScore,
			String v, String oldScore, int boostPercetages, int modelId) {
		if (modelIdToScore == null || modelIdToScore.get(modelId) == null) {
			// Getting next model since current one does not have score
			return modelIdToScore;
		}
		oldScore = modelIdToScore.get(modelId);
		if (source.equals("SYW_OWN")) {
			Double maxScore = modelPercentileMap.get(modelId).get(50);

			if (Double.valueOf(modelIdToScore.get(modelId)) > maxScore) {
				modelIdToScore.put(modelId, maxScore.toString());
			}
		} else if (source.equals("SYW_LIKE")) {
			if (varToCountMap.get(v) <= 10) {
				boostPercetages += ((int) Math.ceil(varToCountMap.get(v) / 2.0)) - 1;
			} else {
				boostPercetages = 5;
			}
			Double maxScore = modelPercentileMap.get(modelId).get(90 + boostPercetages);
			if (Double.valueOf(modelIdToScore.get(modelId)) < maxScore) {
				modelIdToScore.put(modelId, maxScore.toString());
			}
		} else if (source.equals("SYW_WANT")) {
			if (varToCountMap.get(v) <= 8) {
				boostPercetages += ((int) Math.ceil(varToCountMap.get(v) / 2.0)) - 1;
			} else {
				boostPercetages = 4;
			}
			Double maxScore = modelPercentileMap.get(modelId).get(96 + boostPercetages);

			if (Double.valueOf(modelIdToScore.get(modelId)) < maxScore) {
				modelIdToScore.put(modelId, maxScore.toString());
			}
		}
		else if (source.equals("DC")){
			if (varToCountMap.get(v) <= 10) {
				boostPercetages += ((int) Math.ceil(varToCountMap.get(v) / 2.0)) - 1;
			} else {
				boostPercetages = 5;
			}
			Double maxScore = modelPercentileMap.get(modelId).get(90 + boostPercetages);
			if (Double.valueOf(modelIdToScore.get(modelId)) < maxScore) {
				modelIdToScore.put(modelId, maxScore.toString());
			}
		}

		List<Object> listToEmit = new ArrayList<Object>();
		listToEmit.add(lId);
		listToEmit.add(oldScore);
		listToEmit.add(Double.parseDouble(modelIdToScore.get(modelId)));
		listToEmit.add(String.valueOf(modelId));
		listToEmit.add(source);
		listToEmit.add(messageID);
		countMetric.scope("scored_" + source).incr();
		outputCollector.emit("score_stream", listToEmit);
		return modelIdToScore;
	}

	private Map<Integer, String> fillModelIdToScore(String lId, Map<String, Integer> varToCountMap) {
		Map<Integer, String> modelIdToScore = new HashMap<Integer, String>();
		Map<String, String> memberScores = memberScoreDao.getMemberScores(lId);
		Map<String, ChangedMemberScore> changedMemberScores = changedMemberScoresDao.getChangedMemberScores(lId);
		// Also read and keep changedMemberScores
		for (String variableName : varToCountMap.keySet()) {
			// Change dao to take in multiple variable names and return list of
			// modelIds
			// could be a list for DCModel, check return type first

			if (boostModelMap.get(variableName) instanceof Integer) {
				Integer modelId = (Integer) boostModelMap.get(variableName);
				modelIdToScore = insertModelIdToScore(modelIdToScore, memberScores, changedMemberScores, modelId);
			} else if (boostModelMap.get(variableName) instanceof List) {
				List<Integer> modelIds = (List<Integer>) boostModelMap.get(variableName);
				for (Integer modelId : modelIds) {
					modelIdToScore = insertModelIdToScore(modelIdToScore, memberScores, changedMemberScores, modelId);
				}
			}
			// modelId does not exist for current fake variable

		}
		return modelIdToScore;
	}

	private Map<Integer, String> insertModelIdToScore(Map<Integer, String> modelIdToScore, Map<String, String> memberScores, Map<String, ChangedMemberScore> changedMemberScores,
			Integer modelId) {
		ChangedMemberScore cs = changedMemberScores.get(modelId.toString());
		if (cs != null) {
			Date expiry;
			try {
				expiry = simpleDateFormat.parse(cs.getMinDate());
				if (expiry.after(new Date())) {
					modelIdToScore.put(modelId, String.valueOf(cs.getScore()));
				}
			} catch (ParseException e) {
				LOGGER.error("Unable to parse date", e);
			}
		}
		if (!modelIdToScore.containsKey(modelId))
			modelIdToScore.put(modelId, memberScores.get(modelId.toString()));
		return modelIdToScore;
	}

	private Map<String, Integer> getVarToCount(Tuple input) {

		Map<String, String> newChangesVarValueMap = JsonUtils.restoreVariableListFromJson(input.getString(1));
		Map<String, Integer> varToCountMap = new HashMap<String, Integer>();

		for (String variableName : newChangesVarValueMap.keySet()) {
			Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(variableName));
			int totalPidCount = 0;

			if (dateValuesMap != null && dateValuesMap.containsKey("current")) {
				totalPidCount += dateValuesMap.get("current").size();
				dateValuesMap.remove("current");
				if (!dateValuesMap.isEmpty()) {
					for (String key : dateValuesMap.keySet()) {
						try {
							if (!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(7).toDateMidnight().toDate()))
								for (String v : dateValuesMap.get(key)) {
									totalPidCount += Integer.valueOf(v);
								}
						} catch (NumberFormatException e) {
							LOGGER.warn(e.getMessage(), e);
						} catch (ParseException e) {
							LOGGER.warn(e.getMessage(), e);
						}
					}
				}
			}
			varToCountMap.put(variableName, totalPidCount);
		}
		return varToCountMap;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream", new Fields("l_id", "oldScore", "newScore", "model", "source", "messageID"));
		declarer.declareStream("member_stream", new Fields("l_id", "source"));
	}

	public void updateChangedMemberScore(String lId, Map<Integer, String> modelIdToScore, String source) {
		Map<Integer, ChangedMemberScore> updatedScores = new HashMap<Integer, ChangedMemberScore>();
		for (Integer modelId : modelIdToScore.keySet()) {
			// FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR
			// CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
			if (modelIdToScore == null || modelIdToScore.get(modelId) == null) {
				// Getting next model since current one does not have score
				continue;
			}
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, 7); // a week from today
			Date oneWeekFromNow = cal.getTime();

			Date minDate = oneWeekFromNow;
			Date maxDate = oneWeekFromNow;

			// IF THE MODEL IS MONTH SPECIFIC AND THE MIN/MAX DATE IS AFTER THE
			// END OF THE MONTH SET TO THE LAST DAY OF THIS MONTH
			if (monthlyModelsMap.contains(modelId)) {
				Calendar calendar = Calendar.getInstance();
				calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
				Date lastDayOfMonth = calendar.getTime();

				if (minDate != null && minDate.after(lastDayOfMonth)) {
					minDate = lastDayOfMonth;
					maxDate = lastDayOfMonth;
				} else if (maxDate != null && maxDate.after(lastDayOfMonth)) {
					maxDate = lastDayOfMonth;
				}
			}
			String today = simpleDateFormat.format(new Date());
			// APPEND CHANGED SCORE AND MIN/MAX EXPIRATION DATES TO DOCUMENT FOR
			// UPDATE
			updatedScores.put(modelId, new ChangedMemberScore(Double.parseDouble(modelIdToScore.get(modelId)), minDate != null ? simpleDateFormat.format(minDate) : today,
					maxDate != null ? simpleDateFormat.format(maxDate) : today, simpleDateFormat.format(new Date()), source));
		}
		if (updatedScores != null && !updatedScores.isEmpty()) {
			changedMemberScoresDao.upsertUpdateChangedScores(lId, updatedScores);
		}
	}

}
