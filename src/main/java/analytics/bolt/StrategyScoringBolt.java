package analytics.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.DBConnection;
import analytics.util.JsonUtils;
import analytics.util.objects.Change;
import analytics.util.objects.Model;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.objects.StrategyMapper;
import analytics.util.objects.Variable;
import analytics.util.strategies.Strategy;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class StrategyScoringBolt extends BaseRichBolt {
	static final Logger logger = LoggerFactory
			.getLogger(StrategyScoringBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	DB db;
	private DBCollection modelVariablesCollection;
	private DBCollection memberVariablesCollection;
	private DBCollection variablesCollection;
	private DBCollection changedVariablesCollection;
	private DBCollection changedMemberScoresCollection;
	private Map<String, List<Integer>> variableModelsMap;
	private Map<Integer, Map<Integer, Model>> modelsMap;
	private Map<String, String> variableVidToNameMap;
	private Map<String, String> variableNameToVidMap;
	private Map<String, String> variableNameToStrategyMap;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		// this.outputCollector.emit(tuple);
		/*
		 * (non-Javadoc)
		 * 
		 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
		 * backtype.storm.task.TopologyContext,
		 * backtype.storm.task.OutputCollector)
		 */

		logger.info("PREPARING STRATEGY BOLT");

		try {
			db = DBConnection.getDBConnection();
		} catch (ConfigurationException e) {
			logger.error("Unable to contain DB connection", e);
		}

		modelVariablesCollection = db.getCollection("modelVariables");
		memberVariablesCollection = db.getCollection("memberVariables");
		variablesCollection = db.getCollection("Variables");
		changedVariablesCollection = db.getCollection("changedMemberVariables");
		changedMemberScoresCollection = db.getCollection("changedMemberScores");
		logger.debug("Populate variable vid map");
		// populate the variableVidToNameMap
		variableNameToStrategyMap = new HashMap<String, String>();
		variableVidToNameMap = new HashMap<String, String>();
		variableNameToVidMap = new HashMap<String, String>();
		DBCursor vCursor = variablesCollection.find();
		for (DBObject variable : vCursor) {
			String variableName = ((DBObject) variable).get("name").toString()
					.toUpperCase();
			String vid = ((DBObject) variable).get("VID").toString();
			String strategy = ((DBObject) variable).get("strategy").toString();
			if (variableName != null && vid != null) {
				variableVidToNameMap.put(vid, variableName.toUpperCase());
				variableNameToVidMap.put(variableName.toUpperCase(), vid);
				variableNameToStrategyMap.put(variableName.toUpperCase(), strategy);
			}
		}

		logger.debug("Populate variable models map");
		// populate the variableModelsMap
		variableModelsMap = new HashMap<String, List<Integer>>();
		// populate the variableModelsMap and modelsMap
		modelsMap = new HashMap<Integer, Map<Integer, Model>>();

		DBCursor models = modelVariablesCollection.find();
		for (DBObject model : models) {

			// System.out.println("modelId: " + model.get("modelId"));
			// System.out.println("month: " + model.get("month"));
			// System.out.println("constant: " + model.get("constant"));

			int modelId = Integer.valueOf(model.get("modelId").toString());
			int month = Integer.valueOf(model.get("month").toString());
			double constant = Double.valueOf(model.get("constant").toString());

			BasicDBList modelVariables = (BasicDBList) model.get("variable");
			Collection<Variable> variablesCollection = new ArrayList<Variable>();
			for (Object modelVariable : modelVariables) {
				String variableName = ((DBObject) modelVariable).get("name")
						.toString().toUpperCase();
				Double coefficient = Double.valueOf(((DBObject) modelVariable)
						.get("coefficient").toString());
				variablesCollection.add(new Variable(variableName,
						variableNameToVidMap.get(variableName), coefficient));

				if (!variableModelsMap.containsKey(variableName)) {
					List<Integer> modelIds = new ArrayList<Integer>();
					variableModelsMap.put(variableName.toUpperCase(), modelIds);
					variableModelsMap.get(variableName.toUpperCase()).add(
							Integer.valueOf(model.get("modelId").toString()));
				} else {
					if (!variableModelsMap.get(variableName).contains(
							Integer.valueOf(model.get("modelId").toString()))) {
						variableModelsMap.get(variableName.toUpperCase())
								.add(Integer.valueOf(model.get("modelId")
										.toString()));
					}
				}
			}
			if (!modelsMap.containsKey(modelId)) {
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(month, new Model(modelId, month, constant,
						variablesCollection));
				modelsMap.put(modelId, monthModelMap);
			} else {
				modelsMap.get(modelId)
						.put(month,
								new Model(modelId, month, constant,
										variablesCollection));
			}
		}

	}

	private void addModel(DBObject model, String variableName,
			List<Integer> modelIds) {
		modelIds.add(Integer.valueOf(model.get("modelId").toString()));
		variableModelsMap.put(variableName.toUpperCase(), modelIds);
	}

	@Override
	public void execute(Tuple input) {
		logger.debug("The time it enters inside Strategy Bolt execute method"
				+ System.currentTimeMillis());
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
		String l_id = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}

		// 2) Create map of new changes from the input
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));

		// 3) Find all models affected by the changes
		Set<Integer> modelIdList = new HashSet<Integer>();
		for (String changedVariable : newChangesVarValueMap.keySet()) {
			List<Integer> models = variableModelsMap.get(changedVariable);
			for (Integer modelId : models) {
				modelIdList.add(modelId);
			}
		}

		// 4) Find all variables for models
		// Create query
		BasicDBObject variableFilterDBO = new BasicDBObject("_id", 0);
		for (Integer modId : modelIdList) {
			int month;
			if (modelsMap.get(modId).containsKey(0)) {
				month = 0;
			} else {
				month = Calendar.getInstance().get(Calendar.MONTH) + 1;
			}

			for (Variable var : modelsMap.get(modId).get(month).getVariables()) {
				variableFilterDBO.append(var.getVid(), 1);
			}
		}

		// 5) Create a map of variable values, fetched from from memberVariables
		DBObject mbrVariables = memberVariablesCollection.findOne(
				new BasicDBObject("l_id", l_id), variableFilterDBO);
		logger.info(" ### SCORING BOLT FOUND VARIABLES");
		if (mbrVariables == null) {
			logger.info(" ### SCORING BOLT COULD NOT FIND MEMBER VARIABLES");
			return;
		}

		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Object> memberVariablesMap = new HashMap<String, Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while (mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if (!key.equals("l_id") && !key.equals("_id")) {
				memberVariablesMap.put(variableVidToNameMap.get(key)
						.toUpperCase(), mbrVariables.get(key));

			}
		}

		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch
		// all for the member since upsert needs it too
		DBObject changedMbrVariables = changedVariablesCollection
				.findOne(new BasicDBObject("l_id", l_id));

		// 6.1) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE
		// (CHANGE CLASS) and store in allChanges
		Map<String, Change> allChanges = new HashMap<String, Change>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if (changedMbrVariables != null && changedMbrVariables.keySet() != null) {
			for (String key : changedMbrVariables.keySet()) {
				// skip expired changes
				if ("_id".equals(key) || "l_id".equals(key)) {
					continue;
				}
				try {
					// v = VID : e = expiration date : f = effective date
					if (((DBObject) changedMbrVariables.get(key)).get("e") != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get("f") != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get("v") != null
							&& !simpleDateFormat.parse(
									((DBObject) changedMbrVariables.get(key))
											.get("e").toString()).after(
									new Date())) {
						allChanges
								.put(key.toUpperCase(),
										new Change(
												key.toUpperCase(),
												((DBObject) changedMbrVariables
														.get(key)).get("v"),
												simpleDateFormat
														.parse(((DBObject) changedMbrVariables
																.get(key)).get(
																"e").toString()),
												simpleDateFormat
														.parse(((DBObject) changedMbrVariables
																.get(key)).get(
																"f").toString())));
					} else {
						logger.error("Got a null value for "
								+ changedMbrVariables.get(key));
					}
				} catch (ParseException e) {
					logger.error(e.getMessage(), e);
					outputCollector.fail(input);
				}
			}
		}

		// 6) For each variable in new changes, execute strategy and store in
		// allChanges
		// allChanges will contain newChanges and the changedMemberVariables
		Map<String, Change> newChanges = new HashMap<String, Change>();
		for (String variableName : newChangesVarValueMap.keySet()) {
			if (variableModelsMap.containsKey(variableName)) {
				variableName = variableName.toUpperCase();
				if (variableNameToStrategyMap.get(variableName) == null) {
					logger.info(" ~~~ DID NOT FIND VARIABLE: ");
					continue;
				}

				RealTimeScoringContext context = new RealTimeScoringContext();
				context.setValue(newChangesVarValueMap.get(variableName));
				context.setPreviousValue(0);

				if (variableNameToStrategyMap.get(variableName).equals(
						"NONE")) {
					continue;
				}

				Strategy strategy = StrategyMapper.getInstance().getStrategy(
						variableNameToStrategyMap.get(variableName));
				// If this member had a changed variable
				if (allChanges.containsKey(variableName)) {
					context.setPreviousValue(allChanges.get(variableName)
							.getValue());
				}
				// else get it from memberVariablesMap
				else {
					if (memberVariablesMap.get(variableName) != null) {
						context.setPreviousValue(memberVariablesMap
								.get(variableName));
					}
				}
				logger.debug(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
				Change executedValue = strategy.execute(context);
				//TODO: Verify this 
				//newChanges.put(variableName, executedValue);//store in newChanges so that iterate over it and upsert
				allChanges.put(variableName, executedValue);//We do not need newChanges. We upsert allChanges
			}
		}
		// newChanges has both changedMemberVariables and changes

		// 8) Rescore - arbitrate between all changes and memberVariables

		// Score each model in a loop
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		for (Integer modelId : modelIdList) {//Score and emit for all modelIds before mongo inserts
			// recalculate score for model

			// System.out.println(" ### SCORING MODEL ID: " + modelId);
			double baseScore = calcBaseScore(memberVariablesMap, allChanges,modelId);
			double newScore;

			if (baseScore <= -100) {
				newScore = 0;
			} else if (baseScore >= 35) {
				newScore = 1;
			} else {
				// newScore = 1/(1+ Math.exp(-1*( baseScore ))) * 1000;
				newScore = Math.exp(baseScore) / (1 + Math.exp(baseScore));
			}
		
			logger.info("new score before boost var: " + newScore);
			//TODO: Add boosting logic here
			
			// 9) Emit the new score
			double oldScore = 0;
			//TODO: Why are we even emiting oldScore if its always 0
			List<Object> listToEmit = new ArrayList<Object>();
	    	listToEmit.add(l_id);
	    	listToEmit.add(oldScore);
	    	listToEmit.add(newScore);
	    	listToEmit.add(modelId);
	    	listToEmit.add(source);
	    	listToEmit.add(messageID);
	    	modelIdScoreMap.put(modelId, newScore);
	    	//logger.info(" ### SCORING BOLT EMITTING: " + listToEmit);
	    	System.out.println("Scored" + modelId + " " + newScore);
	    	logger.debug("The time spent for creating scores..... "+System.currentTimeMillis()+" and the message ID is ..."+messageID);
	    	this.outputCollector.emit(listToEmit);
		}
		BasicDBObject updateRec = new BasicDBObject();
		for (Integer modelId: modelIdList){
			// 10) Write changedMemberScores with expiry
			// FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR
				// CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
				Date minDate = null;
				Date maxDate = null;
				//TODO: Even if variable is in multiple models, the min and max date are same, so we need not loop through after finding one result 
				for (String key : allChanges.keySet()) {
					// variable models map
					if (variableModelsMap.get(key).contains(modelId)) {
						if (minDate == null) {
							minDate = allChanges.get(key).getExpirationDate();
							maxDate = allChanges.get(key).getExpirationDate();
						} else {
							if (allChanges.get(key).getExpirationDate()
									.before(minDate)) {
								minDate = allChanges.get(key).getExpirationDate();
							}
							if (allChanges.get(key).getExpirationDate()
									.after(maxDate)) {
								maxDate = allChanges.get(key).getExpirationDate();
							}
						}
					}
				}
				
				// IF THE MODEL IS MONTH SPECIFIC AND THE MIN/MAX DATE IS AFTER THE
				// END OF THE MONTH SET TO THE LAST DAY OF THIS MONTH
				if (modelsMap.containsKey(modelId)
						&& modelsMap.get(modelId).containsKey(
								Calendar.getInstance().get(Calendar.MONTH) + 1)) {
					Calendar calendar = Calendar.getInstance();
					calendar.set(Calendar.DATE,
							calendar.getActualMaximum(Calendar.DATE));
					Date lastDayOfMonth = calendar.getTime();

					if (minDate.after(lastDayOfMonth)) {
						minDate = lastDayOfMonth;
						maxDate = lastDayOfMonth;
					} else if (maxDate.after(lastDayOfMonth)) {
						maxDate = lastDayOfMonth;
					}
				}

				// APPEND CHANGED SCORE AND MIN/MAX EXPIRATION DATES TO DOCUMENT FOR
				// UPDATE
				updateRec.append(
						modelId.toString(),
						new BasicDBObject().append("s", modelIdScoreMap.get(modelId))
								.append("minEx", minDate != null ? simpleDateFormat.format(minDate):null)
								.append("maxEx", maxDate != null ? simpleDateFormat.format(maxDate):null)
								.append("f", simpleDateFormat.format(new Date())));

			
				if (updateRec != null) {
					changedMemberScoresCollection.update(
							new BasicDBObject("l_id", l_id), new BasicDBObject("$set",
									updateRec), true, false);

				}
				
				// 11) Write changedMemberVariables with expiry
		        if(!allChanges.isEmpty()){
//		            System.out.println(" ~~~ CHANGES: " + newChanges );
		            
					Iterator<Entry<String, Change>> newChangesIter = newChanges.entrySet().iterator();
					BasicDBObject newDocument = new BasicDBObject();
				    while (newChangesIter.hasNext()) {
				        Map.Entry<String, Change> pairsVarValue = (Map.Entry<String, Change>)newChangesIter.next();
				    	String varVid =  variableNameToVidMap.get(pairsVarValue.getKey().toString().toUpperCase());
						Object val = pairsVarValue.getValue().value;
						newDocument.append(varVid, new BasicDBObject().append("v", val).append("e", pairsVarValue.getValue().getExpirationDateAsString()).append("f", pairsVarValue.getValue().getEffectiveDateAsString()));
				    	
				    //	allChanges.put(varVid, new Change(varVid, val, pairsVarValue.getValue().expirationDate));
				    }

				    BasicDBObject searchQuery = new BasicDBObject().append("l_id", l_id);
				    
				    logger.trace(" ~~~ DOCUMENT TO INSERT:");
				    logger.debug(newDocument.toString());
				    logger.trace(" ~~~ END DOCUMENT");
				    
				    //upsert document
				    changedVariablesCollection.update(searchQuery, new BasicDBObject("$set", newDocument), true, false);
			
		        }
		}


	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","oldScore","newScore","model","source","messageID"));

	}
	
	double calcBaseScore(Map<String, Object> mbrVarMap,
			Map<String, Change> varChangeMap, int modelId) {

		Model model = new Model();
		
		if (modelsMap.get(modelId) != null && modelsMap.get(modelId).containsKey(0)) {
			model = modelsMap.get(modelId).get(0);
		} else if (modelsMap.get(modelId) != null && modelsMap.get(modelId).containsKey(
				Calendar.getInstance().get(Calendar.MONTH) + 1)) {
			model = modelsMap.get(modelId).get(
					Calendar.getInstance().get(Calendar.MONTH) + 1);
		} else {
			return 0;
		}

		double val = (Double) model.getConstant();

		for (Variable variable : model.getVariables()) {
			if (variable.getName() != null
					&& mbrVarMap.get(variable.getName().toUpperCase()) != null) {
				if (mbrVarMap.get(variable.getName().toUpperCase()) instanceof Integer) {
					val = val
							+ ((Integer) calculateVariableValue(mbrVarMap,
									variable, varChangeMap, "Integer") * variable
									.getCoefficient());
				} else if (mbrVarMap.get(variable.getName().toUpperCase()) instanceof Double) {
					val = val
							+ ((Double) calculateVariableValue(mbrVarMap,
									variable, varChangeMap, "Double") * variable
									.getCoefficient());
				}
			} else if (variable.getName() != null
					&& varChangeMap.get(variable.getName().toUpperCase()) != null) {
				if (varChangeMap.get(variable.getName().toUpperCase())
						.getValue() instanceof Integer) {
					val = val
							+ ((Integer) calculateVariableValue(mbrVarMap,
									variable, varChangeMap, "Integer") * variable
									.getCoefficient());
				} else if (varChangeMap.get(variable.getName().toUpperCase())
						.getValue() instanceof Double) {
					val = val
							+ ((Double) calculateVariableValue(mbrVarMap,
									variable, varChangeMap, "Double") * variable
									.getCoefficient());
				}

			} else {
				continue;
			}
		}
		// System.out.println(" base value: " + val);
		return val;
	}
	private Object calculateVariableValue(Map<String, Object> mbrVarMap,
			Variable var, Map<String, Change> changes, String dataType) {
		Object changedValue = null;
		if (var != null) {
			if (changes.containsKey(var.getName().toUpperCase())) {
				changedValue = changes.get(var.getName().toUpperCase())
						.getValue();
				// System.out.println(" ### changed variable: " +
				// var.getName().toUpperCase() + "  value: " + changedValue);
			}
			if (changedValue == null) {
				changedValue = mbrVarMap.get(var.getName().toUpperCase());
				if (changedValue == null) {
					changedValue = 0;
				}
			} else {
				if (dataType.equals("Integer")) {
					// changedValue=Integer.parseInt(changedValue.toString());
					changedValue = (int) Math.round(Double.valueOf(changedValue
							.toString()));
				} else {
					changedValue = Double.parseDouble(changedValue.toString());
				}
			}
		} else {
			return 0;
		}
		return changedValue;
	}


}
