package analytics.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import analytics.util.objects.Change;
import analytics.util.objects.Model;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.objects.StrategyMapper;
import analytics.util.objects.Variable;
import analytics.util.strategies.Strategy;

public class ScoringSingleton {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScoringSingleton.class);
	private DB db;
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
	
	private static ScoringSingleton instance=null;
	
	public static ScoringSingleton getInstance() {
		if(instance == null) {
			synchronized (ScoringSingleton.class) {
				if(instance == null)
					instance = new ScoringSingleton();
			}
		}
		return instance;
	}
	private ScoringSingleton() {
		//Get DB connection
		try {
			db = DBConnection.getDBConnection();
		} catch (ConfigurationException e) {
			LOGGER.error("Unable to contain DB connection", e);
		}
		modelVariablesCollection = db.getCollection("modelVariables");
		memberVariablesCollection = db.getCollection("memberVariables");
		variablesCollection = db.getCollection("Variables");
		changedVariablesCollection = db.getCollection("changedMemberVariables");
		changedMemberScoresCollection = db.getCollection("changedMemberScores");

		LOGGER.debug("Populate variable vid map");
		// populate the variableVidToNameMap
		variableNameToStrategyMap = new HashMap<String, String>();
		variableVidToNameMap = new HashMap<String, String>();
		variableNameToVidMap = new HashMap<String, String>();
		
		DBCursor vCursor = variablesCollection.find();
		for (DBObject variable : vCursor) {
			String variableName = ((DBObject) variable).get(MongoNameConstants.V_NAME).toString()
					.toUpperCase();
			String vid = ((DBObject) variable).get(MongoNameConstants.V_ID).toString();
			String strategy = ((DBObject) variable).get(MongoNameConstants.V_STRATEGY).toString();
			if (variableName != null && vid != null) {
				variableVidToNameMap.put(vid, variableName.toUpperCase());
				variableNameToVidMap.put(variableName.toUpperCase(), vid);
				variableNameToStrategyMap.put(variableName.toUpperCase(),
						strategy);
			}
		}
		
		LOGGER.debug("Populate variable models map");
		// populate the variableModelsMap
		variableModelsMap = new HashMap<String, List<Integer>>();
		// populate the variableModelsMap and modelsMap
		modelsMap = new HashMap<Integer, Map<Integer, Model>>();

		DBCursor models = modelVariablesCollection.find();
		for (DBObject model : models) {
			int modelId = Integer.valueOf(model.get(MongoNameConstants.MODEL_ID).toString());
			int month = Integer.valueOf(model.get(MongoNameConstants.MONTH).toString());
			double constant = Double.valueOf(model.get(MongoNameConstants.CONSTANT).toString());

			Map<String, Variable> variablesMap = populateVariableModelsMap(
					model, modelId);
			
			if (!modelsMap.containsKey(modelId)) {
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(month, new Model(modelId, month, constant,
						variablesMap));
				modelsMap.put(modelId, monthModelMap);
			} else {
				modelsMap.get(modelId).put(month,
						new Model(modelId, month, constant, variablesMap));
			}
		}

	}
	private Map<String, Variable> populateVariableModelsMap(DBObject model,
			int modelId) {
		BasicDBList modelVariables = (BasicDBList) model.get(MongoNameConstants.VARIABLE);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		for (Object modelVariable : modelVariables) {
			String variableName = ((DBObject) modelVariable).get(MongoNameConstants.VAR_NAME)
					.toString().toUpperCase();
			Double coefficient = Double.valueOf(((DBObject) modelVariable)
					.get(MongoNameConstants.COEFFICIENT).toString());
			variablesMap.put(variableName, new Variable(variableName,
					variableNameToVidMap.get(variableName), coefficient));

			if (!variableModelsMap.containsKey(variableName)) {
				List<Integer> modelIds = new ArrayList<Integer>();
				variableModelsMap.put(variableName.toUpperCase(), modelIds);
				variableModelsMap.get(variableName.toUpperCase()).add(modelId);
			} else {
				if (!variableModelsMap.get(variableName).contains(modelId)) {
					variableModelsMap.get(variableName.toUpperCase())
							.add(modelId);
				}
			}
		}
		return variablesMap;
	}
	
	//TODO: Replace this method. Its for backward compatibility. Bad coding
	public HashMap<String, Double> execute(String loyaltyId,
			ArrayList<String> modelIdArrayList, String source) {
		Set<Integer> modelIdList = new HashSet<Integer>();
		for(String model: modelIdArrayList){
			modelIdList.add(Integer.parseInt(model));
		}
		Map<String, Object> memberVariablesMap = ScoringSingleton.getInstance().createVariableValueMap(loyaltyId, modelIdList);
		if(memberVariablesMap==null){
			LOGGER.warn("Unable to find member variables");
			return null;
					
		}
		Map<String, Change> allChanges = ScoringSingleton.getInstance().createChangedVariablesMap(loyaltyId);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		for (Integer modelId : modelIdList) {
			double score = ScoringSingleton.getInstance().calcScore(memberVariablesMap, allChanges,
					modelId);
			modelIdScoreMap.put(modelId, score);
		}
		updateChangedMemberScore(loyaltyId, modelIdList, allChanges, modelIdScoreMap);
		//TODO: This is a very bad way of defining, but a very temp fix before fixing other topologies
		HashMap<String, Double> modelIdStringScoreMap = new HashMap<String, Double>();
		for(Integer modelId: modelIdScoreMap.keySet()){
			modelIdStringScoreMap.put(modelId.toString(), modelIdScoreMap.get(modelId));
		}
		return modelIdStringScoreMap;
	}
	
	public Set<Integer> getModelIdList(Map<String, String> newChangesVarValueMap){
		Set<Integer> modelIdList = new HashSet<Integer>();
		for (String changedVariable : newChangesVarValueMap.keySet()) {
			List<Integer> models = variableModelsMap.get(changedVariable);
			for (Integer modelId : models) {
				modelIdList.add(modelId);
			}
		}
		return modelIdList;
	}
	
	public Map<String, Object> createVariableValueMap(String lId,Set<Integer> modelIdList ) {
		BasicDBObject variableFilterDBO = new BasicDBObject(MongoNameConstants.ID, 0);
		for (Integer modId : modelIdList) {
			int month;
			if (modelsMap.get(modId).containsKey(0)) {
				month = 0;
			} else {
				month = Calendar.getInstance().get(Calendar.MONTH) + 1;
			}

			for (String var : modelsMap.get(modId).get(month).getVariables()
					.keySet()) {
				variableFilterDBO.append(variableNameToVidMap.get(var), 1);
			}
		}
		
		DBObject mbrVariables = memberVariablesCollection.findOne(
				new BasicDBObject("l_id", lId), variableFilterDBO);
		LOGGER.debug(" Creating variable value map");
		if (mbrVariables == null) {
			LOGGER.info("Bolt could not find member variables for " + lId);
			return null;
		}

		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Object> memberVariablesMap = new HashMap<String, Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while (mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if (!key.equals(MongoNameConstants.L_ID) && !key.equals(MongoNameConstants.ID)) {
				memberVariablesMap.put(variableVidToNameMap.get(key)
						.toUpperCase(), mbrVariables.get(key));

			}
		}
		return memberVariablesMap;
	}
	public Map<String, Change> createChangedVariablesMap(String lId) {
		DBObject changedMbrVariables = changedVariablesCollection
				.findOne(new BasicDBObject(MongoNameConstants.L_ID, lId));

		Map<String, Change> allChanges = new HashMap<String, Change>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if (changedMbrVariables != null && changedMbrVariables.keySet() != null) {
			for (String key : changedMbrVariables.keySet()) {
				// skip expired changes
				if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key)) {
					continue;
				}
				try {
					// v = VID : e = expiration date : f = effective date
					if (((DBObject) changedMbrVariables.get(key)).get(MongoNameConstants.MV_EXPIRY_DATE) != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get(MongoNameConstants.MV_EFFECTIVE_DATE) != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get(MongoNameConstants.MV_VID) != null
							&& !simpleDateFormat.parse(
									((DBObject) changedMbrVariables.get(key))
											.get(MongoNameConstants.MV_EXPIRY_DATE).toString()).after(
									new Date())) {
						allChanges
								.put(variableVidToNameMap.get(key),
										new Change(
												key.toUpperCase(),
												((DBObject) changedMbrVariables
														.get(key)).get(MongoNameConstants.MV_VID),
												simpleDateFormat
														.parse(((DBObject) changedMbrVariables
																.get(key)).get(
																		MongoNameConstants.MV_EXPIRY_DATE).toString()),
												simpleDateFormat
														.parse(((DBObject) changedMbrVariables
																.get(key)).get(
																		MongoNameConstants.MV_EFFECTIVE_DATE).toString())));
					} else {
						LOGGER.debug("Got a null value for "
								+ changedMbrVariables.get(key));
					}
				} catch (ParseException e) {
					LOGGER.error(e.getMessage(), e);
					return allChanges;
				}
			}
		}
		return allChanges;
	}
	public Map<String, Change> executeStrategy(Map<String, Change> allChanges,
			Map<String, String> newChangesVarValueMap, Map<String, Object> memberVariablesMap) {
		for (String variableName : newChangesVarValueMap.keySet()) {
			if (variableModelsMap.containsKey(variableName)) {
				variableName = variableName.toUpperCase();
				if (variableNameToStrategyMap.get(variableName) == null) {
					LOGGER.info(" ~~~ DID NOT FIND VARIABLE: ");
					continue;
				}

				RealTimeScoringContext context = new RealTimeScoringContext();
				context.setValue(newChangesVarValueMap.get(variableName));
				context.setPreviousValue(0);

				if (variableNameToStrategyMap.get(variableName).equals("NONE")) {
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
				LOGGER.debug(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
				Change executedValue = strategy.execute(context);
				// TODO: Verify this
				// newChanges so that iterate over it and upsert
				allChanges.put(variableName, executedValue);// We do not need
															// newChanges. We
															// upsert allChanges
			}
		}
		return allChanges;
	}

	public double getBoostScore(Map<String, Change> allChanges, Integer modelId) {
		double boosts = 0;
		for(String ch:allChanges.keySet()) {
			if(ch.substring(0,5).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				if(modelsMap.get(modelId).containsKey(0)) {
					if(modelsMap.get(modelId).get(0).getVariables().containsKey(ch)){
						boosts = boosts 
								+ Double.valueOf(allChanges.get(ch).getValue().toString()) 
								* modelsMap.get(modelId).get(0).getVariables().get(ch).getCoefficient();
					}
				} else {
					if(modelsMap.get(modelId).containsKey(Calendar.getInstance().get(Calendar.MONTH) + 1)) {
						boosts = boosts 
								+ Double.valueOf(allChanges.get(ch).getValue().toString()) 
								* modelsMap.get(modelId).get(Calendar.getInstance().get(Calendar.MONTH) + 1).getVariables().get(ch).getCoefficient();
					}
				}
			}
		}

		return boosts;
	}
	
	public double calcScore(Map<String, Object> mbrVarMap,
			Map<String, Change> varChangeMap, int modelId){
		// recalculate score for model
			double baseScore = ScoringSingleton.getInstance().calcBaseScore(mbrVarMap, varChangeMap,
					modelId);
			double newScore;

			if (baseScore <= -100) {
				newScore = 0;
			} else if (baseScore >= 35) {
				newScore = 1;
			} else {
				newScore = Math.exp(baseScore) / (1 + Math.exp(baseScore));
			}
			return newScore;
	}
	
	public double calcBaseScore(Map<String, Object> mbrVarMap,
			Map<String, Change> varChangeMap, int modelId) {

		Model model = null;

		if (modelsMap.get(modelId) != null
				&& modelsMap.get(modelId).containsKey(0)) {
			model = modelsMap.get(modelId).get(0);
		} else if (modelsMap.get(modelId) != null
				&& modelsMap.get(modelId).containsKey(
						Calendar.getInstance().get(Calendar.MONTH) + 1)) {
			model = modelsMap.get(modelId).get(
					Calendar.getInstance().get(Calendar.MONTH) + 1);
		} else {
			return 0;
		}

		double val = (Double) model.getConstant();

		for (String v : model.getVariables().keySet()) {
			Variable variable = model.getVariables().get(v);
			if (variable.getName() != null
					&& mbrVarMap.get(variable.getName().toUpperCase()) != null
					&& !variable.getName().substring(0, 4).toUpperCase()
							.equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
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
		return val;
	}

	private Object calculateVariableValue(Map<String, Object> mbrVarMap,
			Variable var, Map<String, Change> changes, String dataType) {
		Object changedValue = null;
		if (var != null) {
			if (changes.containsKey(var.getName().toUpperCase())) {
				changedValue = changes.get(var.getName().toUpperCase())
						.getValue();
			}
			if (changedValue == null) {
				changedValue = mbrVarMap.get(var.getName().toUpperCase());
				if (changedValue == null) {
					changedValue = 0;
				}
			} else {
				if (dataType.equals("Integer")) {
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
	
	public void updateChangedMemberScore(String lId, Set<Integer> modelIdList, Map<String, Change> allChanges, Map<Integer,Double> modelIdScoreMap) {
		BasicDBObject updateRec = new BasicDBObject();
		for(Integer modelId: modelIdList){
		// FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR
			// CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
			Date minDate = null;
			Date maxDate = null;

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

				if (minDate!=null && minDate.after(lastDayOfMonth)) {
					minDate = lastDayOfMonth;
					maxDate = lastDayOfMonth;
				} else if (maxDate !=  null && maxDate.after(lastDayOfMonth)) {
					maxDate = lastDayOfMonth;
				}
			}
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			// APPEND CHANGED SCORE AND MIN/MAX EXPIRATION DATES TO DOCUMENT FOR
			// UPDATE
			updateRec.append(
					modelId.toString(),
					new BasicDBObject()
							.append(MongoNameConstants.CMS_SCORE, modelIdScoreMap.get(modelId))
							.append(MongoNameConstants.CMS_MIN_EXPIRY_DATE,
									minDate != null ? simpleDateFormat
											.format(minDate) : null)
							.append(MongoNameConstants.CMS_MAX_EXPIRY_DATE,
									maxDate != null ? simpleDateFormat
											.format(maxDate) : null)
							.append(MongoNameConstants.CMS_EFFECTIVE_DATE, simpleDateFormat.format(new Date())));
		}
		if (updateRec != null) {
			changedMemberScoresCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
					lId), new BasicDBObject("$set", updateRec), true,
					false);

		}	
	}
	public void updateChangedVariables(String lId, Integer modelId,
			Map<String, Change> allChanges) {
		// 11) Write changedMemberVariables with expiry
		if (!allChanges.isEmpty()) {
			Iterator<Entry<String, Change>> newChangesIter = allChanges
					.entrySet().iterator();
			BasicDBObject newDocument = new BasicDBObject();
			while (newChangesIter.hasNext()) {
				Map.Entry<String, Change> pairsVarValue = (Map.Entry<String, Change>) newChangesIter
						.next();
				String varVid = variableNameToVidMap.get(pairsVarValue
						.getKey().toString().toUpperCase());
				Object val = pairsVarValue.getValue().value;
				newDocument
						.append(varVid,
								new BasicDBObject()
										.append(MongoNameConstants.MV_VID, val)
										.append(MongoNameConstants.MV_EXPIRY_DATE,
												pairsVarValue
														.getValue()
														.getExpirationDateAsString())
										.append(MongoNameConstants.MV_EFFECTIVE_DATE,
												pairsVarValue
														.getValue()
														.getEffectiveDateAsString()));

			}

			BasicDBObject searchQuery = new BasicDBObject().append(MongoNameConstants.L_ID,
					lId);

			LOGGER.trace(" ~~~ DOCUMENT TO INSERT:");
			LOGGER.debug(newDocument.toString());
			LOGGER.trace(" ~~~ END DOCUMENT");

			// upsert document
			changedVariablesCollection.update(searchQuery,
					new BasicDBObject("$set", newDocument), true, false);

		}
				
		
	}
}
