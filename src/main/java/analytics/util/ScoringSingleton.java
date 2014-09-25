package analytics.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.ChangedMemberVariablesDao;
import analytics.util.dao.MemberVariablesDao;
import analytics.util.dao.ModelVariablesDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.Model;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.objects.StrategyMapper;
import analytics.util.objects.Variable;
import analytics.util.strategies.Strategy;

public class ScoringSingleton {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScoringSingleton.class);
	private Map<String, List<Integer>> variableModelsMap;
	private Map<Integer, Map<Integer, Model>> modelsMap;
	private Map<String, String> variableVidToNameMap;
	private Map<String, String> variableNameToVidMap;
	private Map<String, String> variableNameToStrategyMap;
	private MemberVariablesDao memberVariablesDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private ChangedMemberVariablesDao changedVariablesDao;
	private VariableDao variableDao;
	private ModelVariablesDao modelVariablesDao;
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
		LOGGER.debug("Populate variable vid map");
		variableDao = new VariableDao();
		modelVariablesDao = new ModelVariablesDao();
		changedVariablesDao = new ChangedMemberVariablesDao();
		memberVariablesDao = new MemberVariablesDao();
		changedMemberScoresDao = new ChangedMemberScoresDao(); 
		// populate the variableVidToNameMap
		variableNameToStrategyMap = new HashMap<String, String>();
		variableVidToNameMap = new HashMap<String, String>();
		variableNameToVidMap = new HashMap<String, String>();
		List<Variable> variables = variableDao.getVariables();
		for(Variable variable:variables){
			if (variable.getName() != null && variable.getVid()!= null) {
				variableVidToNameMap.put(variable.getVid(), variable.getName());
				variableNameToVidMap.put(variable.getName(), variable.getVid());
				variableNameToStrategyMap.put(variable.getName(),variable.getStrategy());
			}
		}
		
		LOGGER.debug("Populate variable models map");
		// populate the variableModelsMap
		variableModelsMap = new HashMap<String, List<Integer>>();
		// populate the variableModelsMap and modelsMap
		modelsMap = new HashMap<Integer, Map<Integer, Model>>();
		//Populate both maps
		//TODO: Refactor this so that it is a simple DAO method. Variable models map can be populated later
		modelVariablesDao.populateModelVariables(modelsMap, variableModelsMap);

	}
	
	//TODO: Replace this method. Its for backward compatibility. Bad coding
	public HashMap<String, Double> execute(String loyaltyId,
			ArrayList<String> modelIdArrayList, String source) {
		Set<Integer> modelIdList = new HashSet<Integer>();
		for(String model: modelIdArrayList){
			modelIdList.add(Integer.parseInt(model));
		}
		//Contains a VID to object mapping, not var name
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
		for(Map.Entry<Integer, Double> entry : modelIdScoreMap.entrySet()){
			modelIdStringScoreMap.put(entry.getKey().toString(), entry.getValue());
		}
		return modelIdStringScoreMap;
	}
	
	public Map<String, Object> createVariableValueMap(String loyaltyId,
			Set<Integer> modelIdList) {
		Map<String,Integer> variableFilter = new HashMap<String, Integer>();
    	for (Integer modId : modelIdList) {
			int month;
			if (modelsMap.get(modId).containsKey(0)) {
				month = 0;
			} else {
				month = Calendar.getInstance().get(Calendar.MONTH) + 1;
			}
			if(modelsMap.get(modId).get(month)!= null && modelsMap.get(modId).get(month).getVariables()!=null){
			for (String var : modelsMap.get(modId).get(month).getVariables()
					.keySet()) {
				variableFilter.put(variableNameToVidMap.get(var), 1);
			}
			}
			else{
				LOGGER.error("Unable to find the model for " + modId);
			}
		}
		return memberVariablesDao.getMemberVariablesFiltered(loyaltyId, variableFilter);

	}
	
	public Set<Integer> getModelIdList(Map<String, String> newChangesVarValueMap){
		Set<Integer> modelIdList = new HashSet<Integer>();
		if(newChangesVarValueMap==null)
			return modelIdList;
		for (String changedVariable : newChangesVarValueMap.keySet()) {
			List<Integer> models = variableModelsMap.get(changedVariable);
			if(models==null)
				continue;//next var
			for (Integer modelId : models) {
				modelIdList.add(modelId);
			}
		}
		return modelIdList;
	}
	
	public Map<String, Change> createChangedVariablesMap(String lId) {
		//This map is VID->Change
		Map<String, Change> changedMbrVariables = changedVariablesDao.getMemberVariables(lId);
		
		//Create a map from VName->Change
		Map<String, Change> allChanges = new HashMap<String, Change>();
		if (changedMbrVariables != null && changedMbrVariables.keySet() != null) {
			for (Map.Entry<String, Change> entry : changedMbrVariables.entrySet()){
				String key = entry.getKey();
				Change value = entry.getValue();
				//key is VID
				// skip expired changes
				if (value.getExpirationDate().after(new Date())){
						allChanges.put(variableVidToNameMap.get(key), value);
				} else {
					LOGGER.debug("Got an expired value for "
							+ value);
				}
			}
		}
		return allChanges;
	}
	
	/**
	 * 
	 * @param allChanges
	 * Varname -> Change
	 * @param newChangesVarValueMap
	 * VarName -> Value
	 * @param memberVariablesMap
	 * L_id -> Variables
	 * @return
	 */
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

				if ("NONE".equals(variableNameToStrategyMap.get(variableName))) {
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
					if (memberVariablesMap.get(variableNameToVidMap.get(variableName)) != null) {
						context.setPreviousValue(memberVariablesMap
								.get(variableNameToVidMap.get(variableName)));
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
		for(Map.Entry<String, Change> entry : allChanges.entrySet()){
			String ch = entry.getKey();
			Change value = entry.getValue();
			if(ch.substring(0,5).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				if(modelsMap.get(modelId).containsKey(0)) {
					if(modelsMap.get(modelId).get(0).getVariables().containsKey(ch)){
						boosts = boosts 
								+ Double.valueOf(value.getValue().toString()) 
								* modelsMap.get(modelId).get(0).getVariables().get(ch).getCoefficient();
					}
				} else {
					if(modelsMap.get(modelId).containsKey(Calendar.getInstance().get(Calendar.MONTH) + 1)) {
						boosts = boosts 
								+ Double.valueOf(value.getValue().toString()) 
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
				if ("Integer".equals(dataType)) {
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
		Map<Integer, ChangedMemberScore> updatedScores = new HashMap<Integer, ChangedMemberScore>();
		for(Integer modelId: modelIdList){
		// FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR
			// CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
			Date minDate = null;
			Date maxDate = null;
			for(Map.Entry<String, Change> entry : allChanges.entrySet()){
				String key = entry.getKey();
				Change value = entry.getValue();
				
				// variable models map
				if (variableModelsMap.get(key).contains(modelId)) {
					Date exprDate = value.getExpirationDate();
					if (minDate == null) {
						minDate = exprDate;
						maxDate = exprDate;
					} else {
						if (exprDate
								.before(minDate)) {
							minDate = exprDate;
						}
						if (exprDate
								.after(maxDate)) {
							maxDate = exprDate;
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
			updatedScores.put(modelId, new ChangedMemberScore(modelIdScoreMap.get(modelId),
					minDate != null ? simpleDateFormat.format(minDate) : null, 
					maxDate != null ? simpleDateFormat.format(maxDate) : null, 
					simpleDateFormat.format(new Date())));
		}
		if (updatedScores != null) {
			changedMemberScoresDao.upsertUpdateChangedScores(lId,updatedScores);
		}	
	}

	public void updateChangedVariables(String lId, Integer modelId,
			Map<String, Change> allChanges) {
		// 11) Write changedMemberVariables with expiry
		if (!allChanges.isEmpty()) {
			// upsert document
			changedVariablesDao.upsertUpdateChangedScores(lId, allChanges, variableNameToVidMap);
		}
				
		
	}
	
	public String getModelName(int modelId){
		int month;
		if (modelsMap.get(modelId).containsKey(0)) {
			month = 0;
		} else {
			month = Calendar.getInstance().get(Calendar.MONTH) + 1;
		}

		return modelsMap.get(modelId).get(month).getModelName();
	}
}
