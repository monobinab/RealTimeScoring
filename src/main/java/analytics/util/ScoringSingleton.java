package analytics.util;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

import analytics.exception.RealTimeScoringException;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.ChangedMemberVariablesDao;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.MemberVariablesDao;
import analytics.util.dao.ModelBoostsDao;
import analytics.util.dao.ModelSeasonalConstantDao;
import analytics.util.dao.ModelSeasonalNationalDao;
import analytics.util.dao.ModelSeasonalZipDao;
import analytics.util.dao.ModelVariablesDao;
import analytics.util.dao.MongoDBConnectionWrapper;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Blackout;
import analytics.util.objects.Boost;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.MemberRTSChanges;
import analytics.util.objects.Model;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.objects.RegionalFactor;
import analytics.util.objects.StrategyMapper;
import analytics.util.objects.Variable;
import analytics.util.strategies.Strategy;

public class ScoringSingleton {
	private static final Logger LOGGER = LoggerFactory.getLogger(ScoringSingleton.class);
	private MemberVariablesDao memberVariablesDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private ChangedMemberVariablesDao changedVariablesDao;
	private VariableDao variableDao;
	private ModelVariablesDao modelVariablesDao;
	private MemberInfoDao memberInfoDao;
	
	private static ScoringSingleton instance = null;
	Set<String> modelIdsWithRegionalFactors;
	
	//boost separation
	ModelBoostsDao modelBoostsDao;
	
	//for RegionalFactor changes
	private ModelSeasonalZipDao modelSeasonalZipDao;
	private ModelSeasonalNationalDao modelSeasonalNationalDao;
	private ModelSeasonalConstantDao modelSeasonalConstantDao;

	private boolean isExecuted = Boolean.FALSE;
	
	public static ScoringSingleton getInstance() {
		if (instance == null) {
			synchronized (ScoringSingleton.class) {
				if (instance == null)
					instance = new ScoringSingleton();
			}
		}
		return instance;
	}

	private ScoringSingleton() {

		String reqSource = System.getProperty(MongoNameConstants.REQ_SOURCE);
		if(StringUtils.isEmpty(reqSource)){
			this.initDAO(null, null);
		}
	}


	public void initDAO(DB db1, DB db2){
		if(!isExecuted){
			isExecuted = Boolean.TRUE;
			if(db1 != null && db2 != null){
				MongoDBConnectionWrapper mongoDBConnectionWrapper = MongoDBConnectionWrapper.getInstance();
				if(mongoDBConnectionWrapper != null){
					mongoDBConnectionWrapper.populateDBConnection(db1, db2);
				}
			}
			variableDao = new VariableDao();
			modelVariablesDao = new ModelVariablesDao();
			changedVariablesDao = new ChangedMemberVariablesDao();
			memberVariablesDao = new MemberVariablesDao();
			changedMemberScoresDao = new ChangedMemberScoresDao();
			memberInfoDao = new MemberInfoDao();
			modelSeasonalZipDao = new ModelSeasonalZipDao();
			modelSeasonalNationalDao = new ModelSeasonalNationalDao();
			modelSeasonalConstantDao = new ModelSeasonalConstantDao();
		}
	}

	public HashMap<String, Double> execute(String l_id, ArrayList<String> modelIdArrayList, String source) {
		
		Set<Integer> modelIdList = new HashSet<Integer>();
		HashMap<String, Double> modelIdStringScoreMap =  new HashMap<String, Double>();
		try{
			for (String modelId : modelIdArrayList) {
				modelIdList.add(Integer.parseInt(modelId));
			}
			
			MemberRTSChanges memberRTSChanges = calcRTSChanges(l_id, null, modelIdList, source );
			if(memberRTSChanges != null){
				List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
				if(changedMemberScoresList != null && changedMemberScoresList.size() > 0){
					updateChangedMemberScore(l_id, changedMemberScoresList, source);
					for(ChangedMemberScore changedMemScore : changedMemberScoresList){
						modelIdStringScoreMap.put(changedMemScore.getModelId(), changedMemScore.getScore());
					}
				}
			}
		}
		catch(Exception e){
			LOGGER.error("Exception occured in rescoring " + l_id + " ", e.getMessage());
		}
			return modelIdStringScoreMap;
	}

	
	public MemberRTSChanges calcRTSChanges(String lId, Map<String, String> newChangesVarValueMap, Set<Integer> modelIdsList, String source){
		MemberRTSChanges memberRTSChanges = null;
		Map<String, String> variableNameToStrategyMap = new HashMap<String, String>();
		Map<String, String> variableNameToVidMap = new HashMap<String, String>();
		Map<String, String> variableVidToNameMap = new HashMap<String, String>();
		
		List<Variable> variables = variableDao.getAllVariables();
		for (Variable variable : variables) {
			if (variable.getName() != null && variable.getVid() != null) {
				variableNameToVidMap.put(variable.getName(), variable.getVid());
				variableNameToStrategyMap.put(variable.getName(), variable.getStrategy());
				variableVidToNameMap.put(variable.getVid(), variable.getName());
			}
		}
		
		Map<Integer, Map<Integer, Model>> modelsMap = modelVariablesDao.getAllModelVariables();
		Map<String, List<Integer>> variableModelsMap = modelVariablesDao.getAllVariableModelsMap();

		Map<String, RegionalFactor> modelSeasonalZipMap = modelSeasonalZipDao.getmodelSeasonalZipFactor();
		Map<Integer, RegionalFactor> modelSeasonalNationalMap = modelSeasonalNationalDao.getmodelSeasonalNationalFactor();
		Map<Integer, Model> modelSeasonalConstantMap = modelSeasonalConstantDao.getmodelSeasonalConstant();
	
		try{		
			//Find all models affected by the new incoming changes if newChangesVarValueMap is null
			if(newChangesVarValueMap !=  null && !newChangesVarValueMap.isEmpty()){
				Iterator<String> itr = newChangesVarValueMap.keySet().iterator();
				while(itr.hasNext()){
					String var = itr.next();
					if(!variableNameToStrategyMap.containsKey(var)){
						LOGGER.info("var NOT in variables collection " + var);
					}
					if(variableNameToStrategyMap.containsKey(var) && variableNameToStrategyMap.get(var).equalsIgnoreCase("NONE")){
						itr.remove();
					}
				}
				 modelIdsList = this.getModelIdList(newChangesVarValueMap, variableModelsMap, modelsMap);
			}
		
			if(modelIdsList != null && !modelIdsList.isEmpty()){ 
				
				long time = System.currentTimeMillis();

				//Create a map of variable values for member, fetched from memberVariables collection
				Map<String, Object> memberVariablesMap = this.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMap, modelsMap);

				LOGGER.info("MemberVariables response time for member " + lId+": " + (System.currentTimeMillis() - time));
					//create a map of non-expired variables and value fetched from changedMembervariables collection

					Map<String, Change> changedMemberVariables = this.createChangedMemberVariablesMap(lId, variableVidToNameMap);
				
					//For each variable in new changes, execute strategy and store in allChanges
					//empty check for newChangesVarValueMap is NOT NEEDED here as empty map will come only from topology
					//and if it is empty, modelList will be empty and the control won't be here
					Map<String, Change> allChanges = null;
					if( newChangesVarValueMap !=  null ){
						allChanges = this.executeStrategy(changedMemberVariables, newChangesVarValueMap, memberVariablesMap, variableNameToStrategyMap, variableNameToVidMap, variableModelsMap, modelsMap);
					}//if this method is called from outside of the topology, newChangesVarValueMap will be null and 
					  //thereby allChanges should be set with changedMemberVariables for scoring
					else{
						allChanges = changedMemberVariables;
					}
		
					memberRTSChanges = new MemberRTSChanges();
					List<ChangedMemberScore> changedMemberScoreList = new ArrayList<ChangedMemberScore>();
									
					for (Integer modelId : modelIdsList) {
						
							double rtsScore = 0.0;
							try {
								Blackout blackout = isBlackOutModel(allChanges, modelId, modelsMap);
								if(!blackout.isBlackoutFlag()){
									//recalculate score for each model
									rtsScore = this.calcScore(memberVariablesMap, allChanges, modelId, variableNameToVidMap, modelsMap);
									
									
									if(modelSeasonalConstantMap != null && modelSeasonalConstantMap.containsKey(modelId)){
										rtsScore = finalScore(rtsScore, lId, modelId, modelsMap, modelSeasonalZipMap, modelSeasonalNationalMap, modelSeasonalConstantMap );
									}
									
									LOGGER.debug("new score before boost var: " + rtsScore);
									
									rtsScore = rtsScore + this.getBoostScore(allChanges, modelId, modelsMap);
									
									if(rtsScore > 1.0)
										rtsScore = 1.0;
									
									if(rtsScore < 0.0)
										rtsScore = 0.0;
									Map<String, Date> minMaxMap = this.getMinMaxExpiry(modelId, allChanges, variableModelsMap, modelsMap);
									getPopulatedChangedMemberScore(source, changedMemberScoreList, modelId, rtsScore, minMaxMap);
								}
								else {
									String blackoutVar = blackout.getBlackoutVariables();
									if(changedMemberVariables.keySet().contains(blackoutVar) ){
										if(newChangesVarValueMap != null && (newChangesVarValueMap.containsKey(blackoutVar))){
											Map<String, Date> minMaxMap = this.getBlackoutMinMaxExpiry(blackout, allChanges);
											getPopulatedChangedMemberScore(source, changedMemberScoreList, modelId, rtsScore, minMaxMap);
											LOGGER.info("PERSIST: member " + lId +" blacked out " + "for model " + modelId +" on " + new Date());
										}
										else{
											allChanges.remove(blackoutVar);
											continue;
										}
									}
									else{
										Map<String, Date> minMaxMap = this.getBlackoutMinMaxExpiry(blackout, allChanges);
										getPopulatedChangedMemberScore(source, changedMemberScoreList, modelId, rtsScore, minMaxMap);
										LOGGER.info("PERSIST: member " + lId +" blacked out " + "for model " + modelId +" on " + new Date());
									}
								}
							}
						   catch(RealTimeScoringException e2){
							   LOGGER.error("Exception scoring modelId " + modelId +" for lId " + lId + " " + e2.getErrorMessage());
							   memberRTSChanges.setMetricsString("exception_per_model");
						   }
						   catch(Exception e){
							   e.printStackTrace();
							   LOGGER.error("Exception scoring modelId " + modelId +" for lId " + lId );
							   memberRTSChanges.setMetricsString("exception_per_model");

						   }
						}
							 memberRTSChanges.setlId(lId);
							 memberRTSChanges.setChangedMemberScoreList(changedMemberScoreList);
							 memberRTSChanges.setAllChangesMap(allChanges);
				 		}	
				else{
					memberRTSChanges = new MemberRTSChanges();
					memberRTSChanges.setMetricsString("no_vars_ofinterest");
				}
			}
		catch(Exception e){
			e.printStackTrace();
			LOGGER.error("Exception scoring lId " + e.getMessage() + "cause: " + e.getCause());
			LOGGER.error(ExceptionUtils.getMessage(e) + "root cause-"+ ExceptionUtils.getRootCauseMessage(e) + ExceptionUtils.getStackTrace(e));
			memberRTSChanges = new MemberRTSChanges();
			memberRTSChanges.setMetricsString("exception_per_member");
		}
			return memberRTSChanges;
	}
	
	@SuppressWarnings("unused")
	private double regionalFactorScore(String lId, Integer modelId, Map<Integer, Model> modelsMap, Map<Model, RegionalFactor> modelSeasonalZipMap){
		String modelName = modelsMap.get(modelId).getModelName();
		String zip = getZip(lId, modelName);
		if(zip != null){
		}
		return 0;
	}
	
	private void getPopulatedChangedMemberScore(String source,
			List<ChangedMemberScore> changedMemberScoreList, Integer modelId,
			double rtsScore, Map<String, Date> minMaxMap) {
		ChangedMemberScore changedMemberScore = new ChangedMemberScore();
		 changedMemberScore.setModelId(modelId.toString());
		 changedMemberScore.setMinDate(getDateFormat(minMaxMap.get("minExpiry")));
		 changedMemberScore.setMaxDate(getDateFormat(minMaxMap.get("maxExpiry")));
		 changedMemberScore.setEffDate(getDateFormat(new Date()));
		 changedMemberScore.setScore(rtsScore);
		 changedMemberScore.setSource(source);
		 changedMemberScoreList.add(changedMemberScore);
	}
	
	public Map<String, Object> createMemberVariableValueMap(String loyaltyId, Set<Integer> modelIdList, Map<String, String> variableNameToVidMap, Map<Integer, Map<Integer, Model>> modelsMap)  {
		Set<String> filteredVariables = new HashSet<String>();
		for (Integer modelId : modelIdList) {
			Map<String, Variable> variables = getModelVariables(modelId, modelsMap);
				if(variables == null){
					LOGGER.error("variables is null for the modelId " + modelId);
					continue;
				}
				for (String var : variables.keySet()) {
						if (!variableNameToVidMap.containsKey(var) || variableNameToVidMap.get(var) == null) {
							LOGGER.error("VID is null for variable " + var);
						} else {
							filteredVariables.add(variableNameToVidMap.get(var));
						}
				}
			}
			
		return memberVariablesDao.getMemberVariablesFiltered(loyaltyId, filteredVariables);
	}

	public Set<Integer> getModelIdList(Map<String, String> newChangesVarValueMap, Map<String, List<Integer>> variableModelsMap, Map<Integer, Map<Integer, Model>> modelsMap) {
		Set<Integer> modelIdList = new HashSet<Integer>();
		if (newChangesVarValueMap == null)
			return modelIdList;
		for (String changedVariable : newChangesVarValueMap.keySet()) {
			List<Integer> models = variableModelsMap.get(changedVariable);
			if (models == null)
				continue;
			for (Integer modelId : models) {
				if (changedVariable.startsWith(MongoNameConstants.BLACKOUT_VAR_PREFIX) ) {
					modelIdList.add(modelId);
				}
				else if(getMonth(modelId, modelsMap) != -1){
					modelIdList.add(modelId);
				}
			}
		}
		return modelIdList;
	}

	public Map<String, Change> createChangedMemberVariablesMap(String lId, Map<String, String> variableVidToNameMap) {
		// This map is VID->Change
		Map<String, Change> changedMbrVariables = changedVariablesDao.getChangedMemberVariables(lId);

		// Create a map from VName->Change
		Map<String, Change> changedMemberVariablesMap = new HashMap<String, Change>();
		if (changedMbrVariables != null ) {
			for (Map.Entry<String, Change> entry : changedMbrVariables.entrySet()) {
				String key = entry.getKey();
				Change value = entry.getValue();
				// key is VID
				// skip expired changes
				if (value.getExpirationDate().after(new Date()) && variableVidToNameMap.containsKey(key)) {
					changedMemberVariablesMap.put(variableVidToNameMap.get(key), value);
				} else {
					LOGGER.debug("Got an expired value for " + value);
				}
			}
		}
		return changedMemberVariablesMap;
	}
	
	
	public String getZip(String lId, String modelName){
		MemberInfo memberInfo = memberInfoDao.getMemberInfo(lId);
		String zip = null;
		if(memberInfo != null ){
			if(modelName.contains("S_SCR") && memberInfo.getSrs_zip() != null){
				zip = memberInfo.getSrs_zip();
			}
			else if(modelName.contains("K_SCR") && memberInfo.getKmt_zip() != null){
				zip = memberInfo.getKmt_zip();
			}
		}
		return zip;
	}

	/**
	 * 
	 * @param allChanges
	 *            Varname -> Change
	 * @param newChangesVarValueMap
	 *            VarName -> Value
	 * @param memberVariablesMap
	 *            L_id -> Variables
	 * @return
	 */
	public Map<String, Change> executeStrategy(Map<String, Change> changedMemberVariables, 
			Map<String, String> newChangesVarValueMap, 
			Map<String, Object> memberVariablesMap, 
			Map<String, String> variableNameToStrategyMap, 
			Map<String, String> variableNameToVidMap,
			Map<String, List<Integer>> variableModelsMap,
			Map<Integer, Map<Integer, Model>> modelsMap) {
			Map<String, Change> allChanges = new HashMap<String, Change>();
			allChanges.putAll(changedMemberVariables);
			for (String variableName : newChangesVarValueMap.keySet()) {
				variableName = variableName.toUpperCase();
				//if (variableModelsMap.containsKey(variableName) || variableName.equalsIgnoreCase("PURCHASE")) {
					if (variableNameToStrategyMap.get(variableName) == null) {
						LOGGER.info(" ~~~ DID NOT FIND VARIABLE IN VARIABLES COLLECTION: " + variableName);
						continue;
					}
	
					RealTimeScoringContext context = new RealTimeScoringContext();
					context.setValue(newChangesVarValueMap.get(variableName));
					// set default previous value to 0 in case the variable does not exist in memberVariables or changedMemberVariables
					// memberVariables with 0 are removed by batch job
					context.setPreviousValue(0);
	
					if ("NONE".equals(variableNameToStrategyMap.get(variableName))) {
						continue;
					}
	
					Strategy strategy = StrategyMapper.getInstance().getStrategy(variableNameToStrategyMap.get(variableName));
					if (strategy == null) {
						LOGGER.error("Unable to obtain strategy for " + variableName);
						continue;
					}
					/*
					 * If this member had a changed variable
					   allChanges at this point only contain changedMemberVariables
					   changedMemberVariables can never be null, so no need for null check 
					   ChangedMemberVarDao will return empty map NOT null 
					 */
					if (allChanges != null && !allChanges.isEmpty() && allChanges.containsKey(variableName)) {
						context.setPreviousValue(allChanges.get(variableName).getValue());
					}
					// else get it from memberVariablesMap
					else {
						if (memberVariablesMap != null && !memberVariablesMap.isEmpty() && memberVariablesMap.get(variableNameToVidMap.get(variableName)) != null) {
							context.setPreviousValue(memberVariablesMap.get(variableNameToVidMap.get(variableName)));
						}
					}
					LOGGER.debug(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
					Change executedValue = strategy.execute(context);
					allChanges.put(variableName, executedValue);
				//}
			}
					return allChanges;
	}
	
	public Map<String, Change> executeStrategyBlackout(Map<String, Change> changedMemberVariables, 
			Map<String, String> newChangesVarValueMap, 
			Map<String, Object> memberVariablesMap, 
			Map<String, String> variableNameToStrategyMap, 
			Map<String, String> variableNameToVidMap,
			Map<String, List<Integer>> variableModelsMap,
			Map<Integer, Map<Integer, Model>> modelsMap, Date transactionDate) {
			Map<String, Change> allChanges = new HashMap<String, Change>();
			if(changedMemberVariables != null && !changedMemberVariables.isEmpty()){
				allChanges.putAll(changedMemberVariables);
			}
			for (String variableName : newChangesVarValueMap.keySet()) {
				variableName = variableName.toUpperCase();
				if (variableModelsMap.containsKey(variableName)) {
					if (variableNameToStrategyMap.get(variableName) == null) {
						LOGGER.info(" ~~~ DID NOT FIND VARIABLE IN VARIABLES COLLECTION: " + variableName);
						continue;
					}
	
					RealTimeScoringContext context = new RealTimeScoringContext();
					context.setValue(newChangesVarValueMap.get(variableName));
					// set default previous value to 0 in case the variable does not exist in memberVariables or changedMemberVariables
					// memberVariables with 0 are removed by batch job
					context.setPreviousValue(0);
	
					if ("NONE".equals(variableNameToStrategyMap.get(variableName))) {
						continue;
					}
	
					Strategy strategy = StrategyMapper.getInstance().getStrategy(variableNameToStrategyMap.get(variableName));
					if (strategy == null) {
						LOGGER.error("Unable to obtain strategy for " + variableName);
						continue;
					}
					/*
					 * If this member had a changed variable
					   allChanges at this point only contain changedMemberVariables
					   changedMemberVariables can never be null, so no need for null check 
					   ChangedMemberVarDao will return empty map NOT null map
					 */
					if (allChanges != null && !allChanges.isEmpty() && allChanges.containsKey(variableName)) {
						context.setPreviousValue(allChanges.get(variableName).getValue());
					}
					// else get it from memberVariablesMap
					else {
						if (memberVariablesMap != null && memberVariablesMap.get(variableNameToVidMap.get(variableName)) != null) {
							context.setPreviousValue(memberVariablesMap.get(variableNameToVidMap.get(variableName)));
						}
					}
					LOGGER.debug(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
					//Change executedValue = strategy.execute(context);
					Change executedValue = strategy.executeBlackout(context, transactionDate);
					allChanges.put(variableName, executedValue);
				}
			}
					return allChanges;
	}
	
	public boolean isBlackOutModel2(Map<String, Change> allChanges,	Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap) {
		int blackFlag = 0;
		Map<String, Variable> variableMap = getBlackoutModelVariables(modelId, modelsMap);
		if(variableMap != null ){
			for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
				String ch = entry.getKey();
				Change value = entry.getValue();
				if(ch == null){
					LOGGER.error("variable in allChanges is null for " + ch + "modelId " + modelId);
				}
			if ( ch != null && ch.startsWith(MongoNameConstants.BLACKOUT_VAR_PREFIX) && variableMap.containsKey(ch)) 
				blackFlag = Integer.valueOf(value.getValue().toString());
				if(blackFlag==1)
				{
					return true;
				}
			}
		}
	  	return false;
	}
	
	
	public Blackout isBlackOutModel(Map<String, Change> allChanges,	Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap) {
		Blackout blackout = new Blackout();
		blackout.setBlackoutFlag(false);
		int blackFlag = 0;
		Map<String, Variable> variableMap = getBlackoutModelVariables(modelId, modelsMap);
		if(variableMap != null ){
			for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
				String ch = entry.getKey();
				Change value = entry.getValue();
				if(ch == null){
					LOGGER.error("variable in allChanges is null for " + ch + "modelId " + modelId);
				}
			if ( ch != null && ch.startsWith(MongoNameConstants.BLACKOUT_VAR_PREFIX) && variableMap.containsKey(ch)) 
				blackFlag = Integer.valueOf(value.getValue().toString());
				if(blackFlag==1)
				{
					blackout.setBlackoutFlag(true);
					blackout.setBlackoutVariables(ch);
					return blackout;
				}
			}
		}
	  	return blackout;
	}

	public double getBoostScore(Map<String, Change> allChanges, Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap) {
		double boosts = 0.0;
	
		if(!modelExists(modelId, modelsMap)){
			LOGGER.warn("getBoostScore() modelId is null");
			return 0;
		}  else if (allChanges == null || allChanges.isEmpty()) {
			LOGGER.warn("getBoostScore() allChanges is null or empty");
			return 0;
		}
		
		Map<String, Variable> varMap = getModelVariables(modelId, modelsMap);
	
		if (varMap == null || varMap.isEmpty()) {
			LOGGER.warn("getBoostScore() variables map is null or empty, modelId: " + modelId); 
			return 0;
		}

		// create boost to send to calculateBoostValue method
		int blackFlag = 0;
		for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
			String ch = entry.getKey();
			Change value = entry.getValue();
			if (ch != null && ch.substring(0, MongoNameConstants.BOOST_VAR_PREFIX.length()).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX) && varMap.containsKey(ch)) {
				Boost boost;
				if (varMap.get(ch) instanceof Boost) {
					boost = (Boost) varMap.get(ch);
					boosts = calculateBoostValue(boosts, blackFlag, value, boost);
				}
			}
		}
		return boosts;
	}
	
	private double calculateBoostValue(double boosts, int blackFlag, Change value, Boost boost) {
		return (boosts + boost.getIntercept() + Double.valueOf(value.getValue().toString()) * boost.getCoefficient()) * Math.abs(blackFlag - 1); 
																																					
	}
	
	public double calcScore(Map<String, Object> mbrVarMap, Map<String, Change> allChanges, Integer modelId, Map<String, String> variableNameToVidMap, Map<Integer, Map<Integer, Model>> modelsMap) throws RealTimeScoringException {
				
		// recalculate score for model
		double baseScore = calcBaseScore(mbrVarMap, allChanges, modelId, variableNameToVidMap, modelsMap);
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
	
	
	protected double finalScore(double newScore, String lId, Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap, Map<String, RegionalFactor> modelSeasonalZipMap, Map<Integer, RegionalFactor> modelSeasonalNationalMap, Map<Integer, Model> modelSeasonalConstantMap){
		String modelName = modelsMap.get(modelId).get(0).getModelName();
		String zip = getZip(lId, modelName);
		double seasonalConstant = 0;
		if(modelSeasonalConstantMap != null && modelSeasonalConstantMap.get(modelId) != null)
			seasonalConstant = modelSeasonalConstantMap.get(modelId).getSeasonalConstant();
		double regionalFactor = 0;
		double regionalScore = 0;
		String key = Integer.toString(modelId) + zip;
		if(zip != null && modelSeasonalZipMap != null && modelSeasonalZipMap.containsKey(key) && modelSeasonalZipMap.get(key) != null ){
				regionalFactor = modelSeasonalZipMap.get(key).getFactor();
		}
		else if(modelSeasonalNationalMap != null && modelSeasonalNationalMap.get(modelId) != null){
			regionalFactor = modelSeasonalNationalMap.get(modelId).getFactor();
		}
		if(seasonalConstant != 0 && regionalFactor != 0)
			regionalScore = regScore(newScore, regionalFactor, seasonalConstant);
		if(regionalScore != 0)
			return regionalScore;
		else
			return newScore;
	}
	
	private double regScore(double newScore, double factor, double seasonalConstant){
		double a = Math.exp(Math.log(newScore)+Math.log(1-seasonalConstant)+Math.log(factor));
		double b = Math.exp(Math.log(1-newScore)+Math.log(seasonalConstant)+Math.log(1-factor));
		double score = Math.exp(Math.log(a)-Math.log(a+b));
		return score;
	}

	public double calcBaseScore(Map<String, Object> mbrVarMap, Map<String, Change> allChanges, Integer modelId, Map<String, String> variableNameToVidMap, Map<Integer, Map<Integer, Model>> modelsMap) throws RealTimeScoringException {

		if (allChanges == null || allChanges.isEmpty()) {
			throw new RealTimeScoringException("changed member variables is null");
		}
		Model model = getModel(modelId, modelsMap);
		if(model == null)
			throw new RealTimeScoringException("Model is null for " +  modelId);
		Map<String, Variable> variableMap = model.getVariables();
		if(variableMap == null || variableMap.isEmpty())
			throw new RealTimeScoringException("variableMap is null for modelId " + modelId);
		
		double val = (Double) model.getConstant();

		for (String v : variableMap.keySet()) {
			Variable variable = variableMap.get(v);

			// if variable does not have a name or VID, skip scoring that model by throwing exception
			if (variable.getName() == null || variableNameToVidMap.get(variable.getName()) == null) {
				LOGGER.error("variable name or VID is null for variable " + variable.getName());
				throw new RealTimeScoringException("variable name or VID is null for variable " );
			}
			// if the variable is a boost variable then skip it
			if (variable.getName().substring(0, MongoNameConstants.BOOST_VAR_PREFIX.length()).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				continue;
			}

			String vid = variableNameToVidMap.get(variable.getName());

			Object variableValue;
			// if the variable has been changed then use the changed value
			// if the variable has not been changed and has a value from
			// memberVariables then use that value
			// otherwise skip it
			if (allChanges.containsKey(variable.getName())) {
				variableValue = allChanges.get(variable.getName().toUpperCase()).getValue();
			} else if (mbrVarMap != null && mbrVarMap.containsKey(vid)) {
				variableValue = mbrVarMap.get(vid);
			} 
			else if(variable.getDefaultValue() != 0){
				variableValue = variable.getDefaultValue();
			}
			else {
				continue;
			}
			
			//if val is not an integer or double, assume that variable value is zero in the model scoring
			//this is discussed and have been concluded to go with zero value for variable
			if (variableValue instanceof Integer) {
				val = val + ((Integer) variableValue * variable.getCoefficient());
			} else if (variableValue instanceof Double) {
				val = val + ((Double) variableValue * variable.getCoefficient());
			} else {
				continue;  
			}
		}
		return val;
	}
	
	public Object getDefaultValue(String varName, List<Variable> variablesList){
		Object defaultValue = null;
		if(variablesList != null && !variablesList.isEmpty()){
			for(Variable var : variablesList){
				if(var.getName().equalsIgnoreCase(varName) && var.getDefaultValue() != 0.0){
					defaultValue = var.getDefaultValue();
					break;
				}
			}
		}
		return defaultValue;
	}
	
	public double calcRegionalFactor(Integer modelId, String state, Map<String, Double> regionalFactorsMap){

		if(StringUtils.isNotEmpty(state)){
			String key = modelId+ "-" + state;
			if(regionalFactorsMap != null  && !regionalFactorsMap.isEmpty() && regionalFactorsMap.containsKey(key)){
					return  regionalFactorsMap.get(key);
				}
			}
			return 1.0;
	}
		
	public Map<String, Date> getMinMaxExpiry(Integer modelId, Map<String, Change> allChanges, Map<String, List<Integer>> variableModelsMap, Map<Integer, Map<Integer, Model>> modelsMap) {
	Date minDate = null;
	Date maxDate = null;
	StringBuilder vars = new StringBuilder();
	Map<String, Date> minMaxMap = new HashMap<String, Date>();

	for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
		String key = entry.getKey();
		Change value = entry.getValue();
		
		if (!variableModelsMap.containsKey(key)) {
			LOGGER.error("Could not find variable in variableModelsMap " + key);
			continue;
		}
		
		vars.append(key +" |");
		if (variableModelsMap.get(key).contains(modelId)) {
		
			Date exprDate = value.getExpirationDate();
			if (minDate == null) {
				minDate = exprDate;
				maxDate = exprDate;
			} else {
				if (exprDate.before(minDate)) {
					minDate = exprDate;
				}
				if (exprDate.after(maxDate)) {
					maxDate = exprDate;
				}
			}
		}
	}
	
	if(minDate == null || maxDate == null){
		LOGGER.info("NULL MIN MAX FOR MODELiD " + modelId + "WITH VARS " + vars);
	}
		// IF THE MODEL IS MONTH SPECIFIC AND THE MIN/MAX DATE IS AFTER THE
		// END OF THE MONTH SET TO THE LAST DAY OF THIS MONTH
		int month = getMonth(modelId, modelsMap);
		if (month != 0 && month != -1) {
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
			Date lastDayOfMonth = calendar.getTime();
	
			if (minDate != null && minDate.after(lastDayOfMonth)) {
				minDate = lastDayOfMonth;
			} 
		}
			minMaxMap.put("minExpiry", minDate);
			minMaxMap.put("maxExpiry", maxDate);
		
		return minMaxMap;
	}
	
	
	public Map<String, Date> getBlackoutMinMaxExpiry(Blackout blackout, Map<String, Change> allChanges) {
		Map<String, Date> minMaxMap = new HashMap<String, Date>();
		minMaxMap.put("minExpiry", allChanges.get(blackout.getBlackoutVariables()).getExpirationDate());
		minMaxMap.put("maxExpiry", allChanges.get(blackout.getBlackoutVariables()).getExpirationDate());
		return minMaxMap;
	}

	/**
	 * 
	 * @param l_id
	 * @param modelIdList
	 * @param modelIdToExpiryMap
	 * @param modelIdScoreMap
	 * @param source
	 */
		
	public void updateChangedMemberScore(String l_id, List<ChangedMemberScore> changedMemberScoresList, String source) {
		changedMemberScoresDao.upsertUpdateChangedScores(l_id, changedMemberScoresList);
	}

	public void updateChangedMemberVariables(String lId, Map<String, Change> allChanges) {
		Map<String, String> variableNameToVidMap = new HashMap<String, String>();
		List<Variable> variablesList = variableDao.getVariables();
		for (Variable variable : variablesList) {
			if (variable.getName() != null && variable.getVid() != null) {
				if(variable.getName().contains("PURCHASE")){
					LOGGER.info("PERSIST: " + variable.getName() + " gets updated for " + lId + "on " + new Date());
				}
				variableNameToVidMap.put(variable.getName(), variable.getVid());
			}
		}
		if (allChanges != null && !allChanges.isEmpty()) {
			// upsert document
			changedVariablesDao.upsertUpdateChangedVariables(lId, allChanges, variableNameToVidMap);
		}
	}
	
	public Boolean modelExists(Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap){
		if(modelsMap.containsKey(modelId))
			return Boolean.TRUE;
		else
			return Boolean.FALSE;
	}

	public Integer getCurrentMonth(){
		return Calendar.getInstance().get(Calendar.MONTH) + 1;
	}
	
	public Model getModel(Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap){
		Model model = null;
		int month = getMonth(modelId, modelsMap);
		if (month != -1 && modelsMap.get(modelId) != null && modelsMap.get(modelId).get(month) != null) {
			model = modelsMap.get(modelId).get(month);
		} 
		return model;
	}
	
	public int getMonth(Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap){
		int month;
		if(modelExists(modelId, modelsMap) && modelsMap.get(modelId).containsKey(getCurrentMonth()))
			month = getCurrentMonth();
		else if(modelExists(modelId, modelsMap) && modelsMap.get(modelId).containsKey(0))
			month = 0;
		else
			month = -1;
		
		return month;
	}
	
	public Map<String, Variable> getModelVariables(Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap){
		Map<String, Variable> variables = null;
		int month = getMonth(modelId, modelsMap);
		if (month != -1 && modelsMap.get(modelId).get(month) != null && modelsMap.get(modelId).get(month).getVariables() != null) {
			variables = modelsMap.get(modelId).get(month).getVariables();
		}
     		return variables;
	}
	
	public Map<String, Variable> getBlackoutModelVariables(Integer modelId, Map<Integer, Map<Integer, Model>> modelsMap){
		Map<String, Variable> variables = null;
		Map<Integer, Model> monthModel = modelsMap.get(modelId);
		for (Map.Entry<Integer, Model> entry : monthModel.entrySet()) {
			if(monthModel.get(entry.getKey()) != null && monthModel.get(entry.getKey()).getVariables() != null){
				variables = monthModel.get(entry.getKey()).getVariables();
			}
		}
	   		return variables;
	}
	
	public String getDateFormat(Date date){
		SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		if(date != null)
			return simpleDateFormatter.format(date);
		else
			return null;
	}

	/*public String getModelName(int modelId) {
		int month;
		if (modelsMap.get(modelId) == null)
			return "";
		if (modelsMap.get(modelId).containsKey(0)) {
			month = 0;
		} else {
			month = Calendar.getInstance().get(Calendar.MONTH) + 1;
		}

		return modelsMap.get(modelId).get(month).getModelName();
	}*/
}