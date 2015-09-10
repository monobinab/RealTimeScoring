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
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.ModelSywBoostDao;
import analytics.util.dao.ModelVariablesDao;
import analytics.util.dao.MongoDBConnectionWrapper;
import analytics.util.dao.RegionalFactorDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Boost;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.MemberRTSChanges;
import analytics.util.objects.Model;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.objects.StrategyMapper;
import analytics.util.objects.Variable;
import analytics.util.strategies.Strategy;

public class ScoringSingleton {
	private static final Logger LOGGER = LoggerFactory.getLogger(ScoringSingleton.class);
	private Map<String, List<Integer>> variableModelsMap;
	private Map<Integer, Map<Integer, Model>> modelsMap;
	private Map<String, String> variableVidToNameMap;
	private Map<String, String> variableNameToVidMap;
	private Map<String, String> variableNameToStrategyMap;
	private Map<String, Double> regionalFactorsMap;
	private MemberVariablesDao memberVariablesDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private ChangedMemberVariablesDao changedVariablesDao;
	private VariableDao variableDao;
	private ModelVariablesDao modelVariablesDao;
	private RegionalFactorDao regionalFactorDao;
	private MemberInfoDao memberInfoDao;
	
	private static ScoringSingleton instance = null;
	Set<String> modelIdsWithRegionalFactors;

	ModelSywBoostDao modelSywBoostDao;
	MemberBoostsDao memberBoostsDao;
	

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
			// Get DB connection
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
			for (Variable variable : variables) {
				if (variable.getName() != null && variable.getVid() != null) {
					variableVidToNameMap.put(variable.getVid(), variable.getName());
					variableNameToVidMap.put(variable.getName(), variable.getVid());
					variableNameToStrategyMap.put(variable.getName(), variable.getStrategy());
				}
			}

			LOGGER.debug("Populate variable models map");
			// populate the variableModelsMap
			variableModelsMap = new HashMap<String, List<Integer>>();
			// populate the variableModelsMap and modelsMap
			modelsMap = new HashMap<Integer, Map<Integer, Model>>();
			// Populate both maps
			// models map can be populated later
			modelVariablesDao.populateModelVariables(modelsMap, variableModelsMap);

			modelSywBoostDao = new ModelSywBoostDao();
			memberBoostsDao = new MemberBoostsDao();
			
			regionalFactorDao = new RegionalFactorDao();
			regionalFactorsMap = regionalFactorDao.populateRegionalFactors();
			
			memberInfoDao = new MemberInfoDao();
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
				//changedMemberScoresList null check is not needed, as calcRTSChanges method will NOT return null map
				if(!changedMemberScoresList.isEmpty()){
					updateChangedMemberScore(l_id, changedMemberScoresList, source);
					for(ChangedMemberScore changedMemScore : changedMemberScoresList){
						modelIdStringScoreMap.put(changedMemScore.getModelId(), changedMemScore.getScore());
					}
				}
			}
		}
		catch(Exception e){
			//e.printStackTrace();
			LOGGER.error("Exception occured in rescoring " + l_id + " ", e.getMessage());
		}
			return modelIdStringScoreMap;
	}

	
	public MemberRTSChanges calcRTSChanges(String lId, Map<String, String> newChangesVarValueMap, Set<Integer> modelIdsList, String source){
			
		MemberRTSChanges memberRTSChanges = null;
		try{
						
			//Find all models affected by the new incoming changes if newChangesVarValueMap is null
			if(  newChangesVarValueMap !=  null && !newChangesVarValueMap.isEmpty() ){
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
				 modelIdsList = this.getModelIdList(newChangesVarValueMap);
			}
		
			if(modelIdsList != null && !modelIdsList.isEmpty()){ 
			
				//Create a map of variable values for member, fetched from memberVariables collection
				Map<String, Object> memberVariablesMap = this.createMemberVariableValueMap(lId, modelIdsList);
			
				//checking only for null map as, with empty memberVaraiblesMap also, scoring should happen with new changes for that member
				if(memberVariablesMap != null){
					
					//create a map of non-expired variables and value fetched from changedMembervariables collection
					Map<String, Change> changedMemberVariables = this.createChangedMemberVariablesMap(lId);
				
					//For each variable in new changes, execute strategy and store in allChanges
					//empty check for newChangesVarValueMap is NOT NEEDED here as empty map will come only from topology
					//and if it is empty, modelList will be empty and the control won't be here
					Map<String, Change> allChanges = null;
					if( newChangesVarValueMap !=  null ){
						allChanges = this.executeStrategy(changedMemberVariables, newChangesVarValueMap, memberVariablesMap);
					}//if this method is called from outside of the topology, newChangesVarValueMap will be null and 
					  //thereby allChanges should be set with changedMemberVariables for scoring
					else{
						allChanges = changedMemberVariables;
					}
				
					//get the state for the memberId to get the regionalFactor for scoring
					String state = this.getState(lId);
					
					memberRTSChanges = new MemberRTSChanges();
					List<ChangedMemberScore> changedMemberScoreList = new ArrayList<ChangedMemberScore>();
					
					for (Integer modelId : modelIdsList) {
					
							double rtsScore = 0.0;
							double regionalFactor = 1.0;
							try {
								if(!isBlackOutModel(allChanges, modelId)){
								
									//recalculate score for each model
									rtsScore = this.calcScore(memberVariablesMap, allChanges, modelId);
									
									LOGGER.debug("new score before boost var: " + rtsScore);
									
									rtsScore = rtsScore + this.getBoostScore(allChanges, modelId );
									
									//get the score weighed with regionalFactor 
									if(StringUtils.isNotEmpty(state)){
										regionalFactor = this.calcRegionalFactor(modelId, state);
									}
									rtsScore = rtsScore * regionalFactor;
									if(rtsScore > 1.0)
										rtsScore = 1.0;
									
									if(rtsScore < 0.0)
										rtsScore = 0.0;
								}
									 Map<String, Date> minMaxMap = this.getMinMaxExpiry(modelId, allChanges);
									 ChangedMemberScore changedMemberScore = new ChangedMemberScore();
									 changedMemberScore.setModelId(modelId.toString());
									 changedMemberScore.setMinDate(getDateFormat(minMaxMap.get("minExpiry")));
									 changedMemberScore.setMaxDate(getDateFormat(minMaxMap.get("maxExpiry")));
									 changedMemberScore.setEffDate(getDateFormat(new Date()));
									 changedMemberScore.setScore(rtsScore);
									 changedMemberScore.setSource(source);
									 changedMemberScoreList.add(changedMemberScore);
						 }
						   catch(RealTimeScoringException e2){
							   LOGGER.error("Exception scoring modelId " + modelId +" for lId " + lId + " " + e2.getErrorMessage());
						   }
						   catch(Exception e){
							   e.printStackTrace();
							   LOGGER.error("Exception scoring modelId " + modelId +" for lId " + lId );
						   }
						}
							 memberRTSChanges.setlId(lId);
							 memberRTSChanges.setChangedMemberScoreList(changedMemberScoreList);
							 memberRTSChanges.setAllChangesMap(allChanges);
				 	}
			 	}	
			}
		catch(Exception e){
			e.printStackTrace();
			LOGGER.error("Exception scoring lId " + e.getMessage() + "cause: " + e.getCause());
			LOGGER.error(ExceptionUtils.getMessage(e) + "root cause-"+ ExceptionUtils.getRootCauseMessage(e) + ExceptionUtils.getStackTrace(e));
		}
			return memberRTSChanges;
	}

	public Map<String, Object> createMemberVariableValueMap(String loyaltyId, Set<Integer> modelIdList)  {
		Set<String> filteredVariables = new HashSet<String>();
		
		for (Integer modelId : modelIdList) {
			try{
				Map<String, Variable> variables = getModelVariables(modelId);
				if(variables == null){
					LOGGER.error("variables is null for the modelId " + modelId);
					continue;
				}
				for (String var : variables.keySet()) {
						if (variableNameToVidMap.get(var) == null) {
							LOGGER.error("VID is null for variable " + var);
						} else {
							filteredVariables.add(variableNameToVidMap.get(var));
						}
					}
				}
			catch(Exception e){
				LOGGER.error("Exception in createMemberVariableValueMap method ", e);
			}
		}
		return memberVariablesDao.getMemberVariablesFiltered(loyaltyId, filteredVariables);
	}

	public Set<Integer> getModelIdList(Map<String, String> newChangesVarValueMap) {
		Set<Integer> modelIdList = new HashSet<Integer>();
		if (newChangesVarValueMap == null)
			return modelIdList;
		for (String changedVariable : newChangesVarValueMap.keySet()) {
			List<Integer> models = variableModelsMap.get(changedVariable);
			if (models == null)
				continue;
			for (Integer modelId : models) {
				if (getMonth(modelId) != -1) 
					modelIdList.add(modelId);
			}
		}
		return modelIdList;
	}

	public Map<String, Change> createChangedMemberVariablesMap(String lId) {
		// This map is VID->Change
		Map<String, Change> changedMbrVariables = changedVariablesDao.getChangedMemberVariables(lId);

		// Create a map from VName->Change
		Map<String, Change> changedMemberVariablesMap = new HashMap<String, Change>();
	//	if (changedMbrVariables != null && changedMbrVariables.keySet() != null) {
		if (changedMbrVariables != null ) {
			for (Map.Entry<String, Change> entry : changedMbrVariables.entrySet()) {
				String key = entry.getKey();
				Change value = entry.getValue();
				// key is VID
				// skip expired changes
				if (value.getExpirationDate().after(new Date())) {
					changedMemberVariablesMap.put(variableVidToNameMap.get(key), value);
				} else {
					LOGGER.debug("Got an expired value for " + value);
				}
			}
		}
		return changedMemberVariablesMap;
	}
	
	
	public String getState(String lId){
		MemberInfo memberIfo = memberInfoDao.getMemberInfo(lId);
		String state = null;
		if(memberIfo != null && memberIfo.getState() != null)
			state = memberIfo.getState();
		return state;
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
	public Map<String, Change> executeStrategy(Map<String, Change> allChanges, Map<String, String> newChangesVarValueMap, Map<String, Object> memberVariablesMap) {
		
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
					if (!allChanges.isEmpty() && allChanges.containsKey(variableName)) {
						context.setPreviousValue(allChanges.get(variableName).getValue());
					}
					// else get it from memberVariablesMap
					else {
						if (memberVariablesMap.get(variableNameToVidMap.get(variableName)) != null) {
							context.setPreviousValue(memberVariablesMap.get(variableNameToVidMap.get(variableName)));
						}
					}
					LOGGER.debug(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
					Change executedValue = strategy.execute(context);
					allChanges.put(variableName, executedValue);
				}
			}
					return allChanges;
	}
	
	public boolean isBlackOutModel(Map<String, Change> allChanges,	Integer modelId) {
		int blackFlag = 0;
		Map<String, Variable> variableMap = getModelVariables(modelId);
		if(variableMap != null ){
			for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
				String ch = entry.getKey();
				Change value = entry.getValue();
				if(ch == null){
					LOGGER.error("variable in allChanges is null for " + ch + "modelId " + modelId);
				}
			if (ch != null && ch.startsWith(MongoNameConstants.BLACKOUT_VAR_PREFIX) && variableMap.containsKey(ch)) 
				blackFlag = Integer.valueOf(value.getValue().toString());
				if(blackFlag==1)
				{
					return true;
				}
			}
		}
	  	return false;
	}

	public double getBoostScore(Map<String, Change> allChanges, Integer modelId) {
		double boosts = 0.0;
	
		//if(modelId == null || !modelExists(modelId)){
		if(!modelExists(modelId)){
			LOGGER.warn("getBoostScore() modelId is null");
			return 0;
		}  else if (allChanges == null || allChanges.isEmpty()) {
			LOGGER.warn("getBoostScore() allChanges is null or empty");
			return 0;
		}
		
		Map<String, Variable> varMap = getModelVariables(modelId);
	
		if (varMap == null || varMap.isEmpty()) {
			LOGGER.warn("getBoostScore() variables map is null or empty, modelId: " + modelId); 
			return 0;
		}

		// create boost to send to calculateBoostValue method
		int blackFlag = 0;
		for (Map.Entry<String, Change> entry : allChanges.entrySet()) {
			String ch = entry.getKey();
			Change value = entry.getValue();
			if (ch.substring(0, MongoNameConstants.BOOST_VAR_PREFIX.length()).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
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
	
	public double calcScore(Map<String, Object> mbrVarMap, Map<String, Change> allChanges, Integer modelId) throws RealTimeScoringException {
				
		// recalculate score for model
		double baseScore = calcBaseScore(mbrVarMap, allChanges, modelId);
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
	
	public double calcBaseScore(Map<String, Object> mbrVarMap, Map<String, Change> allChanges, Integer modelId) throws RealTimeScoringException {

		if (allChanges == null || allChanges.isEmpty()) {
			throw new RealTimeScoringException("changed member variables is null");
		}
		
		Model model = getModel(modelId);
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
			} else if (mbrVarMap.containsKey(vid)) {
				variableValue = mbrVarMap.get(vid);
			} else {
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
	
	public double calcRegionalFactor(Integer modelId, String state){

		if(StringUtils.isNotEmpty(state)){
			String key = modelId+ "-" + state;
			if(regionalFactorsMap != null  && !regionalFactorsMap.isEmpty() && regionalFactorsMap.containsKey(key)){
					return  regionalFactorsMap.get(key);
				}
			}
			return 1.0;
	}
		
	public Map<String, Date> getMinMaxExpiry(Integer modelId, Map<String, Change> allChanges) {
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
		// variable models map
		//if (variableModelsMap.containsKey(key) && variableModelsMap.get(key).contains(modelId)) {
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
		int month = getMonth(modelId);
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
		if (allChanges != null && !allChanges.isEmpty()) {
			// upsert document
			changedVariablesDao.upsertUpdateChangedVariables(lId, allChanges, variableNameToVidMap);
		}
	}
	
	public Boolean modelExists(Integer modelId){
		if(modelsMap.containsKey(modelId))
			return Boolean.TRUE;
		else
			return Boolean.FALSE;
	}

	public Integer getCurrentMonth(){
		return Calendar.getInstance().get(Calendar.MONTH) + 1;
	}
	
	public Model getModel(Integer modelId){
		Model model = null;
		int month = getMonth(modelId);
		if (month != -1 && modelsMap.get(modelId) != null && modelsMap.get(modelId).get(month) != null) {
			model = modelsMap.get(modelId).get(month);
		} 
		return model;
	}
	
	public int getMonth(Integer modelId){
		int month;
		if(modelExists(modelId) && modelsMap.get(modelId).containsKey(getCurrentMonth()))
			month = getCurrentMonth();
		else if(modelExists(modelId) && modelsMap.get(modelId).containsKey(0))
			month = 0;
		else
			month = -1;
		
		return month;
	}
	
	public Map<String, Variable> getModelVariables(Integer modelId){
		Map<String, Variable> variables = null;
		int month = getMonth(modelId);
		if (month != -1 && modelsMap.get(modelId).get(month) != null && modelsMap.get(modelId).get(month).getVariables() != null) {
			variables = modelsMap.get(modelId).get(month).getVariables();
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
