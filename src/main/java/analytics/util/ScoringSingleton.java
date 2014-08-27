/**
 * 
 */
package analytics.util;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import analytics.util.objects.Change;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

import com.mongodb.MongoClient;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ScoringSingleton {

	private static ScoringSingleton instance = null;
	private static final long serialVersionUID = 1L;

	MongoClient mongoClient;
	DB db;
	DBCollection modelVariablesCollection;
	DBCollection memberVariablesCollection;
	DBCollection memberScoreCollection;
	DBCollection variablesCollection;
	DBCollection changedVariablesCollection;
	DBCollection changedMemberScoresCollection;

	private Map<String, Collection<Integer>> variableModelsMap;
	private Map<String, String> variableVidToNameMap;
	private Map<String, String> variableNameToVidMap;
	private Map<Integer, Map<Integer, Model>> modelsMap;

	public void setModelCollection(DBCollection modelCollection) {
		this.modelVariablesCollection = modelCollection;
	}

	public void setMemberCollection(DBCollection memberCollection) {
		this.memberVariablesCollection = memberCollection;
	}

	public void setMemberScoreCollection(DBCollection memberScoreCollection) {
		this.memberScoreCollection = memberScoreCollection;
	}

	public void setVariablesCollection(DBCollection variablesCollection) {
		this.variablesCollection = variablesCollection;
	}

	/*public static void main(String[] args){
		ScoringBolt sb = new ScoringBolt();
		ArrayList<String> list = new ArrayList<String>();
		list.add(35+"");
		HashMap<String, Double> x = sb.execute("E06EMltfrFqV69PK0CO8G1xOUAQ=", list);
		System.out.println(x);
	}*/
	private ScoringSingleton(){
		try {

			mongoClient = new MongoClient(
					"trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = mongoClient.getDB("test");
		//System.out.println(" collections: " + db.getCollectionNames());
		memberVariablesCollection = db.getCollection("memberVariables");
		modelVariablesCollection = db.getCollection("modelVariables");
		memberScoreCollection = db.getCollection("memberScore");
		variablesCollection = db.getCollection("Variables");
		changedVariablesCollection = db.getCollection("changedMemberVariables");
		changedMemberScoresCollection = db.getCollection("changedMemberScores");

		// populate the variableVidToNameMap
		variableVidToNameMap = new HashMap<String, String>();
		variableNameToVidMap = new HashMap<String, String>();
		DBCursor vCursor = variablesCollection.find();
		for (DBObject variable : vCursor) {
			String variableName = ((DBObject) variable).get("name").toString()
					.toUpperCase();
			String vid = ((DBObject) variable).get("VID").toString();
			if (variableName != null && vid != null) {
				variableVidToNameMap.put(vid, variableName.toUpperCase());
				variableNameToVidMap.put(variableName.toUpperCase(), vid);
			}
		}

		// populate the variableModelsMap and modelsMap
		modelsMap = new HashMap<Integer, Map<Integer, Model>>();
		variableModelsMap = new HashMap<String, Collection<Integer>>();

		DBCursor models = modelVariablesCollection.find();
		for (DBObject model : models) {

			System.out.println("modelId: " + model.get("modelId"));
			System.out.println("month: " + model.get("month"));
			System.out.println("constant: " + model.get("constant"));

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
					Collection<Integer> modelIds = new ArrayList<Integer>();
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
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(month, new Model(modelId, month, constant,
					variablesCollection));
			modelsMap.put(modelId, monthModelMap);
		}
	}
	
	public static ScoringSingleton getInstance() {
	      if(instance == null) {
	         instance = new ScoringSingleton();
	      }
	      return instance;
	   }
	public HashMap<String, Double> execute(String loyaltyId,
			ArrayList<String> modelIdList) {

		
		// List<String> modelIdList = modelIdList;

		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList

		// 2) FETCH MEMBER VARIABLES FROM memberVariables COLLECTION
		DBObject mbrVariables = memberVariablesCollection
				.findOne(new BasicDBObject("l_id", loyaltyId));
		if (mbrVariables == null) {
			System.out
					.println(" ### SCORING BOLT COULD NOT FIND MEMBER VARIABLES");

		}
		// 3) CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Object> memberVariablesMap = new HashMap<String, Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while (mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if (!key.equals("l_id") && !key.equals("_id")) {
				memberVariablesMap.put(variableVidToNameMap.get(key)
						.toUpperCase(), mbrVariables.get(key));
					
			}
		}

		// 4) FETCH CHANGED VARIABLES FROM changedMemberVariables COLLECTION
		DBObject changedMbrVariables = changedVariablesCollection
				.findOne(new BasicDBObject("l_id", loyaltyId));
		// 5) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE
		// (CHANGE CLASS)
		Map<String, Change> allChanges = new HashMap<String, Change>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		// System.out.println(" ### CHANGED MEMBER VARIABLES: " +
		// changedMbrVariables);
		if (changedMbrVariables != null && changedMbrVariables.keySet() != null) {
			Iterator<String> collectionChangesIter = changedMbrVariables
					.keySet().iterator();

			while (collectionChangesIter.hasNext()) {
				String key = collectionChangesIter.next();
				// skip expired changes
				if ("_id".equals(key) || "l_id".equals(key)) {
					continue;
				}
				// System.out.println("   ### VARIABLE: " + key);
				// System.out.println("   ### GET VARIABLE: " +
				// changedMbrVariables.get(key));
				// System.out.println("   ### EXPIRATION: " + ((DBObject)
				// changedMbrVariables.get(key)).get("e"));

				try {
					if (((DBObject) changedMbrVariables.get(key)).get("v") != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get("e") != null
							&& ((DBObject) changedMbrVariables.get(key))
									.get("f") != null
							&& simpleDateFormat.parse(
									((DBObject) changedMbrVariables.get(key))
											.get("e").toString()).after(
									Calendar.getInstance().getTime())) {
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
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		// ////////////////////////////////////////////////////////////////////////////////
		// IF NO VARIABLE'S EXPIRATION DATE IS STILL THERE, WE HAVE TO GO ABCK
		// TO THE ORIGINAL SCORES
		HashMap<String, Double> scopreMap = new LinkedHashMap<String, Double>();
		if (allChanges == null || allChanges.isEmpty()) {
			
			for (String modelId : modelIdList) {

				DBObject mbrScores = memberScoreCollection
						.findOne(new BasicDBObject("l_id", loyaltyId));
				scopreMap.put(modelId, (Double) mbrScores.get(modelId));
			}
		}

		// System.out.println(" ### ALL CHANGES MAP: " + allChanges);

		// Score each model in a loop
		BasicDBObject updateRec = new BasicDBObject();
		for (String modelId : modelIdList) {
			// recalculate score for model

			// System.out.println(" ### SCORING MODEL ID: " + modelId);
			double baseScore = calcMbrVar(memberVariablesMap, allChanges,
					Integer.valueOf(modelId));
			double newScore;

			if (baseScore <= -100) {
				newScore = 0;
			} else if (baseScore >= 35) {
				newScore = 1;
			} else {
				// newScore = 1/(1+ Math.exp(-1*( baseScore ))) * 1000;
				newScore = Math.exp(baseScore) / (1 + Math.exp(baseScore));
			}

			
			// FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR
			// CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
			Date minDate = null;
			Date maxDate = null;
			for (String key : allChanges.keySet()) {
				// Get variable name from vid mapping and then lookup in
				// variable models map
				if (variableModelsMap.get(variableVidToNameMap.get(key))
						.contains(Integer.valueOf(modelId))) {
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
			//System.out.println(variableVidToNameMap);

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
					new BasicDBObject().append("s", newScore)
							.append("minEx", minDate != null ? simpleDateFormat.format(minDate):null)
							.append("maxEx", maxDate != null ? simpleDateFormat.format(maxDate):null)
							.append("f", simpleDateFormat.format(new Date())));

			DBObject oldScore = changedMemberScoresCollection
					.findOne(new BasicDBObject("l_id", loyaltyId));
			if (oldScore == null) {
				memberScoreCollection.findOne(new BasicDBObject("l_id", loyaltyId));
			}
			String message = new StringBuffer().append(loyaltyId).append("-")
					.append(modelId).append("-")
					.append(oldScore == null ? "0" : oldScore.get("1"))
					.append("-").append(newScore).toString();

			scopreMap.put(modelId, newScore);
			
		}
		// System.out.println(" ### UPDATE RECORD CHANGED SCORE: " + updateRec);
		if (updateRec != null) {
			changedMemberScoresCollection.update(
					new BasicDBObject("l_id", loyaltyId), new BasicDBObject("$set",
							updateRec), true, false);

		}
		return scopreMap;
	}

	

	double calcMbrVar(Map<String, Object> mbrVarMap,
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
