package analytics.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class FakeMongoStaticCollection {
	static DBCollection varColl;
	static DBCollection modelVarCollection;
	public DBCollection getVarColl() {
		return varColl;
	}
	public static void setVarColl(DBCollection varColl) {
		FakeMongoStaticCollection.varColl = varColl;
	}
	public DBCollection getModelVarCollection() {
		return modelVarCollection;
	}
	public static void setModelVarCollection(DBCollection modelVarCollection) {
		FakeMongoStaticCollection.modelVarCollection = modelVarCollection;
	}
	static Boolean flag = false;
	static DB db;
	public FakeMongoStaticCollection(){
		
		if(!flag == true){
		SystemPropertyUtility.setSystemProperty();
		db = SystemPropertyUtility.getDb();
		varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "variable1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "variable2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "variable3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "variable4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "variable5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "variable6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "variable7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "Boost_Syw_variable7").append("VID", 71).append("strategy","StrategySywTotalCounts"));
		varColl.insert(new BasicDBObject("name", "variable8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "variable9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "variable10").append("VID", 10).append("strategy","StrategySumSales"));
		varColl.insert(new BasicDBObject("name", "Blackout_variable").append("VID", 11).append("strategy","StrategyBlackout"));
		varColl.insert(new BasicDBObject("name", "variable12").append("VID", 12).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable40").append("VID", 40).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable13").append("VID", 13).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "variable14").append("VID", 14).append("strategy","StrategyDCStrengthSum"));
		varColl.insert(new BasicDBObject("name", "BOOST_DC_VAR").append("VID", 15).append("strategy","StrategyDCStrengthSum"));
			
		//fake modelVariables collection
		DBCollection modeVarColl = db.getCollection("modelVariables");
		BasicDBList dbList = new BasicDBList();
		dbList.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		dbList.add(new BasicDBObject("name", "variable10").append("coefficient", 0.05));
		dbList.add(new BasicDBObject("name", "Boost_Syw_variable7").append("coefficient", 0.1).append("intercept", 0.0));
		modeVarColl.insert(new BasicDBObject("modelId", 35).append("modelName", "Model_Name").append("modelDescription", "Apparel").append("constant", 5).append("month", 0).append("variable", dbList));
		BasicDBList dbList2 = new BasicDBList();
		dbList2.add(new BasicDBObject("name", "Blackout_variable").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 46).append("modelName", "Model_Name2").append("modelDescription", "Tools").append("constant", 5).append("month", 0).append("variable", dbList2));
		BasicDBList dbList3 = new BasicDBList();
		dbList3.add(new BasicDBObject("name", "invalidVariable").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 48).append("modelName", "Model_Name3").append("modelDescription", "Home Appliances").append("constant", 5).append("month", 0).append("variable", dbList3));
		
		BasicDBList dbList4 = new BasicDBList();
		dbList4.add(new BasicDBObject("name", "variable12").append("coefficient", 0.015));
		dbList4.add(new BasicDBObject("name", "variable40").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 50).append("modelName", "Model_Name4").append("modelDescription", "Home Appliances").append("constant", 5).append("month", 0).append("variable", dbList4));
		
		BasicDBList dbList5 = new BasicDBList();
		dbList5.add(new BasicDBObject("name", "variable12").append("coefficient", 0.015));
		dbList5.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 55).append("modelName", "Model_Name5").append("modelDescription", "Refrigerator").append("constant", 5).append("month", 0).append("variable", dbList5));
		
		BasicDBList dbList6 = new BasicDBList();
		dbList6.add(new BasicDBObject("name", "BOOST_DC_VAR").append("coefficient", 0.015).append("intercept", 0.0));
		modeVarColl.insert(new BasicDBObject("modelId", 65).append("modelName", "Model_Name6").append("modelDescription", "Electronics").append("constant", 5).append("month", 0).append("variable", dbList6));
			
		setVarColl(varColl);
		setModelVarCollection(modeVarColl);
		
		flag = true;
		}
	}

}
