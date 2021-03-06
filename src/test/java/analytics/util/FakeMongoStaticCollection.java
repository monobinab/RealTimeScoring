package analytics.util;

import java.text.ParseException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class FakeMongoStaticCollection {
	static DBCollection varColl;
	static DBCollection modeVarColl;
	static DBCollection sourceFeedColl;
	static DBCollection cpsOccasionsColl;
	static DBCollection regionalAdjFactorsColl;
	static DBCollection boostBrowseBuSubBuColl;
	static DBCollection modelBoostsColl;
	static DBCollection boostsColl;
	static DBCollection modelSeasonalConstantColl;
	static DBCollection modelSeasonalZipColl;
	static DBCollection modelSeasonalNationalColl;
	
	
	public static DBCollection getModelSeasonalZipColl() {
		return modelSeasonalZipColl;
	}
	public static void setModelSeasonalZipColl(DBCollection modelSeasonalZipColl) {
		FakeMongoStaticCollection.modelSeasonalZipColl = modelSeasonalZipColl;
	}
	public static DBCollection getModelSeasonalNationalColl() {
		return modelSeasonalNationalColl;
	}
	public static void setModelSeasonalNationalColl(
			DBCollection modelSeasonalNationalColl) {
		FakeMongoStaticCollection.modelSeasonalNationalColl = modelSeasonalNationalColl;
	}
	public static DBCollection getModelSeasonalConstantColl() {
		return modelSeasonalConstantColl;
	}
	public static void setModelSeasonalConstantColl(
			DBCollection modelSeasonalConstantColl) {
		FakeMongoStaticCollection.modelSeasonalConstantColl = modelSeasonalConstantColl;
	}
	public static DBCollection getModelBoostsColl() {
		return modelBoostsColl;
	}
	public static void setModelBoostsColl(DBCollection modelBoostsColl) {
		FakeMongoStaticCollection.modelBoostsColl = modelBoostsColl;
	}
	public static DBCollection getBoostsColl() {
		return boostsColl;
	}
	public static void setBoostsColl(DBCollection boostsColl) {
		FakeMongoStaticCollection.boostsColl = boostsColl;
	}
	public static DBCollection getBoostBrowseBuSubBuColl() {
		return boostBrowseBuSubBuColl;
	}
	public static void setBoostBrowseBuSubBuColl(DBCollection boostBrowseBuSubBuColl) {
		FakeMongoStaticCollection.boostBrowseBuSubBuColl = boostBrowseBuSubBuColl;
	}
	public static DBCollection getRegionalAdjFactorsColl() {
		return regionalAdjFactorsColl;
	}
	public static void setRegionalAdjFactorsColl(DBCollection regionalAdjFactorsColl) {
		FakeMongoStaticCollection.regionalAdjFactorsColl = regionalAdjFactorsColl;
	}
	public static DBCollection getCpsOccasionsColl() {
		return cpsOccasionsColl;
	}
	public static void setCpsOccasionsColl(DBCollection cpsOccasionsColl) {
		FakeMongoStaticCollection.cpsOccasionsColl = cpsOccasionsColl;
	}
	public DBCollection getVarColl() {
		return varColl;
	}
	public static void setVarColl(DBCollection varColl) {
		FakeMongoStaticCollection.varColl = varColl;
	}

	public static DBCollection getModeVarColl() {
		return modeVarColl;
	}
	public static void setModeVarColl(DBCollection modeVarColl) {
		FakeMongoStaticCollection.modeVarColl = modeVarColl;
	}
	public static DBCollection getSourceFeedColl() {
		return sourceFeedColl;
	}
	public static void setSourceFeedColl(DBCollection sourceFeedColl) {
		FakeMongoStaticCollection.sourceFeedColl = sourceFeedColl;
	}

	static Boolean flag = false;
	static DB db;
	public FakeMongoStaticCollection() throws ParseException{
		
	//	if(!flag == true){
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
		//varColl.insert(new BasicDBObject("name", "Boost_Syw_variable7").append("VID", 71).append("strategy","StrategySywTotalCounts"));
		varColl.insert(new BasicDBObject("name", "variable8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "variable9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "variable10").append("VID", 10).append("strategy","StrategySumSales"));
		//varColl.insert(new BasicDBObject("name", "Blackout_variable").append("VID", 11).append("strategy","StrategyBlackout"));
		varColl.insert(new BasicDBObject("name", "variable12").append("VID", 12).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable40").append("VID", 40).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable13").append("VID", 13).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "variable14").append("VID", 14).append("strategy","StrategyDCStrengthSum"));
			
		varColl.insert(new BasicDBObject("name", "S_SRS_VAR").append("VID", 16).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "S_SRS_VAR2").append("VID", 17).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "variable41").append("VID", 41).append("strategy","StrategySumSales").append("default", 731));
		varColl.insert(new BasicDBObject("name", "variable42").append("VID", 42).append("strategy","StrategySumSales"));
		varColl.insert(new BasicDBObject("name", "variable400").append("VID", 100).append("strategy","StrategyDaysSinceLast"));
		//varColl.insert(new BasicDBObject("name", "Blackout_variable2").append("VID", 18).append("strategy","StrategyBlackout"));
	
		//fake modelVariables collection
		modeVarColl = db.getCollection("modelVariables");
		BasicDBList dbList = new BasicDBList();
		dbList.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		dbList.add(new BasicDBObject("name", "variable10").append("coefficient", 0.05));
		//dbList.add(new BasicDBObject("name", "Boost_Syw_variable7").append("coefficient", 0.1).append("intercept", 0.0));
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
		dbList6.add(new BasicDBObject("name", "variable40").append("coefficient", 0.015));
		dbList6.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 65).append("modelName", "Model_Name6").append("modelDescription", "Home Appliances2").append("constant", 5).append("month", 0).append("variable", dbList6));
		
		BasicDBList dbList7 = new BasicDBList();
		dbList7.add(new BasicDBObject("name", "S_SRS_VAR").append("coefficient", 1.0));
		modeVarColl.insert(new BasicDBObject("modelId", 70).append("modelName", "Model_Name7").append("modelDescription", "Kids apparel").append("constant", 5).append("month", 0).append("variable", dbList7));
			
		BasicDBList dbList8 = new BasicDBList();
		dbList8.add(new BasicDBObject("name", "S_SRS_VAR2").append("coefficient", 0.015));
		//dbList8.add(new BasicDBObject("name", "Blackout_variable2").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 75).append("modelName", "Model_Name8").append("modelDescription", "Home Appliances2").append("constant", 5).append("month", 0).append("variable", dbList8));
		
		BasicDBList dbList9 = new BasicDBList();
		dbList9.add(new BasicDBObject("name", "variable41").append("coefficient", 0.015));
		dbList9.add(new BasicDBObject("name", "variable42").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 73).append("modelName", "Model_Name73").append("modelDescription", "Home Appliances73").append("constant", 5).append("month", 0).append("variable", dbList9));
		
		BasicDBList dbList10 = new BasicDBList();
		dbList10.add(new BasicDBObject("name", "variable400").append("coefficient", 0.1));
		modeVarColl.insert(new BasicDBObject("modelId", 100).append("modelName", "S_SCR_TEST").append("modelDescription", "Home Appliances100").append("constant", 5).append("month", 0).append("variable", dbList10));
		
		/*BasicDBList dbList10 = new BasicDBList();
		dbList10.add(new BasicDBObject("name", "variable42").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 73).append("modelName", "Model_Name73").append("modelDescription", "Home Appliances73").append("constant", 5).append("month", 0).append("variable", dbList10));*/
		
		//fake regionalFactors collection
		regionalAdjFactorsColl = db.getCollection("regionalAdjustmentFactors");
		regionalAdjFactorsColl.insert(new BasicDBObject("state", "TN").append("modelName", "Model_Name").append("modelId", "35").append("factor", "0.1"));
		
		sourceFeedColl = db.getCollection("sourceFeed");
		sourceFeedColl.insert(new BasicDBObject("testSB", "testSG").append("testInternalSearch", "testIS").append("testBROWSE", "testPR"));
	
		cpsOccasionsColl = db.getCollection("cpsOccasions");
		cpsOccasionsColl.insert(new BasicDBObject("occasionId", "7").append("occasion", "testingWeb").append("priority", 6).append("duration", 8).append("daysInHistory", "30").append("tagExpiresIn", "30"));
		
		//fake boostBrowseBuSubBu collection
		boostBrowseBuSubBuColl = db.getCollection("boostBrowseBuSubBu");
		boostBrowseBuSubBuColl.insert(new BasicDBObject("modelCode", "XYZW1").append("boost", "boost_XYZW1").append("bsb", "XYZW1"));
		boostBrowseBuSubBuColl.insert(new BasicDBObject("modelCode", "12345").append("boost", "boost_12345").append("bsb", "12345"));
		boostBrowseBuSubBuColl.insert(new BasicDBObject("modelCode", "ABCDE").append("boost", "boost_ABCDE").append("bsb", "ABCDE"));
		boostBrowseBuSubBuColl.insert(new BasicDBObject("modelCode", "EFGHI").append("boost", "boost_EFGHI").append("bsb", "EFGHI"));
		
		//fake Boosts collection
		DBCollection boostsColl = db.getCollection("Boosts");
		boostsColl.insert(new BasicDBObject("name", "Boost_Syw_variable7").append("VID", 71).append("rts_flg", 0).append("strategy","StrategySywTotalCounts"));
		boostsColl.insert(new BasicDBObject("name", "Blackout_variable").append("VID", 11).append("rts_flg", 0).append("strategy","StrategyBlackout"));
		boostsColl.insert(new BasicDBObject("name", "Blackout_variable2").append("VID", 18).append("strategy","StrategyBlackout"));
		
		//fake modelBoosts collection
		DBCollection modelBoostsColl = db.getCollection("modelBoost");
		BasicDBList dbList91 = new BasicDBList();
		dbList91.add(new BasicDBObject("name", "Boost_Syw_variable7").append("coefficient", 0.1).append("intercept", 0.0));
		modelBoostsColl.insert(new BasicDBObject("modelId", 35).append("modelName", "Model_Name").append("modelDescription", "Apparel").append("constant", 5).append("month", 0).append("variable", dbList91));
		
		BasicDBList dbList93 = new BasicDBList();
		dbList93.add(new BasicDBObject("name", "Blackout_variable").append("coefficient", 0.015).append("intercept", 0.0));
		modelBoostsColl.insert(new BasicDBObject("modelId", 46).append("modelName", "Model_Name2").append("modelDescription", "Tools").append("constant", 5).append("month", 0).append("variable", dbList93));
		
		BasicDBList dbList92 = new BasicDBList();
		dbList92.add(new BasicDBObject("name", "Blackout_variable2").append("coefficient", 0.015).append("intercept", 0.0));
		modelBoostsColl.insert(new BasicDBObject("modelId", 75).append("modelName", "Model_Name8").append("modelDescription", "Home Appliances2").append("constant", 5).append("month", 0).append("variable", dbList92));
		
		//fake modelSeasonalConstant collection
		modelSeasonalConstantColl = db.getCollection("modelSeasonalConstant");
		modelSeasonalConstantColl.insert(new BasicDBObject("modelName", "S_SCR_TEST").append("modelId", 100).append("modelCode", "modelCode").append("constant", 0.01));
	
		//fake modelSeasonalZip collection
		modelSeasonalZipColl = db.getCollection("modelSeasonalZip");
		modelSeasonalZipColl.insert(new BasicDBObject("modelName", "S_SCR_TEST" ).append("modelId", 100).append("modelCode", "modelCode").append("zip", "99000").append("f_dt", "2016-06-10").append("factor", 0.007));
		modelSeasonalZipColl.insert(new BasicDBObject("modelName", "S_SCR_TEST" ).append("modelId", 100).append("modelCode", "modelCode").append("zip", "46142").append("f_dt", "2016-06-10").append("factor", 0.00006));
		
		//fake modelSeasonalNational collection
		modelSeasonalNationalColl = db.getCollection("modelSeasonalNational");
		modelSeasonalNationalColl.insert(new BasicDBObject("modelName", "S_SCR_TEST" ).append("modelId", 100).append("modelCode", "modelCode").append("f_dt", "2016-06-10").append("factor", 0.008));
		
		setVarColl(varColl);
		setModeVarColl(modeVarColl);
		setRegionalAdjFactorsColl(regionalAdjFactorsColl);
		setSourceFeedColl(sourceFeedColl);
		setCpsOccasionsColl(cpsOccasionsColl);
		setBoostBrowseBuSubBuColl(boostBrowseBuSubBuColl);
		setBoostsColl(boostsColl);
		setModelBoostsColl(modelBoostsColl);
		setModelSeasonalZipColl(modelSeasonalZipColl);
		setModelSeasonalNationalColl(modelSeasonalNationalColl);
		setModelSeasonalConstantColl(modelSeasonalConstantColl);
		/*setMemberVarsColl(memVarColl);
		setChangedMemberVarsColl(changedMemberVar);
		*/
	//	flag = true;
//		}
	}

}
