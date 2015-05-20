package analytics.util;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import junit.framework.Assert;
import analytics.util.objects.StrategyMapper;
import analytics.util.strategies.Strategy;
import analytics.util.strategies.StrategyBoostProductTotalCount;
import analytics.util.strategies.StrategyCountTraitDates;
import analytics.util.strategies.StrategyCountTraits;
import analytics.util.strategies.StrategyCountTransactions;
import analytics.util.strategies.StrategyDaysSinceLast;
import analytics.util.strategies.StrategyTurnOnFlag;

public class StrategyMapperTest {

	//static DB db;
	@BeforeClass
	public static void setup() throws ConfigurationException {
		/*System.setProperty("rtseprod", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();*/
		
		SystemPropertyUtility.setSystemProperty();
		DB db = SystemPropertyUtility.getDb();
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));

	}
	
	@AfterClass
	public static void cleanUp(){
		/*if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());*/
		
		SystemPropertyUtility.dropDatabase();
	}
	
	@Test
	public void testGetStrategyCountTransactions() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTransactions");
		Assert.assertTrue(s instanceof StrategyCountTransactions);
	}
	@Test
	public void testGetStrategyCountTraitDates() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTraitDates");
		Assert.assertTrue(s instanceof StrategyCountTraitDates);
	}
	@Test
	public void testGetStrategyCountTraits() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTraits");
		Assert.assertTrue(s instanceof StrategyCountTraits);
	}
	@Test
	public void testGetStrategyDaysSinceLast() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyDaysSinceLast");
		Assert.assertTrue(s instanceof StrategyDaysSinceLast);
	}
	@Test
	public void testGetStrategyTurnOnFlag() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyTurnOnFlag");
		Assert.assertTrue(s instanceof StrategyTurnOnFlag);
	}
	@Test
	public void testGetStrategyBoostProductTotalCount() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyBoostProductTotalCount");
		Assert.assertTrue(s instanceof StrategyBoostProductTotalCount);
	}
}
