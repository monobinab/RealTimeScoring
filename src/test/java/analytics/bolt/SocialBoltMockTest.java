package analytics.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.StormTestUtils;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import backtype.storm.tuple.Tuple;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class SocialBoltMockTest {

	static Map<String,String> conf;
	@BeforeClass
	public static void initializeFakeMongo(){
		System.setProperty("rtseprod", "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));		
	}
	
	@Test
	public void setVariablesAfterBoltInitialization() throws ConfigurationException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		DB conn = FakeMongo.getTestDB();
				
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        SocialBolt boltUnderTest = new SocialBolt();
       
        boltUnderTest.prepare(conf, null, outputCollector);
        String input = "8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher";
        String expectedLid = "2gpsDmSmaKudbyxsGUbpDeTU1Q=";
        String expectedBoostVar = "{\"BOOST_DISHWASHER_SOCIAL\":\"0.0\"}";//postive score is only considered here
        String source = "facebook";
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockTuple(input, source);
        
        //Pass the collections that need to be looked up by the bolt
        /* Use reflection to set if we can not get handle to DB after some refactoring again
         * FacebookLoyaltyIdDao dao = new FacebookLoyaltyIdDao();
  	    Field fbLoyalty = FacebookLoyaltyIdDao.class.getDeclaredField("fbLoyaltyCollection");
  	    fbLoyalty.setAccessible(true);
        DBCollection fbLoyaltyCollection = (DBCollection) fbLoyalty.get(dao);
        fbLoyaltyCollection.insert(new BasicDBObject(MongoNameConstants.L_ID, expectedLid).append(MongoNameConstants.SOCIAL_ID,"1123404212"));
        fbLoyalty.set(dao, fbLoyaltyCollection);
        
        Field fbCollection = SocialBolt.class.getDeclaredField("facebookLoyaltyIdDao");
        fbCollection.setAccessible(true);
        fbCollection.set(boltUnderTest, dao);
        */
		DBCollection fbLoyaltyCollection = conn.getCollection("fbLoyaltyIds");
        fbLoyaltyCollection.insert(new BasicDBObject(MongoNameConstants.L_ID, expectedLid).append(MongoNameConstants.SOCIAL_ID,"1123404212"));
        
		DBCollection socialVariable = conn.getCollection("socialVariable");
		socialVariable.insert(new BasicDBObject(MongoNameConstants.SOCIAL_KEYWORD, "dishwasher").append(MongoNameConstants.SOCIAL_VARIABLE,"BOOST_DISHWASHER_SOCIAL"));
		
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple().get("main");
        
        Assert.assertEquals(expectedLid,outputTuple.get(0));
        Assert.assertEquals(expectedBoostVar, outputTuple.get(1));
        Assert.assertEquals(source, outputTuple.get(2));
        //[y2gpsDmSmaKudbyxsGUbpDeTU1Q=, {"BOOST_DISHWASHER_FB":"0."}, FB]
	}

}
