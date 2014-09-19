package analytics.util;

import java.lang.reflect.Field;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import analytics.MockOutputCollector;
import analytics.StormTestUtils;
import analytics.bolt.SocialBolt;
import analytics.util.dao.FacebookLoyaltyIdDao;
import analytics.util.dao.SocialVariableDao;
import backtype.storm.tuple.Tuple;

public class SocialBoltMockTest {

	
	@Test
	public void setVariablesAfterBoltInitialization() throws ConfigurationException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		System.setProperty("rtseprod", "test");
		//TODO:Since we have a get DB connection, do not need to use reflection to set the values unlike before, 
		//so rewrite to just set the collection values. Refer to the integration test for a sample
		
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        SocialBolt boltUnderTest = new SocialBolt();
       
        boltUnderTest.prepare(null, null, outputCollector);
        String input = "8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher";
        String expectedLid = "y2gpsDmSmaKudbyxsGUbpDeTU1Q=";
        String expectedBoostVar = "{\"BOOST_DISHWASHER_SOCIAL\":\"0.0\"}";//postive score is only considered here
        String source = "facebook";
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockTuple(input, source);
        
        //Pass the collections that need to be looked up by the bolt
        FacebookLoyaltyIdDao dao = new FacebookLoyaltyIdDao();
  	    Field fbLoyalty = FacebookLoyaltyIdDao.class.getDeclaredField("fbLoyaltyCollection");
  	    fbLoyalty.setAccessible(true);
        DBCollection fbLoyaltyCollection = (DBCollection) fbLoyalty.get(dao);
        fbLoyaltyCollection.insert(new BasicDBObject(MongoNameConstants.L_ID, expectedLid).append(MongoNameConstants.SOCIAL_ID,"1123404212"));
        fbLoyalty.set(dao, fbLoyaltyCollection);

        SocialVariableDao varDao = new SocialVariableDao();
  	    Field socialVar = SocialVariableDao.class.getDeclaredField("socialVariable");
        socialVar.setAccessible(true);
        DBCollection socialVariable = (DBCollection) socialVar.get(varDao);
        socialVariable.insert(new BasicDBObject(MongoNameConstants.SOCIAL_KEYWORD, "dishwasher").append(MongoNameConstants.SOCIAL_VARIABLE,"BOOST_DISHWASHER_SOCIAL"));
        socialVar.set(varDao, socialVariable);

        System.out.println(varDao.getVariableFromTopic("dishwasher"));
        
        Field fbCollection = SocialBolt.class.getDeclaredField("facebookLoyaltyIdDao");
        fbCollection.setAccessible(true);
        fbCollection.set(boltUnderTest, dao);
        
        Field socialVarCollection = SocialBolt.class.getDeclaredField("socialVariableDao");
        socialVarCollection.setAccessible(true);
        socialVarCollection.set(boltUnderTest, varDao);
        
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple();
        
        Assert.assertEquals(expectedLid,outputTuple.get(0));
        Assert.assertEquals(expectedBoostVar, outputTuple.get(1));
        Assert.assertEquals(source, outputTuple.get(2));
        //[y2gpsDmSmaKudbyxsGUbpDeTU1Q=, {"BOOST_DISHWASHER_FB":"0."}, FB]
	}

}
