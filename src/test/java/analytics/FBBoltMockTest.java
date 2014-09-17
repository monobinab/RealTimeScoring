package analytics;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import analytics.bolt.SocialBolt;
import backtype.storm.tuple.Tuple;

public class FBBoltMockTest {
	@Test
	public void basicFBTest() {
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        SocialBolt boltUnderTest = new SocialBolt();
       
        boltUnderTest.prepare(null, null, outputCollector);
        String input = "8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher";
        String expectedLid = "y2gpsDmSmaKudbyxsGUbpDeTU1Q=";
        String expectedBoostVar = "{\"BOOST_DISHWASHER_FB\":\"0.0\"}";//postive score is only considered here
        String source = "FB";
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockTuple(input);
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple();
        
        Assert.assertEquals(expectedLid, outputTuple.get(0));
        Assert.assertEquals(expectedBoostVar, outputTuple.get(1));
        Assert.assertEquals(source, outputTuple.get(2));
        //[y2gpsDmSmaKudbyxsGUbpDeTU1Q=, {"BOOST_DISHWASHER_FB":"0."}, FB]
	}

}
