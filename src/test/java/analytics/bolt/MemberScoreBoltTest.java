package analytics.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import analytics.MockOutputCollector;
import analytics.StormTestUtils;
import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.ListenerThread;
import analytics.util.MongoNameConstants;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MemberScoreBoltTest {
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
	public void invalidInteractionTypeIsIgnored() throws ConfigurationException, InterruptedException{		
        String input = "nIeO76q2TF8QBTjKkchXxBoGoUY=";
        String redisHost = "rtsapp301p.qa.ch3.s.com";
        int port = 6379;
        DB conn = DBConnection.getDBConnection();
		DBCollection memberZip = conn.getCollection("memberZip");
		
		memberZip.insert(new BasicDBObject(MongoNameConstants.ZIP,"11111").append(MongoNameConstants.L_ID, input));
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		//Publish to a test db in REDIS
        MemberPublishBolt boltUnderTest = new MemberPublishBolt(redisHost, port,"member_test");
   
        boltUnderTest.prepare(conf, null, outputCollector);
        Tuple tuple = StormTestUtils.mockMemberTuple(input,"unit_test_source");
        
        //initialize subscriber
        LinkedBlockingQueue<String> queue;
        JedisPool pool;
        queue = new LinkedBlockingQueue<String>(10);
        pool = new JedisPool(new JedisPoolConfig(),redisHost, port);
        ListenerThread listener = new ListenerThread(queue,pool,"member_test");
        listener.start();
        
        //call the bolt method
        boltUnderTest.execute(tuple);
        
        String ret = queue.poll();
        if(ret==null)
        {
        	Utils.sleep(1000);
        	ret = queue.poll();
        }
        Assert.assertEquals("nIeO76q2TF8QBTjKkchXxBoGoUY=,11111,unit_test_source", ret);
	}
	
}
