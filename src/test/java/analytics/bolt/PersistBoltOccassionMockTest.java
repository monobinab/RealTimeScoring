package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.MemberTraitsDao;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.github.fakemongo.Fongo;
import com.google.gson.JsonArray;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PersistBoltOccassionMockTest {
	static Map<String,String> conf;
	static DB db;
	DBCollection memberMDTagsColl;
	MemberMDTagsDao memberMDTagsDao;

	@Before
	public void initialize() throws ConfigurationException {
		System.setProperty("rtseprod", "test");
		conf = new HashMap<String, String>();
		conf.put("rtseprod", "test");
		conf.put("nimbus.host", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();

		// fake memberMDTags collection
		memberMDTagsColl = db.getCollection("memberMdTags");
		BasicDBList list = new BasicDBList();
		list.add("HACKS2010");

		BasicDBList list2 = new BasicDBList();
		list2.add("HACKS2010");
		list2.add("HARFS2010");

		memberMDTagsColl.insert(new BasicDBObject("l_id",
				"iFTsBvgexZasfSxbq2nOtwAj4bc=").append("tags", list));
		memberMDTagsColl.insert(new BasicDBObject("l_id",
				"jnJgNqJpVI3Lt4olN7uCUH0Zcuc=").append("tags", list2));

		memberMDTagsDao = new MemberMDTagsDao();
	}

	@Test
	public void persistBoltTest(){
		DBObject dbObjBefore = memberMDTagsColl.findOne(new BasicDBObject("l_id", "iFTsBvgexZasfSxbq2nOtwAj4bc="));
		BasicDBList listBefore = (BasicDBList) dbObjBefore.get("tags");
		Assert.assertEquals("memberMdTags coll before adding incoming member tags", 1, listBefore.size());
		PersistOccasionBolt boltUnderTest =  new PersistOccasionBolt();
		List<Object> emitToPersist = new ArrayList<Object>();
		StringBuilder tags = new StringBuilder();
		tags.append("HACKS2010");
		tags.append(",");
		tags.append("HAGES2010");
		emitToPersist.add("iFTsBvgexZasfSxbq2nOtwAj4bc=");
		emitToPersist.add(tags.toString());
		Tuple tuple = StormTestUtils.mockTupleList(emitToPersist, "PurchaseOccassion");
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		boltUnderTest.prepare(conf, context, outputCollector);
		boltUnderTest.execute(tuple);
		DBObject dbObj = memberMDTagsColl.findOne(new BasicDBObject("l_id", "iFTsBvgexZasfSxbq2nOtwAj4bc="));
		BasicDBList list = (BasicDBList) dbObj.get("tags");
		Assert.assertEquals("memberMdTags coll after adding incoming member tags", 2, list.size());
		Assert.assertEquals("memberMdTags coll after adding incoming member tags", "HACKS2010", list.get(0));
		Assert.assertEquals("memberMdTags coll after adding incoming member tags", "HAGES2010", list.get(1));
		
		System.out.println(SecurityUtils.hashLoyaltyId("7081000000000001"));
	}

	@Test
	public void persistBoltTest2(){
		DBObject dbObjBefore = memberMDTagsColl.findOne(new BasicDBObject("l_id", "jnJgNqJpVI3Lt4olN7uCUH0Zcuc="));
		BasicDBList listBefore = (BasicDBList) dbObjBefore.get("tags");
		Assert.assertEquals("memberMdTags coll before adding incoming member tags", 2, listBefore.size());
		PersistOccasionBolt boltUnderTest =  new PersistOccasionBolt();
		List<Object> emitToPersist = new ArrayList<Object>();
		StringBuilder tags = new StringBuilder();
		emitToPersist.add("jnJgNqJpVI3Lt4olN7uCUH0Zcuc=");
		emitToPersist.add(tags.toString());
		Tuple tuple = StormTestUtils.mockTupleList(emitToPersist, "PurchaseOccassion");
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);

		boltUnderTest.prepare(conf, context, outputCollector);
		boltUnderTest.execute(tuple);
		DBObject dbObj = memberMDTagsColl.findOne(new BasicDBObject("l_id", "jnJgNqJpVI3Lt4olN7uCUH0Zcuc="));
		Assert.assertEquals(null, dbObj);
	}
	
	@AfterClass
	public static void cleanUp(){
		if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());
	}


}
