package analytics.bolt;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.SystemPropertyUtility;
import analytics.util.dao.MemberMDTagsDao;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PersistBoltOccassionTest {
	//static DB db;
	DBCollection memberMDTagsColl;
	MemberMDTagsDao memberMDTagsDao;

	@Before
	public void initialize() throws ConfigurationException {
		/*System.setProperty("rtseprod", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();*/
		
		SystemPropertyUtility.setSystemProperty();

		// fake memberMDTags collection
		memberMDTagsColl = SystemPropertyUtility.getDb().getCollection("memberMdTags");
		BasicDBList list = new BasicDBList();
		list.add("HACKS2010");

		BasicDBList list2 = new BasicDBList();
		list2.add("HACKS2010");
		list2.add("HARFS2010");

		memberMDTagsColl.insert(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id").append("tags", list));
		memberMDTagsColl.insert(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2").append("tags", list2));

		memberMDTagsDao = new MemberMDTagsDao();
	}

	@Test
	public void addMemberMDTagsTest() {
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS2010");
		memberMDTagsDao.addMemberMDTags("OccassionTopologyTestingl_id", tags);
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("tags");
		Assert.assertEquals(1, cursor.size());
		Assert.assertEquals("HALAS2010", list.get(0));
	}

	@Test
	public void addMemberMDTagsTest2() {
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS2010");
		tags.add("HACKS2010");
		memberMDTagsDao.addMemberMDTags("OccassionTopologyTestingl_id", tags);
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("tags");
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("HALAS2010", list.get(0));
		Assert.assertEquals("HACKS2010", list.get(1));
		memberMDTagsColl.remove(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
	}

	@Test
	public void addMemberMDTagsTest3() {
		List<String> tags = new ArrayList<String>();
		memberMDTagsDao.addMemberMDTags("OccassionTopologyTestingl_id", tags);
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("tags");
		Assert.assertEquals(0, list.size());
		memberMDTagsColl.remove(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
	}

	@Test
	public void addMemberMDTagsTest4() {
		List<String> tags = new ArrayList<String>();
		tags.add("HACKS2010");
		memberMDTagsDao.addMemberMDTags("OccassionTopologyTestingl_id2", tags);
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("tags");
		Assert.assertEquals(1, list.size());
		Assert.assertEquals("HACKS2010", list.get(0));
		memberMDTagsColl.remove(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2"));
	}

	@Test
	public void getMemberMDTagsTest() {
		memberMDTagsDao.getMemberMDTags("OccassionTopologyTestingl_id");
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		Assert.assertEquals(1, cursor.size());
	}

	@Test
	public void deleteMemberMDTagsTest() {
		memberMDTagsDao.deleteMemberMDTags("OccassionTopologyTestingl_id");
		DBCursor cursor = memberMDTagsColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		Assert.assertEquals(0, cursor.size());
	}

	@AfterClass
	public static void teardown() {
		/*if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());*/
		SystemPropertyUtility.dropDatabase();
	}

}
