package analytics.bolt;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.dao.MemberMDTagsDao;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class PersistBoltOccassionMockTest {
	static DB db;
	DBCollection memberMDTagsColl;
	MemberMDTagsDao memberMDTagsDao;

	@Before
	public void initialize() throws ConfigurationException {
		System.setProperty("rtseprod", "test");
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
				"OccassionTopologyTestingl_id").append("tags", list));
		memberMDTagsColl.insert(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2").append("tags", list2));

		memberMDTagsDao = new MemberMDTagsDao();
	}

	@Test
	public void persistBoltTest(){
		
	}

}
