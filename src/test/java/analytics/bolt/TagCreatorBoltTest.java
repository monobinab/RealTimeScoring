package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagCreatorBoltTest {
	//static DB db;
	DBCollection memberMDTagsWithDatesColl;
	MemberMDTags2Dao memberMDTags2Dao;
	Date dNow = new Date( );
	Date tomorrow = new Date(dNow.getTime() + (1000 * 60 * 60 * 24));
	SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd");

	@Before
	public void initialize() throws ConfigurationException {
		
		SystemPropertyUtility.setSystemProperty();
		

		// fake memberMDTags collection
		memberMDTagsWithDatesColl = SystemPropertyUtility.getDb().getCollection("memberMdTagsWithDates");
		//RTS Tags
		BasicDBList rtsTagsList = new BasicDBList();
		BasicDBObject newObj = new BasicDBObject();
		newObj.append("t", "SPFTK823600153010");
		newObj.append("f", ft.format(dNow));
		newObj.append("e", ft.format(tomorrow));
		
		BasicDBObject newObj2 = new BasicDBObject();
		newObj2.append("t", "SPGMS8");
		newObj2.append("f", ft.format(dNow));
		newObj2.append("e", ft.format(tomorrow));
		
		rtsTagsList.add(newObj);
		rtsTagsList.add(newObj2);
		
		//MDTags
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject newObj3 = new BasicDBObject();
		newObj3.append("t", "SPFTK823600153010");
		newObj3.append("f", ft.format(dNow));
		newObj3.append("e", ft.format(tomorrow));
		
		BasicDBObject newObj4 = new BasicDBObject();
		newObj4.append("t", "SPGMS823600153010");
		newObj4.append("f", ft.format(dNow));
		newObj4.append("e", ft.format(dNow));
		
		mdTagsList.add(newObj3);
		mdTagsList.add(newObj4);
		
		memberMDTagsWithDatesColl.insert(new BasicDBObject("l_id","OccassionTopologyTestingl_id")
			.append("tags", mdTagsList)
			.append("rtsTags", rtsTagsList));

		memberMDTags2Dao = new MemberMDTags2Dao();
	}

	@Test
	public void addMemberRtsTagsAlreadyExistingTest() {
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS8");
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags);
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		Assert.assertEquals(1, cursor.size());
		Assert.assertEquals(list.size(), 2);
	}

	@Test
	public void addMemberRtsTagsNewTagSameLidTest() {
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS8");
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags);
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		Assert.assertEquals(1, cursor.size());
		Assert.assertEquals(list.size(), 3);
	}

	@Test
	public void addMemberRtsTagsNewTagNewLidTest() {
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS8");
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id2", tags);
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		Assert.assertEquals(1, cursor.size());
		Assert.assertEquals(list.size(), 1);
	}

	@AfterClass
	public static void teardown() {
		SystemPropertyUtility.dropDatabase();
	}
}

