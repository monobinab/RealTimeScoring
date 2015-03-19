package analytics.bolt;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.SystemPropertyUtility;
import analytics.util.dao.MemberDCDao;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PersistDCBoltTest {

	//static Map<String,String> stormConf;
	MemberDCDao memberDCDao;
	String date;
	//static DB db;
	@Before
	public void setUp() throws Exception {
		/*System.setProperty(MongoNameConstants.IS_PROD, "test");
		stormConf = new HashMap<String, String>();
		//stormConf.put("rtseprod", "test");
        FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();*/
		
		SystemPropertyUtility.setSystemProperty();
		
        memberDCDao = new MemberDCDao();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		date = simpleDateFormat.format(new Date());
	}

/*	@After
	public void tearDown() throws Exception {
		if(db.toString()=="FongoDB.test")
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());
	}*/

	@Test
	public void testPersistWhenMemberNotExist() throws JSONException {
		double strength = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength), map.get(date));
	}
	
	@Test
	public void testPersistWhenDateNotExist() throws JSONException{
		double strength1 = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", "2014-12-15");
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		
		double strength2 = 13.0;
		json.put("d", "2014-12-16");
		dcObject.put("DC_Appliance", strength2);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1), map.get("2014-12-15"));
		assertEquals(Double.toString(strength2), map.get("2014-12-16"));
	}
	
	@Test
	public void testPersistWhenCategoryNotExist() throws JSONException{
		double strength1 = 12.0;
		double strength2 = 13.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		json.put("d", "2014-12-16");
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Beverage", strength2);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1), map.get(date));
		
		map = memberDCDao.getDateStrengthMap("DC_Beverage", l_id);
		assertEquals(Double.toString(strength2), map.get("2014-12-16"));
		//This is because json.toString actually converts double to integer, it will be converted back 
	
	}
	
	@Test
	public void testPersistWhenCategoryExists() throws JSONException{
		double strength1 = 12.0;
		double strength2 = 13.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		json.put("d", date);
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Appliance", strength2);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1+strength2), map.get(date));
	}
	
	@Test
	public void testPersistToEmptydcList() throws JSONException{
		double strength1 = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		json.put("d", date);
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1), map.get(date));
	}
	
	@Test
	public void testPersistToEmptyDateList() throws JSONException{
		DB db = FakeMongo.getTestDB();
		memberDCDao.setDB(db);
		double strength1 = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		DBObject query = new BasicDBObject();
		query.put("l_id", l_id);
		DBCollection memberDCCollection = db.getCollection("memberDC");
		DBObject member_object = memberDCCollection.findOne(query);
		member_object.put("date", new BasicDBList());
		memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), member_object, true, false);
		json.put("d", date);
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1), map.get(date));
	}
	
	@Test
	public void testPersistToNullDateList() throws JSONException{
		DB db = FakeMongo.getTestDB();
		memberDCDao.setDB(db);
		double strength1 = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		DBObject query = new BasicDBObject();
		query.put("l_id", l_id);
		DBCollection memberDCCollection = db.getCollection("memberDC");
		DBObject member_object = memberDCCollection.findOne(query);
		member_object.removeField("date");
		memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), member_object, true, false);
		json.put("d", date);
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Appliance", strength1);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength1), map.get(date));
	}
	
	@Test
	public void testPersistSameMemberTwiceWithSameContent() throws JSONException {
		double strength = 12.0;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength), map.get(date));
		
		JSONObject dcObject1 = new JSONObject();
		JSONObject json1 = new JSONObject();
		double strength1 = 13;
		dcObject1.put("DC_Appliance", strength1);
		json1.put("d", date);
		json1.put("dc", dcObject1);
		memberDCDao.addDateDC(l_id, json1.toString());
		map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(Double.toString(strength+strength1), map.get(date));
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
