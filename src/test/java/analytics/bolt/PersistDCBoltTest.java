package analytics.bolt;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberDCDao;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PersistDCBoltTest {

	static Map<String,String> conf;
	MemberDCDao memberDCDao;
	String date;
	@Before
	public void setUp() throws Exception {
		System.setProperty(MongoNameConstants.IS_PROD, "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
        FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
        memberDCDao = new MemberDCDao();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		date = simpleDateFormat.format(new Date());
	}

	@After
	public void tearDown() throws Exception {
	}

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
		assertEquals(map.get(date),Double.toString(strength));
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
		assertEquals(map.get("2014-12-15"), Double.toString(strength1));
		assertEquals(map.get("2014-12-16"), Double.toString(strength2));
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
		assertEquals(map.get(date), Double.toString(strength1));
		
		map = memberDCDao.getDateStrengthMap("DC_Beverage", l_id);
		assertEquals(map.get("2014-12-16"), Double.toString(strength2));
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
		assertEquals(map.get(date), Double.toString(strength1+strength2));
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
		assertEquals(map.get(date), Double.toString(strength1));
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
		assertEquals(map.get(date), Double.toString(strength1));
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
		assertEquals(map.get(date), Double.toString(strength1));
	}

}
