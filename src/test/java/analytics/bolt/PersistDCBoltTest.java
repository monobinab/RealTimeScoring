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

public class PersistDCBoltTest {

	static Map<String,String> conf;
	MemberDCDao memberDCDao;
	String date;
	@Before
	public void setUp() throws Exception {
		System.setProperty(MongoNameConstants.IS_PROD, "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
		//Below line ensures an empty DB rather than reusing a DB with values in it
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
		int strength = 12;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(map.get(date),Integer.toString(strength));
	}
	
	@Test
	public void testPersistWhenDateNotExist() throws JSONException{
		int strength = 12;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		JSONObject json = new JSONObject();
		json.put("d", "2014-12-15");
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		
		json.put("d", "2014-12-16");
		dcObject.put("DC_Appliance", "13");
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Appliance", l_id);
		assertEquals(map.get("2014-12-15"), "12");
		assertEquals(map.get("2014-12-16"), "13");
	}
	
	@Test
	public void testPersistWhenCategoryNotExist() throws JSONException{
		int strength = 12;
		String l_id = "=ASDDDDDDDDDDDDDDDDDDD";
		System.out.println(memberDCDao.getDateStrengthMap("DC_Appliance", l_id));
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put("DC_Appliance", strength);
		json.put("dc", dcObject);
		memberDCDao.addDateDC(l_id, json.toString());
		json.put("d", "2014-12-16");
		dcObject.remove("DC_Appliance");
		dcObject.put("DC_Beverage", 13);
		json.put("dc", dcObject);
		System.out.println(json.toString());
		memberDCDao.addDateDC(l_id, json.toString());
		
		Map<String, String> map = memberDCDao.getDateStrengthMap("DC_Beverage", l_id);
		System.out.println(map);
		//TODO: strength val is always double from parsingbolt, please change it to double in tests
		assertEquals(map.get("2014-12-16"), "13.0");
	}

}
