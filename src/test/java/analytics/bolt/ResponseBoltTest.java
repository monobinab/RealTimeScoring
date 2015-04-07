/*package analytics.bolt;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberDCDao;
import analytics.util.objects.TagMetadata;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ResponseBoltTest {

	static Map<String,String> conf;
	MemberDCDao memberDCDao;
	ResponseBolt responseBolt;
	String date;
	static DB db;
	@Before
	public void setUp() throws Exception {
		System.setProperty(MongoNameConstants.IS_PROD, "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
        FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();
		responseBolt = new ResponseBolt("rtsapp302p.qa.ch3.s.com",6379);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		date = simpleDateFormat.format(new Date());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCallRtsAPI() throws JSONException {
		String l_id = "7081057588230760";
		String str = responseBolt.callRtsAPI(l_id);
		assertTrue(str.contains("statusCode\":\"200"));
	}
	
	
	@Test
	public void testRemoveExponentialFromXml() throws JSONException, SAXException, IOException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
		String l_id = "7081057588230760";
		String expoXmlStr = responseBolt.callRtsAPI(l_id);
		String str = responseBolt.removeExponentialFromXml(expoXmlStr);
		assertTrue(!str.contains("E-"));
	}
	
	@Test
	public void testCreateCustomXml() throws JSONException, SAXException, IOException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
		String l_id = "7081057588230760";
		String eid = "248143645";
		String custEventNm = "RTS_Moving";
		TagMetadata tagMetaData = new TagMetadata();
		tagMetaData.setPurchaseOccassion("Moving");
		String expoXmlStr = responseBolt.callRtsAPI(l_id);
		String str = responseBolt.removeExponentialFromXml(expoXmlStr);
		String custXml = responseBolt.createCustomXml(str, eid, custEventNm, tagMetaData);
		assertTrue(str.contains("<memberId>7081057588230760</memberId>"));
	}
	
	
	@AfterClass
	public static void cleanUp(){
		db.dropDatabase();
	}

}
*/