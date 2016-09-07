package analytics.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.codehaus.jettison.json.JSONException;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.SystemPropertyUtility;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.Model;
import analytics.util.objects.ModelScore;

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
	TagVariableDao tagVariableDao;
	TagCreatorBolt tagCreatorBolt;
	Map<Integer, String> modelTagsMap = new HashMap<Integer, String>();
	Map<Integer, Model> modelsMap = new HashMap<Integer, Model>();
	Date dNow = new Date( );
	Date tomorrow = new Date(dNow.getTime() + (1000 * 60 * 60 * 24));
	Date eighthDay = new Date(dNow.getTime() + (1000 * 60 * 60 * 24)*8);
	SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd");
	
	HashMap<String, String> occasionDurationMap = new HashMap<String, String>();
	HashMap<String, String> occasionPriorityMap = new HashMap<String, String>();

	@Before
	public void initialize() throws ConfigurationException {
		
		SystemPropertyUtility.setSystemProperty();

		// fake memberMDTags collection
		memberMDTagsWithDatesColl = SystemPropertyUtility.getDb().getCollection("memberMdTagsWithDates");
		//RTS Tags
		BasicDBList rtsTagsList = new BasicDBList();
		BasicDBObject rtsObj = new BasicDBObject();
		rtsObj.append("t", "SPFTS823600153010");
		rtsObj.append("f", ft.format(dNow));
		rtsObj.append("e", ft.format(tomorrow));
		
		/*BasicDBObject rtsObj2 = new BasicDBObject();
		rtsObj2.append("t", "SPGMS8");
		rtsObj2.append("f", ft.format(dNow));
		rtsObj2.append("e", ft.format(tomorrow));*/
		
		BasicDBObject rtsObj3 = new BasicDBObject();
		rtsObj3.append("t", "SPGMS6");
		rtsObj3.append("f", ft.format(dNow));
		rtsObj3.append("e", ft.format(tomorrow));
		
		rtsTagsList.add(rtsObj);
		//rtsTagsList.add(rtsObj2);
		rtsTagsList.add(rtsObj3);
		
		//MDTags
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject tagObj = new BasicDBObject();
		tagObj.append("t", "SPFTK823600153010");
		tagObj.append("f", ft.format(dNow));
		tagObj.append("e", ft.format(tomorrow));
/*		
		tagObj.append("t", "CECAS723600153010");
		tagObj.append("f", "2015-09-30");
		tagObj.append("e", "2016-03-30");*/
		
		BasicDBObject tagObj2 = new BasicDBObject();
		tagObj2.append("t", "SPLAS823600153010");
		tagObj2.append("f", ft.format(dNow));
		tagObj2.append("e", ft.format(dNow));
		
/*		tagObj2.append("t", "CETVS723600153010");
		tagObj2.append("f", "2015-09-30");
		tagObj2.append("e", "2016-03-30");*/
		
		/*BasicDBObject tagObj3 = new BasicDBObject();
		tagObj3.append("t", "SPGMS723600153010");
		tagObj3.append("f", ft.format(dNow));
		tagObj3.append("e", ft.format(dNow));*/
		
		mdTagsList.add(tagObj);
		mdTagsList.add(tagObj2);
		//mdTagsList.add(tagObj3);
		
		memberMDTagsWithDatesColl.insert(new BasicDBObject("l_id","OccassionTopologyTestingl_id")
			.append("tags", mdTagsList)
			.append("rtsTags", rtsTagsList));

		memberMDTags2Dao = new MemberMDTags2Dao();
		tagVariableDao = new TagVariableDao();
		
		tagCreatorBolt = new TagCreatorBolt(System.getProperty("rtseprod"));
		
		modelTagsMap.put(28,"SPGMS");
		modelTagsMap.put(29,"HAGAS");
		
		Model model = null;
		model = new Model();
		model.setModelId(28);
		model.setModelCode("SPGMS");
		modelsMap.put(28, model);
		
		model = new Model();
		model.setModelId(29);
		model.setModelCode("HAGAS");
		modelsMap.put(29, model);
		
		occasionDurationMap.put("1", "8");
		occasionDurationMap.put("2", "8");
		occasionDurationMap.put("3", "61");
		occasionDurationMap.put("4", "61");
		occasionDurationMap.put("5", "8");
		occasionDurationMap.put("6", "8");
		occasionDurationMap.put("7", "3");
		occasionDurationMap.put("8", "3");
		
		
		occasionPriorityMap.put("1", "1");
		occasionPriorityMap.put("2", "2");
		occasionPriorityMap.put("3", "3");
		occasionPriorityMap.put("4", "5");
		occasionPriorityMap.put("5", "4");
		occasionPriorityMap.put("6", "7");
		occasionPriorityMap.put("7", "6");
		occasionPriorityMap.put("8", "8");
		
	}
	
	public void initialize2() throws ConfigurationException {
		
		SystemPropertyUtility.setSystemProperty();
		

		// fake memberMDTags collection
		memberMDTagsWithDatesColl = SystemPropertyUtility.getDb().getCollection("memberMdTagsWithDates");
		/*//RTS Tags
		BasicDBList rtsTagsList = new BasicDBList();
		BasicDBObject rtsObj = new BasicDBObject();
		rtsObj.append("t", "SPFTS823600153010");
		rtsObj.append("f", ft.format(dNow));
		rtsObj.append("e", ft.format(tomorrow));
		
		BasicDBObject rtsObj2 = new BasicDBObject();
		rtsObj2.append("t", "SPGMS8");
		rtsObj2.append("f", ft.format(dNow));
		rtsObj2.append("e", ft.format(tomorrow));
		
		BasicDBObject rtsObj3 = new BasicDBObject();
		rtsObj3.append("t", "SPGMS6");
		rtsObj3.append("f", ft.format(dNow));
		rtsObj3.append("e", ft.format(tomorrow));
		
		rtsTagsList.add(rtsObj);
		rtsTagsList.add(rtsObj2);
		rtsTagsList.add(rtsObj3);*/
		
		//MDTags
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject tagObj = new BasicDBObject();
		tagObj.append("t", "SPFTK823600153010");
		tagObj.append("f", ft.format(dNow));
		tagObj.append("e", ft.format(tomorrow));
		
		BasicDBObject tagObj2 = new BasicDBObject();
		tagObj2.append("t", "SPLAS823600153010");
		tagObj2.append("f", ft.format(dNow));
		tagObj2.append("e", ft.format(dNow));
		
		mdTagsList.add(tagObj);
		mdTagsList.add(tagObj2);

		
		memberMDTagsWithDatesColl.insert(new BasicDBObject("l_id","OccassionTopologyTestingl_id")
			.append("tags", mdTagsList)
			//.append("rtsTags", rtsTagsList)
			);

		memberMDTags2Dao = new MemberMDTags2Dao();
		tagVariableDao = new TagVariableDao();
		
		tagCreatorBolt = new TagCreatorBolt(System.getProperty("rtseprod"));
		
		modelTagsMap.put(28,"SPGMS");
		modelTagsMap.put(29,"HAGAS");
		
		occasionDurationMap.put("1", "8");
		occasionDurationMap.put("2", "8");
		occasionDurationMap.put("3", "61");
		occasionDurationMap.put("4", "61");
		occasionDurationMap.put("5", "8");
		occasionDurationMap.put("6", "8");
		occasionDurationMap.put("7", "3");
		occasionDurationMap.put("8", "3");
		
		
		occasionPriorityMap.put("1", "1");
		occasionPriorityMap.put("2", "2");
		occasionPriorityMap.put("3", "3");
		occasionPriorityMap.put("4", "5");
		occasionPriorityMap.put("5", "4");
		occasionPriorityMap.put("6", "7");
		occasionPriorityMap.put("7", "6");
		occasionPriorityMap.put("8", "8");
		
	}

	// MdTags Junit Test Cases
	@Test
	public void addMemberMdTagsAlreadyExistingTest() throws JSONException, ParseException {
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS523600153010");
		//tags.add("CETVS823600153010");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addMemberMDTags("OccassionTopologyTestingl_id", tags, occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");

		//Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(1).toString()).get("t")).equals("SPGMS5"), true);
		Assert.assertEquals(list.size(), 1);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010"));
		
		Assert.assertEquals(tagList.size(), 1);
		Assert.assertTrue(tagList.toString().contains("SPGMS523600153010"));
	}
	
	@Test
	public void addMemberMdTagsAlreadyExistingTest2() throws JSONException, ParseException, ConfigurationException {
		
		initialize();
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS823600153010");
		tags.add("SPFTK823600153010");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addMemberMDTags("OccassionTopologyTestingl_id", tags, occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		//Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(1).toString()).get("t")).equals("SPGMS8"), true);
		Assert.assertEquals(list.size(), 2);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010") && list.toString().contains("SPGMS6"));
		
		Assert.assertEquals(tagList.size() , 1);
		Assert.assertTrue(tagList.toString().contains("SPFTK823600153010"));
	}
	

	@Test
	public void addMemberMdTagsNewTagSameLidTest() throws ParseException, ConfigurationException {
		initialize();
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS823600153010");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addMemberMDTags("OccassionTopologyTestingl_id", tags,occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		Assert.assertEquals(list.size(), 2);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010") && list.toString().contains("SPGMS6"));
		Assert.assertEquals(tagList.size() , 1);
		Assert.assertTrue(tagList.toString().contains("HALAS823600153010"));
	}
	
	@Test
	public void addMemberMdTagsNewTagEmptyMdTagSameLidTest() throws ParseException, ConfigurationException {
		initialize2();
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS123600153010");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		////System.out.println("Initial RTS Tags : " + list == null ? "null" : list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addMemberMDTags("OccassionTopologyTestingl_id", tags,occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		////System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		Assert.assertEquals(list, null);
		Assert.assertEquals(tagList.size() , 1);
		Assert.assertTrue(tagList.toString().contains("SPGMS123600153010"));
	}

	@Test
	public void addMemberMdTagsNewTagNewLidTest() throws JSONException, ParseException, ConfigurationException {
		initialize();
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS823600153010");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list == null ? "null" : list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addMemberMDTags("OccassionTopologyTestingl_id2", tags,occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		////System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		Assert.assertEquals(list, null);
		//Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(0).toString()).get("t")).equals("HALAS8"), true);
		Assert.assertEquals(tagList.size() , 1);
		Assert.assertTrue(tagList.toString().contains("HALAS823600153010"));
		
	}
	
	
	//RtsTags Junit Test Cases
	@Test
	public void addMemberRtsTagsAlreadyExistingTest() throws JSONException, ParseException {
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS5");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags, occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");

		Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(1).toString()).get("t")).equals("SPGMS5"), true);
		Assert.assertEquals(list.size(), 2);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010") && list.toString().contains(ft.format(eighthDay)));
		
		Assert.assertEquals(tagList.size(), 2);
		Assert.assertTrue(tagList.toString().contains("SPLAS823600153010") && tagList.toString().contains("SPFTK823600153010"));
	}
	
	@Test
	public void addMemberRtsTagsAlreadyExistingTest2() throws JSONException, ParseException, ConfigurationException {
		
		initialize();
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS8");
		tags.add("SPFTK8");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags, occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		//Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(1).toString()).get("t")).equals("SPGMS8"), true);
		Assert.assertEquals(list.size(), 3);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010") &&  list.toString().contains("SPGMS6") && list.toString().contains("SPFTK8")); 
		
		Assert.assertEquals(tagList.size() , 1);
		Assert.assertTrue(tagList.toString().contains("SPLAS823600153010"));
	}
	

	@Test
	public void addMemberRtsTagsNewTagSameLidTest() throws ParseException, ConfigurationException {
		initialize();
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS8");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		//System.out.println("Initial RTS Tags : " + list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags,occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		Assert.assertEquals(list.size(), 3);
		Assert.assertTrue(list.toString().contains("SPFTS823600153010") && list.toString().contains("SPGMS6") && list.toString().contains("HALAS8"));
		
		Assert.assertEquals(tagList.size() , 2);
		Assert.assertTrue(tagList.toString().contains("SPLAS823600153010") && tagList.toString().contains("SPFTK823600153010"));
	}
	
	@Test
	public void addMemberRtsTagsNewTagEmptyRtsTagSameLidTest() throws ParseException, ConfigurationException {
		initialize2();
		List<String> tags = new ArrayList<String>();
		tags.add("SPGMS1");
		
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		////System.out.println("Initial RTS Tags : " + list == null ? "null" : list.toString());
		//System.out.println("Initial Md Tags : " + tagList.toString());
		
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id", tags,occasionDurationMap, occasionPriorityMap);
		cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id"));
		obj = cursor.next();
		list = (BasicDBList) obj.get("rtsTags");
		tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		//System.out.println("Organized Md Tags : " + tagList.toString()+"\n");
		
		Assert.assertEquals(list.size(), 1);
		Assert.assertTrue(list.toString().contains("SPGMS1"));
		Assert.assertEquals(tagList.size() , 2);
		Assert.assertTrue(tagList.toString().contains("SPLAS823600153010") && tagList.toString().contains("SPFTK823600153010"));
	}

	@Test
	public void addMemberRtsTagsNewTagNewLidTest() throws JSONException, ParseException {
		List<String> tags = new ArrayList<String>();
		tags.add("HALAS8");
		
				
		memberMDTags2Dao.addRtsMemberTags("OccassionTopologyTestingl_id2", tags,occasionDurationMap, occasionPriorityMap);
		DBCursor cursor = memberMDTagsWithDatesColl.find(new BasicDBObject("l_id",
				"OccassionTopologyTestingl_id2"));
		DBObject obj = cursor.next();
		BasicDBList list = (BasicDBList) obj.get("rtsTags");
		BasicDBList tagList = (BasicDBList) obj.get("tags");
		
		//System.out.println("Organized RTS Tags : " + list.toString());
		////System.out.println("Organized Md Tags : " +  tagList == null ? "null" : tagList.toString()); 
		
		Assert.assertEquals(list.size(), 1);
		Assert.assertTrue(list.toString().contains("HALAS8"));
		//Assert.assertEquals((new org.codehaus.jettison.json.JSONObject(list.get(0).toString()).get("t")).equals("HALAS8"), true);
		Assert.assertEquals(tagList , null);
		
	}
	
	
	@Test
	public void testCreateTagWithExistingMDTag() {
		
		ModelScore modelScore = new ModelScore();
		modelScore.setModelId("28");
		tagCreatorBolt.setModelsMap(modelsMap);
		tagCreatorBolt.setMemberMDTags2Dao(memberMDTags2Dao);
		String tag = tagCreatorBolt.createTag(modelScore, "OccassionTopologyTestingl_id", "8");
		Assert.assertEquals(tag, "SPGMS8");
	}
	
	@Test
	public void testCreateTagWithNewRtsTag() {
		
		ModelScore modelScore = new ModelScore();
		modelScore.setModelId("29");
		tagCreatorBolt.setModelsMap(modelsMap);
		tagCreatorBolt.setMemberMDTags2Dao(memberMDTags2Dao);
		String tag = tagCreatorBolt.createTag(modelScore, "OccassionTopologyTestingl_id", "8");
		Assert.assertEquals(tag, "HAGAS8");
	}
	
	
	

	@AfterClass
	public static void teardown() {
		SystemPropertyUtility.dropDatabase();
	}
}

