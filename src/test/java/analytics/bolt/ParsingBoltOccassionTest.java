package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.SystemPropertyUtility;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.OccasionVariableDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.tuple.Tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.fakemongo.Fongo;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ParsingBoltOccassionTest{
//static DB db;
static DBCollection memberMDTagsColl;
static DBCollection tagMetadataColl;
static DBCollection tagVariableColl;
static DBCollection occassionVariableColl;
static DBCollection modelPercColl;
static ParsingBoltOccassion parsingBoltOccassion;
static String input;
static Tuple tuple;
static MemberMDTagsDao memberMDTagsDao;
static TagVariableDao tagVariableDao;
static OccasionVariableDao occasionVariableDao;
static ModelPercentileDao modelPercDao;
	
@BeforeClass

public static void intialize() throws Exception{
		
		//get the fakeMongoColl from ParsingBoltOccassionFakeMonogColl
		ParsingBoltOccassionFakeMonogColl.fakeMongoColl();
		memberMDTagsColl = ParsingBoltOccassionFakeMonogColl.getMemberMDTagsColl();
		tagVariableColl = ParsingBoltOccassionFakeMonogColl.getTagVariableColl();
		tagMetadataColl = ParsingBoltOccassionFakeMonogColl.getTagMetadataColl();
		modelPercColl = ParsingBoltOccassionFakeMonogColl.getModelPercColl();
		memberMDTagsDao = new MemberMDTagsDao();
		tagVariableDao = new TagVariableDao();
		modelPercDao = new ModelPercentileDao();
			
		parsingBoltOccassion = new ParsingBoltOccassion(System.getProperty("rtseprod"));
		parsingBoltOccassion.setMemberTagsDao();
		parsingBoltOccassion.setTagMetadataDao();
		parsingBoltOccassion.setTagVariableDao();
		parsingBoltOccassion.setModelPercDao();
		
}

public Tuple mockTuple(){
	//mock the tuple
			String message = "{\"lyl_id_no\":\"Occ\",\"tags\":[\"HACKS2010\"]}";
			Tuple tuple = mock(Tuple.class, message);
			 when(tuple.getStringByField("message")).thenReturn(message);
			return tuple;
}

public Tuple mockTuple2(){
	//mock the tuple
			String message = "{\"lyl_id_no\":\"Occ\",\"tags\":\"HACKS2010\"]}";
			Tuple tuple = mock(Tuple.class, message);
			 when(tuple.getStringByField("message")).thenReturn(message);
			return tuple;
}

//test for null l_id
@Test
public void getMemberTagsNullTest(){
	List<String> mdTags = memberMDTagsDao.getMemberMDTags("");
	Assert.assertEquals(null, mdTags);
}

//test to check the mdTags for a specific l_id
@Test
public void getMemberTagsTest(){
	List<String> mdTagsActual = memberMDTagsDao.getMemberMDTagsForVariables("OccassionTopologyTestingl_id");
	List<String> mdTagsExpected = new ArrayList<String>();
	mdTagsExpected.add("HACKS");
	mdTagsExpected.add("HARFS");
	mdTagsExpected.add("HALAS");
	mdTagsExpected.add("HADHS");
	Assert.assertEquals(4, mdTagsActual.size());
	Assert.assertEquals(mdTagsExpected.get(0), mdTagsActual.get(0));
}

//test to check the variablesList for mdTagsList, every tag is associated with one variable, so number of tags = number of variables
@Test
public void getTagVariablesListTest(){
	List<String> tagsList = new ArrayList<String>();
	tagsList.add("HACKS");
	tagsList.add("HARFS");
	List<String> tagsVarListActual = tagVariableDao.getTagVariablesList(tagsList);
	List<String> tagsVarListExpected = new ArrayList<String>();
	tagsVarListExpected.add("BOOST_PO_HA_COOK_TEST");
	tagsVarListExpected.add("BOOST_PO_HA_REF_TEST");
	Assert.assertEquals(tagsVarListExpected.get(0), tagsVarListActual.get(0));
	Assert.assertEquals(tagsVarListExpected.get(1), tagsVarListActual.get(1));
}

//test to check for empty variableList for non existent mdTags in tagVariables collection
@Test
public void getTagVariablesListEmptyTest(){
	List<String> tagsList = new ArrayList<String>();
	tagsList.add("HAKKS2010");
	List<String> tagsVarListActual = tagVariableDao.getTagVariablesList(tagsList);
	Assert.assertEquals(0, tagsVarListActual.size());
	}

//test to check the resetting of variableValueMap to zero before persisting tags to memberMDTags collection
@Test
public void resetVariableValueMap(){
	Map<String, String> varValueMap = new HashMap<String, String>();
	parsingBoltOccassion.resetVariableValuesMap(varValueMap, "OccassionTopologyTestingl_id");
	Assert.assertEquals(3, varValueMap.size());
	Assert.assertEquals("0", varValueMap.get("BOOST_PO_HA_COOK_TEST"));
}

//test to check the empty variableBalueMap for non existent member in memberMDTags collection
@Test
public void resetVariableValueMapForNonExistentMemberId(){
	
	Map<String, String> varValueMap = new HashMap<String, String>();
	parsingBoltOccassion.resetVariableValuesMap(varValueMap, "testingId");
	Assert.assertEquals(0, varValueMap.size());
}

//test to check the variableValueMap for non existent variable in tagVariables collection
@Test
public void resetVariableValueMapFornonexistentVar(){
	Map<String, String> varValueMap = new HashMap<String, String>();
	parsingBoltOccassion.resetVariableValuesMap(varValueMap, "OccassionTopologyTestingl_id");
	Assert.assertEquals(3, varValueMap.size());
}

//test to check the StringBuilder which will be added to object, that is to be emitted to persistbolt
@Test
public void persistToMemberTagsTest(){
	String tagString = "{\"l_id\":\"OccassionTopologyTestingl_id\", \"tags\":[\"CETVS2010\"]}";
	StringBuilder stringBuilder = new StringBuilder();
	JsonParser parser = new JsonParser();
	JsonElement json = parser.parse(tagString);
	JsonArray tagsArray = (JsonArray) json.getAsJsonObject().get("tags");
	parsingBoltOccassion.persistTagsToMemberTagsColl(json.getAsJsonObject().get("l_id").getAsString(), stringBuilder, tagsArray);
	Assert.assertEquals(9, stringBuilder.length());
	}

//test to check the metaData from tagMetadata collection
@Test
public void getTagMetaDataTest(){
	JsonParser parser = new JsonParser();
	JsonElement tag = parser.parse("HACKS2010");
	TagMetadata tagMetaData = parsingBoltOccassion.getTagMetaData(tag);
	Assert.assertEquals("Duress", tagMetaData.getPurchaseOccasion());
	Assert.assertEquals("HA", tagMetaData.getBusinessUnit());
	}
//test to check for null if mdTag does not exist in tagMetadata collection
@Test
public void getTagMetaDataNullTest(){
	JsonParser parser = new JsonParser();
	JsonElement tag = parser.parse("HAKKS2010");
	TagMetadata tagMetaData = parsingBoltOccassion.getTagMetaData(tag);
	Assert.assertNull(tagMetaData);
	}

//test to the variable value for mdTag from tagVariable collection
@Test
public void getVariableValueTest(){
	
	String tagVarValue = parsingBoltOccassion.getTagVarValue("68");
	Assert.assertEquals("0.11", tagVarValue);
}

@Test
public void getVariableValueNullTest(){
	String tagVarValue = parsingBoltOccassion.getTagVarValue(null);
	Assert.assertNull(tagVarValue);
}

//test to check the variable for specific mdTag from tagVariables collection
@Test
public void getVariableTest(){
	JsonParser parser = new JsonParser();
	JsonElement tag = parser.parse("HACKS2010");
	Map<String, String> tagVarActual = parsingBoltOccassion.getTagVariable(tag);
	Map<String, String> tagVarExpected = new HashMap<String, String>();
	tagVarExpected.put("BOOST_PO_HA_COOK_TEST", "35");
	Assert.assertEquals(tagVarExpected, tagVarActual);
}

//test to check the variable for non existent mdTag from tagVariables collection
@Test
public void getVariablenullTest(){
	JsonParser parser = new JsonParser();
	JsonElement tag = parser.parse("HAKKS2010");
	Map<String, String> tagVar = parsingBoltOccassion.getTagVariable(tag);
	Assert.assertNull(tagVar);
}

//test to check the tags ["HASCKS2010", "HALAS2010"] format of the input from spout
@Test
public void getTagsFromInputTest() {
	String str = "{\"lyl_id_no\":\"Occ\",\"tags\":[\"HACKS2010\"]}";
	JsonParser parser = new JsonParser();
	JsonElement jsonElement = parser.parse(str);
	JsonArray array = parsingBoltOccassion.getTagsFromInput(jsonElement);
	String actual = array.getAsString();
	Assert.assertEquals("HACKS2010", actual);
}

//test to check the tags if the "tags" is not there in the input
@Test
public void getTagsFromInputTest2() {
	String str = "{\"lyl_id_no\":\"Occ\",\"tag\":[\"HACKS2010\"]}";
	JsonParser parser = new JsonParser();
	JsonElement jsonElement = parser.parse(str);
	Assert.assertEquals(null, parsingBoltOccassion.getTagsFromInput(jsonElement));
	
}

//test the incoming tuple for its format
@Test
public void getParsedJsonTest(){
	Tuple tuple = mockTuple();
	JsonParser parser = new JsonParser();
	JsonElement jsonElementActual = parsingBoltOccassion.getParsedJson(tuple, parser);
	String str = "{\"lyl_id_no\":\"Occ\",\"tags\":[\"HACKS2010\"]}";
	JsonElement jsonElementExpected = parser.parse(str);
	Assert.assertEquals(jsonElementExpected, jsonElementActual);

}

//test the incoming tuple for its format
@Test(expected = com.google.gson.JsonSyntaxException.class)
public void getParsedJsonTest2(){
	Tuple tuple = mockTuple2();
	JsonParser parser = new JsonParser();
	 parsingBoltOccassion.getParsedJson(tuple, parser);
}

@AfterClass
public static void tearDown(){
	SystemPropertyUtility.dropDatabase();
}
}
 