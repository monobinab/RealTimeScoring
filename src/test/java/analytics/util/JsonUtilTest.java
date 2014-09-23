package analytics.util;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import analytics.util.JsonUtils;

public class JsonUtilTest {
@Test
public void testCreateJsonWithEmptyMap(){
	Map<String,String> variableValuesMap= new HashMap<String, String>();
	String output = (String) JsonUtils.createJsonFromStringStringMap(variableValuesMap);
	Assert.assertEquals("{}", output);
}
@Test
public void testCreateJsonWithNullMap(){
	Map<String,String> variableValuesMap= null;
	String output = (String) JsonUtils.createJsonFromStringStringMap(variableValuesMap);
	Assert.assertEquals("null", output);
}
}
