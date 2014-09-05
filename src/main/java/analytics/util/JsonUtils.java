package analytics.util;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class JsonUtils {

	public static final boolean hasModelVariable(List<String> modelVariablesList, Collection<String> varCollection) {
		boolean isModVar = false;
		for(String v:varCollection) {
			if(modelVariablesList.contains(v)) {
				isModVar = true;
			}
		}
		return isModVar;
	}

	
	public static final Object createJsonFromStringStringMap(Map<String,String> variableValuesMap) {
		
		Gson gson = new Gson();		
    	Type varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
    	String varValueString = gson.toJson(variableValuesMap, varValueType);
		return varValueString;
	}
}
