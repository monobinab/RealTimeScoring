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
		for (String v : varCollection) {
			if (modelVariablesList.contains(v)) {
				isModVar = true;
			}
		}
		return isModVar;
	}

	public static final Object createJsonFromStringStringMap(Map<String, String> variableValuesMap) {

		Gson gson = new Gson();
		Type varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return gson.toJson(variableValuesMap, varValueType);
	}

	public static final Object createJsonFromStringObjectMap(Map<String, Object> variableValuesMap) {

		Gson gson = new Gson();
		Type varValueType = new TypeToken<Map<String, Object>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return gson.toJson(variableValuesMap, varValueType);
	}

	public static Map<String, List<String>> restoreDateTraitsMapFromJson(String json) {
		Type dateTraitType = new TypeToken<Map<String, Collection<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return new Gson().fromJson(json, dateTraitType);
	}

	public static Map<String, Object> restoreDateDCMapFromJson(String json) {
		Type dateDCType = new TypeToken<Map<String, Object>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return new Gson().fromJson(json, dateDCType);
	}

	public static Map<String, String> restoreVariableListFromJson(String json) {
		Type varListType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return new Gson().fromJson(json, varListType);
	}
	//Need to find a better place to put this method
	public static Double convertToDouble(Object num) {
		if (num instanceof Integer) {
			return ((Integer) num).doubleValue();
		} else if (num instanceof Double) {
			return (Double) num;
		}
		return 0.0;

	}

}
