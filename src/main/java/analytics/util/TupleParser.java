package analytics.util;

import backtype.storm.tuple.Tuple;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class TupleParser {	
	
	public static JsonElement getParsedJson(Tuple input)throws JsonSyntaxException {
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = null;
		jsonElement = parser.parse(input.getString(0));
		return jsonElement;
	}

}
