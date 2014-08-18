package analytics.bolt;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PersistTraitsBolt extends BaseRichBolt {

	 DB db;
	 MongoClient mongoClient;
	 DBCollection memberTraitsCollection;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			mongoClient = new MongoClient("trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = mongoClient.getDB("test");
		memberTraitsCollection = db.getCollection("memberTraits");
		
	}

	@Override
	public void execute(Tuple input) {
		//Get the encrypted loyalty id
		String l_id = input.getString(0);
		//The data comes as below
		/*"M_WEB_TRAIT_POWER_TOOL_8_14":"{\"2014-08-15\":[\"206096\",\"80974\",\"206373\",\"238846\",\"212720\",\"272416\"]}",
		      "M_WEB_DAY_HAND_TOOL_0_7":"{\"2014-08-15\":[\"206096\",\"80974\",\"206373\",\"238846\",\"212720\",\"272416\"]}",
		   "M_WEB_TRAIT_POWER_TOOL_0_7":"{\"2014-08-15\":[\"206096\",\"80974\",\"206373\",\"238846\",\"212720\",\"272416\"]}"
		        ,"M_WEB_DAY_REFRIG_0_7":"{\"2014-08-15\":[\"206096\",\"80974\",\"206373\",\"238846\",\"212720\",\"272416\"]}",
		      "M_WEB_TRAIT_COOKING_0_7":"{\"2014-08-15\":[\"206096\",\"80974\",\"206373\",\"238846\",\"212720\",\"272416\"]}"},*/
		//We need the values from any trait, since the data is replicated across them
		//This would also contain past data, so we can replace the content in mongo
		//Get the map from wire
		DBObject traitDateMap = (DBObject) JSON.parse(input.getString(1));

		//Read a key from the map
		for(String traitName: traitDateMap.keySet()){
			DBObject dateTrait = (DBObject) JSON.parse(traitDateMap.get(traitName).toString());
			BasicDBList dateTraitList = new BasicDBList();
			for(String date : dateTrait.keySet()){
				dateTraitList.add(new BasicDBObject("d", date).append("t", dateTrait.get(date)));
			}
			DBObject objectToInsert = new BasicDBObject();
			objectToInsert.put("l_id", l_id);
			objectToInsert.put("date", dateTraitList);
			memberTraitsCollection.insert(objectToInsert);
			break;//all trait names have same map
		}
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Nothing to emit
		
	}

    
	public static Map<String, String> restoreVariableListFromJson(String json)
    {
		Map<String, String> varList = new HashMap<String, String>();
        Type varListType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;}.getType();

        varList = new Gson().fromJson(json, varListType);
        System.out.println(" JSON string: " + json);
        System.out.println(" Map: " + varList);
        return varList;
    }

}
