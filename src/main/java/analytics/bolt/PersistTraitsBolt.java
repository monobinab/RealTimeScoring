package analytics.bolt;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.MemberTraitsDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class PersistTraitsBolt extends BaseRichBolt {
	static final Logger logger = LoggerFactory
			.getLogger(PersistTraitsBolt.class);
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {		
	}

	@Override
	public void execute(Tuple input) {
		logger.debug("Persisting trait date map in mongo");
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
			new MemberTraitsDao().addDateTrait(l_id, dateTraitList);
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
        logger.debug(" JSON string: " + json);
        logger.debug(" Map: " + varList);
        return varList;
    }

}
