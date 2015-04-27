package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.MemberTraitsDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class PersistTraitsBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistTraitsBolt.class);
    private MemberTraitsDao memberTraitsDao;
	private OutputCollector outputCollector;
	
	 public PersistTraitsBolt(String systemProperty){
		 super(systemProperty);
	 }
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {	
		this.outputCollector = collector;
		super.prepare(stormConf, context, collector);
	}
	
	 
	@Override
	public void execute(Tuple input) {
		LOGGER.debug("Persisting trait date map in mongo");
		//countMetric.scope("incoming_record").incr();
		redisCountIncr("incoming_record");
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

		try {
			JSONObject obj = new JSONObject(input.getString(1));
			Map<String, List<String>> dateTraitMap = new HashMap<String, List<String>>();
			if(obj.keys().hasNext()){//we need only 1 trait, so no need to loop
				String traitDateString = obj.get(obj.keys().next().toString()).toString();
				JSONObject dateTraitObj = new JSONObject(traitDateString);
				Iterator<Object> keys = dateTraitObj.keys();
				while(keys.hasNext()){
					String date = keys.next().toString();
					//JSONObject traits = new JSONObject(dateTraitObj.get(date).toString());
					JSONArray traitArray = new JSONArray(dateTraitObj.get(date).toString());
					List<String> list = new ArrayList<String>();
					for (int i=0; i<traitArray.length(); i++) {
					    list.add( traitArray.getString(i) );
					}
					dateTraitMap.put(date, list);
				}
			}
			memberTraitsDao.addDateTrait(l_id, dateTraitMap);
    		//countMetric.scope("persisted_traits").incr();
			redisCountIncr("persisted_traits");
    		outputCollector.ack(input);
		} catch (JSONException e) {
			LOGGER.error("unable to persist trait",e);
    		//countMetric.scope("persist_failed").incr();
			redisCountIncr("persist_failed");
    		outputCollector.fail(input);
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
        LOGGER.debug(" JSON string: " + json);
        LOGGER.debug(" Map: " + varList);
        return varList;
    }

}
