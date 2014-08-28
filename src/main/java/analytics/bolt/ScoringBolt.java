/**
 * 
 */
package analytics.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import analytics.util.ScoringSingleton;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ScoringBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    DBCollection modelVariablesCollection;
    DBCollection memberVariablesCollection;
    DBCollection memberScoreCollection;
    DBCollection variablesCollection;
    DBCollection changedVariablesCollection;
    DBCollection changedMemberScoresCollection;
    
    
//    private Map<String,Collection<Integer>> variableModelsMap;
//    private Map<String, String> variableVidToNameMap;
//    private Map<String, String> variableNameToVidMap;
//    private Map<Integer,Map<Integer, Model>> modelsMap;
//
//    private Jedis jedis;

    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void setModelCollection(DBCollection modelCollection) {
        this.modelVariablesCollection = modelCollection;
    }

    public void setMemberCollection(DBCollection memberCollection) {
        this.memberVariablesCollection = memberCollection;
    }

    public void setMemberScoreCollection(DBCollection memberScoreCollection) {
        this.memberScoreCollection = memberScoreCollection;
    }

    public void setVariablesCollection(DBCollection variablesCollection) {
        this.variablesCollection = variablesCollection;
    }

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}
	

    /*
     * (non-Javadoc)
     *
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
	@Override
	public void execute(Tuple input) {
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD
		String l_id = input.getString(0);
		String source = input.getString(2);
		
		
		// SCORING BOLTS READS A LIST OF OBJECTS WITH THE FIRST ELEMENT BEING THE HASHED LOYALTY ID
		// AND n MODEL IDs AFTER
		ArrayList<String> modelIdList = restoreModelListFromJson(input.getString(1));

		Map<String, Double> modelScoresMap = ScoringSingleton.getInstance().execute(l_id, modelIdList);
		
        // EMIT CHANGES
//		double oldScore = 0;
//    	List<Object> listToEmit = new ArrayList<Object>();
//    	listToEmit.add(l_id);
//    	listToEmit.add(oldScore);
//    	listToEmit.add(newScore);
//    	listToEmit.add(modelId);
//    	listToEmit.add(source);
//    	System.out.println(" ### SCORING BOLT EMITTING: " + listToEmit);
//    	this.outputCollector.emit(listToEmit);

            
            //System.out.println(message);
            //jedis.publish("score_changes", message);
    	//System.out.println(" ### UPDATE RECORD CHANGED SCORE: " + updateRec);
//        if(updateRec != null) {
//        	changedMemberScoresCollection.update(new BasicDBObject("l_id", l_id), new BasicDBObject("$set", updateRec), true, false);
//
//        }
    }



    /*
      * (non-Javadoc)
      *
      * @see
      * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
      * topology.OutputFieldsDeclarer)
      */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","oldScore","newScore","model","source"));
		
	}

	public static ArrayList<String> restoreModelListFromJson(String json)
    {
        //System.out.println(" ### MODEL LIST STRING: " + json);
		//modelList = new ArrayList<Object>();
        
        String strings[]=StringUtils.split(json,",");
        ArrayList<String> modelList = new ArrayList<String>();
        for(String s: strings) {
        	modelList.add(s);
        }
        //System.out.println(" ### MODEL LIST PARSED: " + json);
        /*
        Type lineItemListType = new TypeToken<List<Object>>() {}.getType();
        List<Object> modelList = new Gson().fromJson(json, lineItemListType);
        */
        return modelList;
    }
}	

