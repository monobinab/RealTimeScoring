package analytics.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.DBConnection;
import analytics.util.JsonUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StrategyScoringBolt extends BaseRichBolt {
	static final Logger logger = LoggerFactory
			.getLogger(StrategyScoringBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    private DBCollection modelVariablesCollection;
    private DBCollection memberVariablesCollection;
    private DBCollection variablesCollection;
    private DBCollection changedVariablesCollection;
    private Map<String,List<Integer>> variableModelsMap;
    private Map<String, String> variableVidToNameMap;
    private Map<String, String> variableNameToVidMap;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
//      this.outputCollector.emit(tuple);
	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */

      logger.info("PREPARING STRATEGY BOLT");
      
      try {
			db = DBConnection.getDBConnection();
		} catch (ConfigurationException e) {
			logger.error("Unable to contain DB connection",e);
		}

      modelVariablesCollection = db.getCollection("modelVariables");
      memberVariablesCollection = db.getCollection("memberVariables");
      variablesCollection = db.getCollection("Variables");
      changedVariablesCollection = db.getCollection("changedMemberVariables");
      
      logger.debug("Populate variable models map");
      // populate the variableModelsMap
      variableModelsMap = new HashMap<String, List<Integer>>();
      DBCursor models = modelVariablesCollection.find();
      for(DBObject model:models){
           BasicDBList modelVariables = (BasicDBList) model.get("variable");
           for(Object modelVariable:modelVariables)
           {
               String variableName = ((DBObject) modelVariable).get("name").toString().toUpperCase();
               if (variableModelsMap.get(variableName) == null)
               {
                   List<Integer> modelIds = new ArrayList<Integer>();
                   addModel(model, variableName.toUpperCase(), modelIds);
               }
               else
               {
                   List<Integer> modelIds = variableModelsMap.get(variableName.toUpperCase());
                   addModel(model, variableName.toUpperCase(), modelIds);
               }
           }
      }

      logger.debug("Populate variable vid map");
      // populate the variableVidToNameMap
      variableVidToNameMap = new HashMap<String, String>();
      variableNameToVidMap = new HashMap<String, String>();
      DBCursor vCursor = variablesCollection.find();
      for(DBObject variable:vCursor){
			String variableName = ((DBObject) variable).get("name").toString().toUpperCase();
			String vid = ((DBObject) variable).get("VID").toString();
			if (variableName != null && vid != null)
			{
				variableVidToNameMap.put(vid, variableName.toUpperCase());
				variableNameToVidMap.put(variableName.toUpperCase(),vid);
			}
      }
		
	}
	 private void addModel(DBObject model, String variableName, List<Integer> modelIds) {
	        modelIds.add(Integer.valueOf(model.get("modelId").toString()));
	        variableModelsMap.put(variableName.toUpperCase(), modelIds);
	    }

	@Override
	public void execute(Tuple input) {
		logger.debug("The time it enters inside Strategy Bolt execute method"+System.currentTimeMillis());
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		// 2) Create map of new changes from the input
		// 3) Find all models affected by the changes
		// 4) Find all variables for models
		// 5) Create a map of variable values, fetched from from memberVariables
		// 6) For each variable in new changes, execute strategy
		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch all for the member since upsert needs it too
		// 8) Rescore - arbitrate between new changes, changedMemberVariables and memberVariables
		// 9) Emit the score
		// 10) Write changedMemberScores with expiry
		// 11) Write changedMemberVariables with expiry
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		String l_id = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if(input.contains("messageID")){
			messageID = input.getStringByField("messageID");
		}
		
		// 2) Create map of new changes from the input
		Map<String, String> newChangesVarValueMap = JsonUtils.restoreVariableListFromJson(input.getString(1));
		
		// 3) Find all models affected by the changes
		Set<Object> modelIdList = new HashSet<Object>();
        for(String changedVariable:newChangesVarValueMap.keySet())
        {
        	List<Integer> models = variableModelsMap.get(changedVariable);
            for (Integer modelId: models){
            	modelIdList.add(modelId);
            }           
        }
        
		// 4) Find all variables for models
		// 5) Create a map of variable values, fetched from from memberVariables
		// 6) For each variable in new changes, execute strategy
		// 7) Fetch changedMemberVariables for rescoring and create a map- fetch all for the member since upsert needs it too
		// 8) Rescore - arbitrate between new changes, changedMemberVariables and memberVariables
		// 9) Emit the score
		// 10) Write changedMemberScores with expiry
		// 11) Write changedMemberVariables with expiry
		
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
