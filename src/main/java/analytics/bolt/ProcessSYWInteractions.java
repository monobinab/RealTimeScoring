package analytics.bolt;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.MongoUtils;
import analytics.util.SYWAPICalls;
import analytics.util.objects.SYWEntity;
import analytics.util.objects.SYWInteraction;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ProcessSYWInteractions extends BaseRichBolt {

	private List<String> entityTypes;
	private DB db;
    private DBCollection pidVarCollection;
    private OutputCollector outputCollector;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		entityTypes = new ArrayList<String>();
		entityTypes.add("Product");
		/**
		 * Ignore Story,Image,Video TODO: Might even make sense to ignore these
		 * right at the parsing bolt level
		 */	

      try {
		db = MongoUtils.getClient("DEV");
	} catch (UnknownHostException e) {
		e.printStackTrace();
	}

      pidVarCollection = db.getCollection("pidDivLn");
	}

	@Override
	public void execute(Tuple input) {
		//Get l_id", "message", "InteractionType" from parsing bolt
		JsonParser parser = new JsonParser();
		JsonObject interactionObject = (JsonObject) input.getValueByField("message");

		//Create a SYW Interaction object
		Gson gson = new Gson();
		SYWInteraction obj = gson.fromJson(interactionObject, SYWInteraction.class);
		
		//Variable map stores the vars to send to Strategy Bolt
		Map<String,String> variableValueMap = new HashMap<String, String>();
		if(obj!=null && obj.getEntities()!=null){
			
			for(SYWEntity currentEntity : obj.getEntities()){
				System.out.println(currentEntity.getType());
				//if(entityTypes.contains(entityTypes)){ 
				//TODO: If more types handle in a more robust manner. If we expect only Products, this makes sense
				if(currentEntity!=null && currentEntity.getType().equals("Product")){
					String productId = SYWAPICalls.getCatalogIds(currentEntity.getId());
					/* Product does not exist? */
					if(productId.equals("UNKNOWN")){
						System.out.println("Unable to find the product id");
						continue;
					}
					else{
						DBObject obj1 = pidVarCollection.findOne(new BasicDBObject("pid", productId));
						if(obj1 != null)
						{
							String variable = MongoUtils.getBoostVariable((String)obj1.get("d"), (String)obj1.get("l"));
							variableValueMap.put(variable, "1");
						}
						else
						{
							System.out.println("Unable to get information for pid" + productId);
							continue;
						}
					}
					
			    	Type varValueType = new TypeToken<Map<String, String>>() {}.getType();
			    	String varValueString = gson.toJson(variableValueMap, varValueType);
		        	List<Object> listToEmit = new ArrayList<Object>();
		        	//TODO: sandbox ids will not be found. So hardcoding a random member id.
		        	//Find a solution and replace the below line
		        	listToEmit.add("CXcU+gBUakT3ro2ILK21u2Q8ujY=");//input.getValueByField("l_id"));
		        	listToEmit.add(varValueString);
		        	listToEmit.add("SYW");
		        	System.out.println(" @@@ SYW PARSING BOLT EMITTING: " + listToEmit);
		        	this.outputCollector.emit(listToEmit);
				}
			}
		}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source"));
	}

}