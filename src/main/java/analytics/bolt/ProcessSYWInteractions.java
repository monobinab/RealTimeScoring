package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.SywApiCalls;
import analytics.util.dao.BoostDao;
import analytics.util.dao.DivLnBoostDao;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.SYWEntity;
import analytics.util.objects.SYWInteraction;
import analytics.util.objects.Variable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ProcessSYWInteractions extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ProcessSYWInteractions.class);
	private List<String> entityTypes;
	private OutputCollector outputCollector;
	private PidDivLnDao pidDivLnDao;
	private DivLnBoostDao divLnBoostDao;
	SywApiCalls sywApiCalls;
	private Map<String, List<String>> divLnBoostVariblesMap;
	private Map<String, Variable> boostMap;
	private VariableDao variableDao;
	private BoostDao boostDao;
	private MemberBoostsDao memberBoostsDao;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		sywApiCalls = new SywApiCalls();
		
		pidDivLnDao = new PidDivLnDao();
		divLnBoostDao = new DivLnBoostDao();
		boostDao = new BoostDao();
		variableDao = new VariableDao();
		memberBoostsDao = new MemberBoostsDao();
		entityTypes = new ArrayList<String>();
		entityTypes.add("Product");
		divLnBoostVariblesMap = divLnBoostDao.getDivLnBoost();// TODO: Should we pass the source topic?
		List<String> boostList = boostDao.getBoosts("SYW");//Feed prefix
		List<Variable> variableList = variableDao.getVariables();
		boostMap = new HashMap<String, Variable>();
		for (Variable v : variableList) {
			if (boostList.contains(v.getName())) {
				boostMap.put(v.getName(), v);
			}
		}

		/**
		 * Ignore Story,Image,Video TODO: Might even make sense to ignore these
		 * right at the parsing bolt level
		 */
	}

	@Override
	public void execute(Tuple input) {
		boolean catalogFlag = false;
		String feedType = null;
		// Get l_id", "message", "InteractionType" from parsing bolt
		JsonObject interactionObject = (JsonObject) input
				.getValueByField("message");
		String lId = input.getStringByField("l_id");
		
		// Create a SYW Interaction object
		Gson gson = new Gson();
		SYWInteraction obj = gson.fromJson(interactionObject,
				SYWInteraction.class);
		if("AddToCatalog".equals(obj.getType()))
			catalogFlag=true;
		
		/*Ignore interactions that we dont want.*/
		/*
		 * We currently process Catalogs: Like, Want, Own
		 * Others: Like
		 */
		if(catalogFlag){
			//Find type of catalog to process, and hence the boost variable
			for (SYWEntity currentEntity : obj.getEntities()) {
				if (currentEntity != null)
				{
					if("Catalog".equals(currentEntity.getType())){
						String catalogType = sywApiCalls.getCatalogType(currentEntity.getId());
						System.out.println(catalogType);
						if(catalogType.equals("Own"))
							feedType = "Own";
						else if(catalogType.equals("Like"))
							feedType = "Like";
						else if(catalogType.equals("Want"))
							feedType = "Want";
						
					}
				}
			}
		}
		if(feedType==null){
			LOGGER.info("We process only own, like and want catalogs & SYW Likes");
			outputCollector.fail(input);
		}
		// Variable map stores the vars to send to Strategy Bolt
		Map<String, String> variableValueMap = new HashMap<String, String>();
		Map<String, List<String>> boostValuesMap = new HashMap<String, List<String>>();
		if (obj != null && obj.getEntities() != null) {

			for (SYWEntity currentEntity : obj.getEntities()) {
				if (currentEntity != null)
					LOGGER.debug(currentEntity.getType());

				// TODO: If more types handle in a more robust manner. If we
				// expect only Products, this makes sense
				if (currentEntity != null
						&& "Product".equals(currentEntity.getType())) {
					String productId = sywApiCalls.getCatalogId(currentEntity
							.getId());
					/* Product does not exist? */
					if (productId == null) {
						LOGGER.info("Unable to get product id for "
								+ currentEntity.getId());
						// Get the next entity
						continue;
					} else {
						// TODO: We should also handle Kmart items - in which
						// case it would be divLnItm???
						PidDivLnDao.DivLn divLnObj = pidDivLnDao
								.getVariableFromTopic(productId);
						System.out.println("product id is" + productId);
						if (divLnObj != null) {
							String div = divLnObj.getDiv();
							String divLn = divLnObj.getDivLn();
							List<String> var = new ArrayList<String>();
							// Get the boost variable
							if(divLnBoostVariblesMap.containsKey(div)) {
								for(String b: divLnBoostVariblesMap.get(div)) {
									if(!boostValuesMap.containsKey(b)) {
										boostValuesMap.put(b, new ArrayList<String>());
									}
									boostValuesMap.get(b).add(productId);
								}
							}
							if(divLnBoostVariblesMap.containsKey(divLn)) {
								for(String b: divLnBoostVariblesMap.get(divLn)) {
									if(!boostValuesMap.containsKey(b)) {
										boostValuesMap.put(b, new ArrayList<String>());
									}
									boostValuesMap.get(b).add(productId);
								}
							}
						}
						// Eg - BOOST_SYW_APP_LIKETCOUNT
						// divLnBoost -
						// d:004 , b:BOOST_SYW_APP_LIKETCOUNT
						// 004 , BOOST_ATC_APP_TCOUNT
					}


					}

				}
			}
			//Get the memberBoostVariables for current lid. Consolidate into a map
			Map<String, Map<String, List<String>>> allBoostValuesMap = memberBoostsDao.getMemberBoostsValues(lId, boostValuesMap.keySet());
			if(allBoostValuesMap==null){
				allBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
			}
			
			for(String b: boostValuesMap.keySet()) {
				if(allBoostValuesMap.containsKey(b)) {
					allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
				} else {
					allBoostValuesMap.put(b,new HashMap<String, List<String>>());
					allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
				}
				variableValueMap.put(b, createJsonDoc(allBoostValuesMap.get(b)));
			}
			if(variableValueMap!=null && !variableValueMap.isEmpty()){
			Type varValueType = new TypeToken<Map<String, String>>() {
			}.getType();
			String varValueString = gson.toJson(variableValueMap,varValueType);
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(input.getValueByField("l_id"));
			listToEmit.add(varValueString);
			listToEmit.add(feedType);
			System.out.println(" @@@ SYW PARSING BOLT EMITTING: "
					+ listToEmit);
			this.outputCollector.emit(listToEmit);
			}
			this.outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source"));
	}
	
    private String createJsonDoc(Map<String, List<String>> dateValuesMap) {
    	LOGGER.debug("dateValuesMap: " + dateValuesMap);
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type boostValueType = new TypeToken<Map<String, List<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
		return gson.toJson(dateValuesMap, boostValueType);
	}

}
