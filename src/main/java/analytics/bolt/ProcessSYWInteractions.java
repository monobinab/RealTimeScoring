package analytics.bolt;

import analytics.util.SywApiCalls;
import analytics.util.dao.BoostDao;
import analytics.util.dao.DivLnBoostDao;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.PidDivLnDao;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessSYWInteractions extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ProcessSYWInteractions.class);
	private List<String> entityTypes;
	private OutputCollector outputCollector;
	private PidDivLnDao pidDivLnDao;
	private DivLnBoostDao divLnBoostDao;
	SywApiCalls sywApiCalls;
	private Map<String, List<String>> divLnBoostVariblesMap;
	private BoostDao boostDao;
	private MemberBoostsDao memberBoostsDao;
	Map<String, List<String>> boostListMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		sywApiCalls = new SywApiCalls();

		pidDivLnDao = new PidDivLnDao();
		divLnBoostDao = new DivLnBoostDao();
		boostDao = new BoostDao();
		memberBoostsDao = new MemberBoostsDao();
		entityTypes = new ArrayList<String>();
		entityTypes.add("Product");
		divLnBoostVariblesMap = divLnBoostDao.getDivLnBoost();
		List<String> feeds = new ArrayList<String>();
		feeds.add("SYW_LIKE");
		feeds.add("SYW_OWN");
		feeds.add("SYW_WANT");
		boostListMap = boostDao.getBoostsMap(feeds);// Feed prefix
	}

	@Override
	public void execute(Tuple input) {
		String feedType = null;
		// Get l_id", "message", "InteractionType" from parsing bolt
		JsonObject interactionObject = (JsonObject) input
				.getValueByField("message");
		String lId = input.getStringByField("l_id");

		// Create a SYW Interaction object
		Gson gson = new Gson();
		SYWInteraction obj = gson.fromJson(interactionObject,
				SYWInteraction.class);

		/* Ignore interactions that we dont want. */
		/*
		 * We currently process Catalogs: Like, Want, Own Others: Like
		 */

		// Find type of catalog to process, and hence the boost variable
		for (SYWEntity currentEntity : obj.getEntities()) {
			if (currentEntity != null) {
				if ("Catalog".equals(currentEntity.getType())) {
					String catalogType = sywApiCalls
							.getCatalogType(currentEntity.getId());
					LOGGER.debug(catalogType);
					if (catalogType != null) {
						if (catalogType.equals("Own"))
							feedType = "SYW_OWN";
						else if (catalogType.equals("Like"))
							feedType = "SYW_LIKE";
						else if (catalogType.equals("Want"))
							feedType = "SYW_WANT";
					}
				}
			}
		}

		if (feedType == null) {
			LOGGER.info("We process only own, like and want catalogs & SYW Likes");
			outputCollector.fail(input);
			return;
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
                        LOGGER.trace(" div line for " + productId + " are " + divLnObj+ " lid: " + lId);
						if (divLnObj != null) {
							String div = divLnObj.getDiv();
							String divLn = divLnObj.getDivLn();
							// Get the boost variable
							if (divLnBoostVariblesMap.containsKey(div)) {
								for (String b : divLnBoostVariblesMap.get(div)) {
									if (boostListMap.get(feedType).contains(b)) {// If
																					// it
																					// belongs
																					// to
																					// current
																					// feed
										if (!boostValuesMap.containsKey(b)) {
											boostValuesMap.put(b,
													new ArrayList<String>());
										}
										boostValuesMap.get(b).add(productId);
									}
								}
							}
							if (divLnBoostVariblesMap.containsKey(divLn)) {
								for (String b : divLnBoostVariblesMap
										.get(divLn)) {
									if (boostListMap.get(feedType).contains(b)) {// If
																					// it
																					// belongs
																					// to
																					// current
																					// feed
										if (!boostValuesMap.containsKey(b)) {
											boostValuesMap.put(b,
													new ArrayList<String>());
										}
										boostValuesMap.get(b).add(productId);
									}
								}
							}
						}
						// Eg - BOOST_SYW_APP_LIKETCOUNT
						// divLnBoost -
						// d:004 , b:BOOST_SYW_APP_LIKETCOUNT
						// 004                        
                        LOGGER.trace(" boost value map :" + boostValuesMap+ " lid: " + lId);
					}

				}

			}
		}
		// Get the memberBoostVariables for current lid. Consolidate into a map
		Map<String, Map<String, List<String>>> allBoostValuesMap = memberBoostsDao
				.getMemberBoostsValues(lId, boostValuesMap.keySet());
		if (allBoostValuesMap == null) {
			allBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
		}

		for (String b : boostValuesMap.keySet()) {
			if (allBoostValuesMap.containsKey(b)) {
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			} else {
				allBoostValuesMap.put(b, new HashMap<String, List<String>>());
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			}
			variableValueMap.put(b, createJsonDoc(allBoostValuesMap.get(b)));
		}
		if (variableValueMap != null && !variableValueMap.isEmpty()) {
			Type varValueType = new TypeToken<Map<String, String>>() {
			}.getType();
			String varValueString = gson.toJson(variableValueMap, varValueType);
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(input.getValueByField("l_id"));
			listToEmit.add(varValueString);
			listToEmit.add(feedType);
			LOGGER.debug(" @@@ SYW PARSING BOLT EMITTING: " + listToEmit+ " lid: " + lId);
			this.outputCollector.emit(listToEmit);
		}
		this.outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source"));
	}

	private String createJsonDoc(Map<String, List<String>> dateValuesMap) {
		LOGGER.trace("dateValuesMap: " + dateValuesMap);
		// Create string in JSON format to emit
		Gson gson = new Gson();
		Type boostValueType = new TypeToken<Map<String, List<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return gson.toJson(dateValuesMap, boostValueType);
	}

}
