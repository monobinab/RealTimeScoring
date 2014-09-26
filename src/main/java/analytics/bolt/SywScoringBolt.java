package analytics.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDate;

import analytics.util.JsonUtils;
import analytics.util.dao.BoostDao;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.objects.Change;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SywScoringBolt  extends BaseRichBolt{
	private BoostDao boostDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private MemberScoreDao memberScoreDao;
	
	private Map<String,Integer> boostModelMap;
	private Map<Integer, Map<Integer, Double>> modelPercentileMap;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		boostDao = new BoostDao();
		memberScoreDao = new MemberScoreDao();
		changedMemberScoresDao = new ChangedMemberScoresDao();
		
		boostModelMap = buildBoostModelMap();
		modelPercentileMap = new HashMap<Integer, Map<Integer, Double>>();
	}

	@Override
	public void execute(Tuple input) {
		//l_id="jgjh" , source="SYW_LIKE/OWN/WANT
		//newChangesVarValueMap - similar to strategy bolt
		//current-pid, 2014-09-25-[6],...
		String lId = input.getStringByField("l_id");
		if(lId.equals("dxo0b7SN1eER9shCSj0DX+eSGag=")){
			System.out.println("ME");
		}
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}

		// 2) Create map of new changes from the input
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));
		Map<String,Integer> varToCountMap = new HashMap<String, Integer>();
		for (String variableName : newChangesVarValueMap.keySet()) {
	    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(variableName));
	    	int totalPidCount = 0;
	    	
	    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
		    	for(String v: dateValuesMap.get("current")) {
		    		totalPidCount++;
		    	}
		    	dateValuesMap.remove("current");
		    	if(!dateValuesMap.isEmpty()) {
		    		for(String key: dateValuesMap.keySet()) {
		    			try {
							if(!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(7).toDateMidnight().toDate()))
							for(String v: dateValuesMap.get(key)) {
								totalPidCount+=Integer.valueOf(v);
							}
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (ParseException e) {
							e.printStackTrace();
						}
		    		}
		    	}
	    	}
	    	varToCountMap.put(variableName, totalPidCount);
		}
		//boost_syw... hand_tools_tcount
		//boost_syw... tools_tcount
		//var to Count map - 
		//varname, totalcount across all days
		
		Map<Integer,String> modelIdToScore = new HashMap<Integer, String>();
		Map<String,String> memberScores = memberScoreDao.getMemberScores(lId);
		//Also read and keep changedMemberScores
		for(String variableName:varToCountMap.keySet()){
			//Change dao to take in multiple variable names and return list of modelIds
			Integer modelId = boostModelMap.get(variableName);
			
			
			//TODO: First check changed memberScore 
			//Get changed member score for this modelId
			//If this is null/expired, get the memberScore
			String score = memberScores.get(modelId.toString());
			modelIdToScore.put(modelId,score);
		}
		
		//TODO: Loop through the modelIdToScore map
		//Add scoring logic here
		//varToCount map has the total count for each variable
		
		for(String v: varToCountMap.keySet()) {
			int boostPercetages = 0;
			int modelId = boostModelMap.get(v);
			
			if(varToCountMap.get(v)<=10){
				boostPercetages += ((int) Math.ceil(varToCountMap.get(v) / 2.0))-1;
			} else {
				boostPercetages = 5;
			}
			
			Double maxScore = modelPercentileMap.get(modelId).get(90 + boostPercetages);
			if(Double.valueOf(modelIdToScore.get(modelId)) < maxScore) {
				modelIdToScore.put(modelId, maxScore.toString());
			}
		}
		
		
		
		System.out.println("rescored all models in modelIds list");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	private Map<String, Integer> buildBoostModelMap() {
		
		Map<String, Integer> boostModelMap = new HashMap<String, Integer>();
		
		boostModelMap.put("BOOST_SYW_LIKE_ALL_APP_TCOUNT",21);
		boostModelMap.put("BOOST_SYW_LIKE_AU_BATTERY_TCOUNT",22);
		boostModelMap.put("BOOST_SYW_LIKE_AU_TIRE_TCOUNT",24);
		boostModelMap.put("BOOST_SYW_LIKE_AUTO_TCOUNT",25);
		boostModelMap.put("BOOST_SYW_LIKE_BABY_TCOUNT",26);
		boostModelMap.put("BOOST_SYW_LIKE_CE_TCOUNT",27);
		boostModelMap.put("BOOST_SYW_LIKE_CE_CAMERA_TCOUNT",28);
		boostModelMap.put("BOOST_SYW_LIKE_CE_LAPTOP_TCOUNT",29);
		boostModelMap.put("BOOST_SYW_LIKE_FIT_EQUIP_TCOUNT",30);
		boostModelMap.put("BOOST_SYW_LIKE_FITNESS_TCOUNT",31);
		boostModelMap.put("BOOST_SYW_LIKE_FNJL_TCOUNT",32);
		boostModelMap.put("BOOST_SYW_LIKE_FOOTWEAR_TCOUNT",33);
		boostModelMap.put("BOOST_SYW_LIKE_HA_ALL_TCOUNT",34);
		boostModelMap.put("BOOST_SYW_LIKE_HA_COOK_TCOUNT",35);
		boostModelMap.put("BOOST_SYW_LIKE_HA_DISH_TCOUNT",36);
		boostModelMap.put("BOOST_SYW_LIKE_HA_VAC_TCOUNT",37);
		boostModelMap.put("BOOST_SYW_LIKE_HA_WH_TCOUNT",38);
		boostModelMap.put("BOOST_SYW_LIKE_HAND_TOOLS_TCOUNT",39);
		boostModelMap.put("BOOST_SYW_LIKE_HE_FS_TCOUNT",40);
		boostModelMap.put("BOOST_SYW_LIKE_HF_TCOUNT",41);
		boostModelMap.put("BOOST_SYW_LIKE_HG_HL_TCOUNT",42);
		boostModelMap.put("BOOST_SYW_LIKE_HOME_TCOUNT",43);
		boostModelMap.put("BOOST_SYW_LIKE_KAPP_TCOUNT",44);
		boostModelMap.put("BOOST_SYW_LIKE_LE_TCOUNT",46);
		boostModelMap.put("BOOST_SYW_LIKE_LG_ADAS_TCOUNT",47);
		boostModelMap.put("BOOST_SYW_LIKE_LG_MOWER_TCOUNT",48);
		boostModelMap.put("BOOST_SYW_LIKE_LG_PATIO_TCOUNT",49);
		boostModelMap.put("BOOST_SYW_LIKE_LG_TRACTOR_TCOUNT",50);
		boostModelMap.put("BOOST_SYW_LIKE_LG_TRIM_EDG_TCOUNT",51);
		boostModelMap.put("BOOST_SYW_LIKE_MAPP_TCOUNT",52);
		boostModelMap.put("BOOST_SYW_LIKE_MATTRESS_TCOUNT",53);
		boostModelMap.put("BOOST_SYW_LIKE_OD_GRILL_TCOUNT",54);
		boostModelMap.put("BOOST_SYW_LIKE_ODL_TCOUNT",55);
		boostModelMap.put("BOOST_SYW_LIKE_POWER_TOOLS_TCOUNT",56);
		boostModelMap.put("BOOST_SYW_LIKE_REGRIG_TCOUNT",57);
		boostModelMap.put("BOOST_SYW_LIKE_TOOL_STRG_TCOUNT",58);
		boostModelMap.put("BOOST_SYW_LIKE_TOOLS_TCOUNT",59);
		boostModelMap.put("BOOST_SYW_LIKE_TOYS_TCOUNT",60);
		boostModelMap.put("BOOST_SYW_LIKE_TV_TCOUNT",61);
		boostModelMap.put("BOOST_SYW_LIKE_WAPP_TCOUNT",62);
		boostModelMap.put("BOOST_SYW_LIKE_WASH_DRY_TCOUNT",63);
		boostModelMap.put("BOOST_SYW_LIKE_AC_TCOUNT",72);
		boostModelMap.put("BOOST_SYW_LIKE_LG_SM_OTH_TCOUNT",73);
		boostModelMap.put("BOOST_SYW_LIKE_GAME_ROOM_TCOUNT",74);
		boostModelMap.put("BOOST_SYW_LIKE_CHRISTMAS_TCOUNT",75);
		boostModelMap.put("BOOST_SYW_LIKE_LG_SNOW_BLOWER_TCOUNT",76);
		boostModelMap.put("BOOST_SYW_LIKE_GAMER_TCOUNT",77);
		boostModelMap.put("BOOST_SYW_WANT_ALL_APP_TCOUNT",21);
		boostModelMap.put("BOOST_SYW_WANT_AU_BATTERY_TCOUNT",22);
		boostModelMap.put("BOOST_SYW_WANT_AU_TIRE_TCOUNT",24);
		boostModelMap.put("BOOST_SYW_WANT_AUTO_TCOUNT",25);
		boostModelMap.put("BOOST_SYW_WANT_BABY_TCOUNT",26);
		boostModelMap.put("BOOST_SYW_WANT_CE_TCOUNT",27);
		boostModelMap.put("BOOST_SYW_WANT_CE_CAMERA_TCOUNT",28);
		boostModelMap.put("BOOST_SYW_WANT_CE_LAPTOP_TCOUNT",29);
		boostModelMap.put("BOOST_SYW_WANT_FIT_EQUIP_TCOUNT",30);
		boostModelMap.put("BOOST_SYW_WANT_FITNESS_TCOUNT",31);
		boostModelMap.put("BOOST_SYW_WANT_FNJL_TCOUNT",32);
		boostModelMap.put("BOOST_SYW_WANT_FOOTWEAR_TCOUNT",33);
		boostModelMap.put("BOOST_SYW_WANT_HA_ALL_TCOUNT",34);
		boostModelMap.put("BOOST_SYW_WANT_HA_COOK_TCOUNT",35);
		boostModelMap.put("BOOST_SYW_WANT_HA_DISH_TCOUNT",36);
		boostModelMap.put("BOOST_SYW_WANT_HA_VAC_TCOUNT",37);
		boostModelMap.put("BOOST_SYW_WANT_HA_WH_TCOUNT",38);
		boostModelMap.put("BOOST_SYW_WANT_HAND_TOOLS_TCOUNT",39);
		boostModelMap.put("BOOST_SYW_WANT_HE_FS_TCOUNT",40);
		boostModelMap.put("BOOST_SYW_WANT_HF_TCOUNT",41);
		boostModelMap.put("BOOST_SYW_WANT_HG_HL_TCOUNT",42);
		boostModelMap.put("BOOST_SYW_WANT_HOME_TCOUNT",43);
		boostModelMap.put("BOOST_SYW_WANT_KAPP_TCOUNT",44);
		boostModelMap.put("BOOST_SYW_WANT_LE_TCOUNT",46);
		boostModelMap.put("BOOST_SYW_WANT_LG_ADAS_TCOUNT",47);
		boostModelMap.put("BOOST_SYW_WANT_LG_MOWER_TCOUNT",48);
		boostModelMap.put("BOOST_SYW_WANT_LG_PATIO_TCOUNT",49);
		boostModelMap.put("BOOST_SYW_WANT_LG_TRACTOR_TCOUNT",50);
		boostModelMap.put("BOOST_SYW_WANT_LG_TRIM_EDG_TCOUNT",51);
		boostModelMap.put("BOOST_SYW_WANT_MAPP_TCOUNT",52);
		boostModelMap.put("BOOST_SYW_WANT_MATTRESS_TCOUNT",53);
		boostModelMap.put("BOOST_SYW_WANT_OD_GRILL_TCOUNT",54);
		boostModelMap.put("BOOST_SYW_WANT_ODL_TCOUNT",55);
		boostModelMap.put("BOOST_SYW_WANT_POWER_TOOLS_TCOUNT",56);
		boostModelMap.put("BOOST_SYW_WANT_REGRIG_TCOUNT",57);
		boostModelMap.put("BOOST_SYW_WANT_TOOL_STRG_TCOUNT",58);
		boostModelMap.put("BOOST_SYW_WANT_TOOLS_TCOUNT",59);
		boostModelMap.put("BOOST_SYW_WANT_TOYS_TCOUNT",60);
		boostModelMap.put("BOOST_SYW_WANT_TV_TCOUNT",61);
		boostModelMap.put("BOOST_SYW_WANT_WAPP_TCOUNT",62);
		boostModelMap.put("BOOST_SYW_WANT_WASH_DRY_TCOUNT",63);
		boostModelMap.put("BOOST_SYW_WANT_AC_TCOUNT",72);
		boostModelMap.put("BOOST_SYW_WANT_LG_SM_OTH_TCOUNT",73);
		boostModelMap.put("BOOST_SYW_WANT_GAME_ROOM_TCOUNT",74);
		boostModelMap.put("BOOST_SYW_WANT_CHRISTMAS_TCOUNT",75);
		boostModelMap.put("BOOST_SYW_WANT_LG_SNOW_BLOWER_TCOUNT",76);
		boostModelMap.put("BOOST_SYW_WANT_GAMER_TCOUNT",77);
		boostModelMap.put("BOOST_SYW_OWN_ALL_APP_TCOUNT",21);
		boostModelMap.put("BOOST_SYW_OWN_AU_BATTERY_TCOUNT",22);
		boostModelMap.put("BOOST_SYW_OWN_AU_TIRE_TCOUNT",24);
		boostModelMap.put("BOOST_SYW_OWN_AUTO_TCOUNT",25);
		boostModelMap.put("BOOST_SYW_OWN_BABY_TCOUNT",26);
		boostModelMap.put("BOOST_SYW_OWN_CE_TCOUNT",27);
		boostModelMap.put("BOOST_SYW_OWN_CE_CAMERA_TCOUNT",28);
		boostModelMap.put("BOOST_SYW_OWN_CE_LAPTOP_TCOUNT",29);
		boostModelMap.put("BOOST_SYW_OWN_FIT_EQUIP_TCOUNT",30);
		boostModelMap.put("BOOST_SYW_OWN_FITNESS_TCOUNT",31);
		boostModelMap.put("BOOST_SYW_OWN_FNJL_TCOUNT",32);
		boostModelMap.put("BOOST_SYW_OWN_FOOTWEAR_TCOUNT",33);
		boostModelMap.put("BOOST_SYW_OWN_HA_ALL_TCOUNT",34);
		boostModelMap.put("BOOST_SYW_OWN_HA_COOK_TCOUNT",35);
		boostModelMap.put("BOOST_SYW_OWN_HA_DISH_TCOUNT",36);
		boostModelMap.put("BOOST_SYW_OWN_HA_VAC_TCOUNT",37);
		boostModelMap.put("BOOST_SYW_OWN_HA_WH_TCOUNT",38);
		boostModelMap.put("BOOST_SYW_OWN_HAND_TOOLS_TCOUNT",39);
		boostModelMap.put("BOOST_SYW_OWN_HE_FS_TCOUNT",40);
		boostModelMap.put("BOOST_SYW_OWN_HF_TCOUNT",41);
		boostModelMap.put("BOOST_SYW_OWN_HG_HL_TCOUNT",42);
		boostModelMap.put("BOOST_SYW_OWN_HOME_TCOUNT",43);
		boostModelMap.put("BOOST_SYW_OWN_KAPP_TCOUNT",44);
		boostModelMap.put("BOOST_SYW_OWN_LE_TCOUNT",46);
		boostModelMap.put("BOOST_SYW_OWN_LG_ADAS_TCOUNT",47);
		boostModelMap.put("BOOST_SYW_OWN_LG_MOWER_TCOUNT",48);
		boostModelMap.put("BOOST_SYW_OWN_LG_PATIO_TCOUNT",49);
		boostModelMap.put("BOOST_SYW_OWN_LG_TRACTOR_TCOUNT",50);
		boostModelMap.put("BOOST_SYW_OWN_LG_TRIM_EDG_TCOUNT",51);
		boostModelMap.put("BOOST_SYW_OWN_MAPP_TCOUNT",52);
		boostModelMap.put("BOOST_SYW_OWN_MATTRESS_TCOUNT",53);
		boostModelMap.put("BOOST_SYW_OWN_OD_GRILL_TCOUNT",54);
		boostModelMap.put("BOOST_SYW_OWN_ODL_TCOUNT",55);
		boostModelMap.put("BOOST_SYW_OWN_POWER_TOOLS_TCOUNT",56);
		boostModelMap.put("BOOST_SYW_OWN_REGRIG_TCOUNT",57);
		boostModelMap.put("BOOST_SYW_OWN_TOOL_STRG_TCOUNT",58);
		boostModelMap.put("BOOST_SYW_OWN_TOOLS_TCOUNT",59);
		boostModelMap.put("BOOST_SYW_OWN_TOYS_TCOUNT",60);
		boostModelMap.put("BOOST_SYW_OWN_TV_TCOUNT",61);
		boostModelMap.put("BOOST_SYW_OWN_WAPP_TCOUNT",62);
		boostModelMap.put("BOOST_SYW_OWN_WASH_DRY_TCOUNT",63);
		boostModelMap.put("BOOST_SYW_OWN_AC_TCOUNT",72);
		boostModelMap.put("BOOST_SYW_OWN_LG_SM_OTH_TCOUNT",73);
		boostModelMap.put("BOOST_SYW_OWN_GAME_ROOM_TCOUNT",74);
		boostModelMap.put("BOOST_SYW_OWN_CHRISTMAS_TCOUNT",75);
		boostModelMap.put("BOOST_SYW_OWN_LG_SNOW_BLOWER_TCOUNT",76);
		boostModelMap.put("BOOST_SYW_OWN_GAMER_TCOUNT",77);

		return boostModelMap;
		
	}
	
	
	
	
}
