package analytics.integration;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class TestHelper {
	public static void initializeDBForTests() throws ConfigurationException{
		//Ensure usage of a clean new test DB
		System.setProperty("rtseprod", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		
		DB conn = DBConnection.getDBConnection();
		DBCollection divLnItem = conn.getCollection("divLnItm");
		divLnItem.insert(new BasicDBObject("d", "071" ).append("i", "77380").append("l", "45"));
		DBCollection memberVarCollection = conn.getCollection("memberVariables");
		
		Map<String, Object> memVarsMap = new HashMap<String, Object>()
				{{
					put ("658" , 0.00210061);put(  "216" , 2);put(  "565" , 1);put(  "668" , 0.23744242);put(  "519" , 3);put(  "83" , 731);put(  "426" , 3);put(  "76" , 169);put(  "398" , 1);put(  "233" , 7);put(  "677" , 0.61851136);put(  "679" , 0.59719795);put(  "243" , 0.04);put(  "694" , 1);put(  "657" , 0.01897918);put(  "100" , 248);put(  "377" , 13);put(  "144" , 1.0);put(  "261" , 1);put(  "521" , 12);put(  "242" , 0.03);put(  "344" , 1);put(  "666" , 0.00405381);put(  "376" , 731);put(  "108" , 1);put(  "571" , 1);put(  "364" , 731);put(  "457" , 1);put(  "367" , 34);put(  "189" , 1);put(  "675" , 1.50239644);put(  "397" , 14.99);put(  "149" , 272.99);put(  "568" , 183);put(  "20" , 1);put(  "526" , 1);put(  "348" , 731);put(  "513" , 6);put(  "26" , 1);put(  "429" , 1);put(  "217" , 2);put(  "347" , 731);put(  "361" , 731);put(  "248" , 0.333);put(  "482" , 1);put(  "349" , 731);put(  "380" , 731);put(  "74" , 1);put(  "159" , 0.167);put(  "372" , 13);put(  "129" , 1);put(  "368" , 13);put(  "674" , 0.84056985);put(  "545" , 3650);put(  "128" , 1);put(  "673" , 0.48066943);put(  "676" , 0.80003937);put(  "213" , 1);put(  "552" , 3650);put(  "341" , 1);put(  "79" , 731);put(  "358" , 59);put(  "342" , 1);put(  "351" , 731);put(  "550" , 3650);put(  "362" , 731);put(  "681" , 0.99975499);put(  "340" , 1);put(  "158" , 0.2);put(  "530" , 3650);put(  "191" , 1);put(  "572" , 3650);put(  "515" , 1);put(  "523" , 6);put(  "379" , 731);put(  "148" , 0.38);put(  "555" , 3650);put(  "381" , 731);put(  "212" , 1);put(  "142" , 1);put(  "85" , 731);put(  "29" , 15.99);put(  "299" , 1);put(  "352" , 731);put(  "585" , 43.84);put(  "354" , 75);put(  "95" , 138);put(  "16" , 15.99);put(  "146" , 0.37);put(  "156" , 1);put(  "220" , 1);put(  "345" , 731);put(  "382" , 731);put(  "656" , 0.00106873);put(  "425" , 1);put(  "560" , 3650);put(  "522" , 3);put(  "96" , 731);put(  "366" , 731);put(  "598" , 0.20285714285714285);put(  "91" , 138);put(  "360" , 59);put(  "145" , 0.37);put(  "266" , 147.39);put(  "193" , 1);put(  "86" , 731);put(  "465" , 1);put(  "21" , 272.99);put(  "73" , 1);put(  "295" , 1);put(  "98" , 731);put(  "195" , 1);put(  "78" , 731);put(  "685" , 1.54285132);put(  "589" , 1);put(  "271" , 4);put(  "343" , 9.99);put(  "187" , 1);put(  "542" , 3650);put(  "682" , 0.92947734);put(  "97" , 731);put(  "346" , 731);put(  "143" , 1);put(  "428" , 18.52);put(  "81" , 731);put(  "218" , 1);put(  "84" , 169);put(  "80" , 731);put(  "671" , 0.00825502);put(  "240" , 1);put(  "23" , 272.99);put(  "72" , 1);put(  "15" , 1);put(  "667" , 0.00088447);put(  "102" , 1);put(  "269" , 5);put(  "235" , 1);put(  "661" , 0.00346416);put(  "77" , 248);put(  "680" , 1.35056344);put(  "94" , 731);put(  "466" , 1);put(  "257" , 1);put(  "219" , 1);put(  "566" , 183);put(  "188" , 1);put(  "514" , 1);put(  "75" , 1309);put(  "665" , 0.00243228);put(  "374" , 731);put(  "383" , 731);put(  "375" , 731);put(  "151" , 272.99);put(  "456" , 1);put(  "660" , 0.00423807);put(  "357" , 731);put(  "363" , 731);put(  "162" , 1);put(  "152" , 272.99);put(  "350" , 731);put(  "520" , 1);put(  "27" , 1);put(  "595" , 18);put(  "535" , 7);put(  "373" , 34);put(  "141" , 1);put(  "99" , 248);put(  "686" , 0.99765807);put(  "663" , 0.00556477);put(  "670" , 0.01923715);put(  "88" , 731);put(  "525" , 1);put(  "672" , 0.41461486);put(  "463" , 1);put(  "669" , 0.04201216);put(  "356" , 731);put(  "570" , 3650);put(  "131" , 1);put(  "547" , 3650);put(  "481" , 14.99);put(  "353" , 75);put(  "18" , 1);put(  "664" , 0.04746637);put(  "462" , 1);put(  "87" , 731);put(  "92" , 731);put(  "458" , 4);put(  "558" , 3650);put(  "662" , 0.00095817);put(  "512" , 12);put(  "369" , 13);put(  "245" , 15.99);put(  "93" , 248);put(  "378" , 731);put(  "190" , 1);put(  "90" , 731);put(  "260" , 1);put(  "239" , 1);put(  "516" , 1);put(  "464" , 1);put(  "82" , 731);put(  "455" , 92.02);put(  "430" , 2);put(  "371" , 731);put(  "423" , 18.32);put(  "365" , 731);put(  "221" , 1);put(  "478" , 2);put(  "359" , 731);put(  "517" , 1);put(  "427" , 37.05);put(  "529" , 3650);put(  "588" , 42.0);put(  "531" , 1279);put(  "527" , 1);put(  "424" , 18.32);put(  "19" , 2);put(  "370" , 731);put(  "511" , 1);put(  "683" , 0.66262379);put(  "684" , 0.90186487);put(  "355" , 731);put(  "678" , 0.4948219);put(  "258" , 1);put(  "89" , 731);put(  "659" , 0.00055279);
				}};
			BasicDBObject memVars = new BasicDBObject("l_id","1hGa3VmrRXWbAcwTcw0qw6BfzS4=" );
		for(String key:memVarsMap.keySet()){
			memVars.append(key, memVarsMap.get(key));
		}
		memberVarCollection.insert(memVars);
		
		DBCollection divLnVariableCollection = conn.getCollection("divLnVariable");
		divLnVariableCollection.insert(new BasicDBObject("d", "07145").append("v", "S_DSL_LG_TRS"));
		divLnVariableCollection.insert(new BasicDBObject("d", "07145").append("v", "S_LG_24M_IND"));
		divLnVariableCollection.insert(new BasicDBObject("d", "07145").append("v", "S_LG_3M_IND"));
		divLnVariableCollection.insert(new BasicDBObject("d", "07145").append("v", "m_TRIMMERS_EDGERS_history_flg"));
	
		DBCollection varCollection = conn.getCollection("Variables");
		varCollection.insert(new BasicDBObject("name","S_DSL_LG_TRS").append("VID", 88).append("trs_lvl_fl", 0).append("strategy", "StrategyDaysSinceLast"));
		varCollection.insert(new BasicDBObject("name","S_LG_24M_IND").append("VID", 194).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOnFlag"));
		varCollection.insert(new BasicDBObject("name","S_LG_3M_IND").append("VID", 196).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOnFlag"));
		varCollection.insert(new BasicDBObject("name","M_TRIMMERS_EDGERS_HISTORY_FLG").append("VID", 596).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOnFlag"));
		varCollection.insert(new BasicDBObject("name","S_TOOLS_TL_TRS_3M").append("VID", 290).append("trs_lvl_fl", 0).append("strategy", "StrategyCountTransactions"));
		varCollection.insert(new BasicDBObject("name","M_WEB_DAY_HAND_TOOL_0_7").append("VID", 606).append("trs_lvl_fl", 0).append("strategy", "StrategyCountTraitDates"));
		
		varCollection.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varCollection.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varCollection.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varCollection.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varCollection.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varCollection.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyBoostProductTotalCount"));
		varCollection.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategySumSales"));
		varCollection.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyTurnOffFlag"));
		
		DBCollection modelVarCollection = conn.getCollection("modelVariables");
		String json = "{"+
				"  \"modelId\" : 59,"+
				"  \"modelDescription\" : \"Tools\","+
				"  \"constant\" : -3.0729,"+
				"  \"modelName\" : \"S_SCR_TOOLS\","+
				"  \"month\" : 0,"+
				"  \"variable\" : [{"+
				"      \"coefficient\" : 0.2451,"+
				"      \"name\" : \"S_TOOLS_TL_TRS_3M\""+
				"    }, {"+
				"      \"coefficient\" : 0.5573,"+
				"      \"name\" : \"M_WEB_DAY_HAND_TOOL_0_7\""+
				"    }, {"+
				"      \"coefficient\" : -0.00077,"+
				"      \"name\" : \"S_DSL_LG_TRS\""+
				"    }]"+
				"}";
		DBObject dbObject = (DBObject)JSON.parse(json);
		modelVarCollection.insert(dbObject);
	}
}
