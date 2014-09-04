/**
 * 
 */
package analytics.bolt;

import analytics.util.DBConnection;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RankPublishBolt extends BaseRichBolt {

	static final Logger logger = Logger
			.getLogger(RankPublishBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    final String host;
    final int port;
    final String pattern;


    DB db;
    DBCollection memberZipCollection;
    DBCollection memberScoreCollection;


    private Map<String,Collection<Integer>> variableModelsMap;
    private Map<String, String> variableVidToNameMap;
    private Map<String,String> modelIdToModelNameMap;
    private Map<Integer,Double> haAllRankToScoreMap;
    private Map<Integer,Double> haCookRankToScoreMap;

    private Jedis jedis;


    public RankPublishBolt(String host, int port, String pattern) {
        this.host = host;
        this.port = port;
        this.pattern = pattern;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        jedis = new Jedis(host, port);
        this.outputCollector = collector;


        //prepare mongo

        try {
			db = DBConnection.getDBConnection();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        memberZipCollection = db.getCollection("memberZip");
        memberScoreCollection = db.getCollection("memberScore");
        
        modelIdToModelNameMap = new HashMap<String,String>();
        modelIdToModelNameMap.put("34", "S_SCR_HA_ALL");
        modelIdToModelNameMap.put("35", "S_SCR_HA_COOK");
        
        haAllRankToScoreMap = new HashMap<Integer,Double>();
        haAllRankToScoreMap.put(100,0.0505114);
        haAllRankToScoreMap.put(99,0.0373944);
        haAllRankToScoreMap.put(98,0.0305787);
        haAllRankToScoreMap.put(97,0.0261711);
        haAllRankToScoreMap.put(96,0.0229111);
        haAllRankToScoreMap.put(95,0.0204238);
        haAllRankToScoreMap.put(94,0.0184591);
		haAllRankToScoreMap.put(93,0.0168337);
		haAllRankToScoreMap.put(92,0.0154968);
		haAllRankToScoreMap.put(91,0.0141645);
		haAllRankToScoreMap.put(90,0.0133917);
		haAllRankToScoreMap.put(89,0.0124964);
		haAllRankToScoreMap.put(88,0.0116958);
		haAllRankToScoreMap.put(87,0.0109911);
		haAllRankToScoreMap.put(86,0.0103994);
		haAllRankToScoreMap.put(85,0.0098879);
		haAllRankToScoreMap.put(84,0.0094214);
		haAllRankToScoreMap.put(83,0.0089848);
		haAllRankToScoreMap.put(82,0.0085667);
		haAllRankToScoreMap.put(81,0.0082520);
		haAllRankToScoreMap.put(80,0.0078921);
		haAllRankToScoreMap.put(79,0.0075510);
		haAllRankToScoreMap.put(78,0.0072064);
		haAllRankToScoreMap.put(77,0.0068667);
		haAllRankToScoreMap.put(76,0.0065846);
		haAllRankToScoreMap.put(75,0.0063311);
		haAllRankToScoreMap.put(74,0.0060524);
		haAllRankToScoreMap.put(73,0.0058954);
		haAllRankToScoreMap.put(72,0.0057135);
		haAllRankToScoreMap.put(71,0.0054958);
		haAllRankToScoreMap.put(70,0.0053038);
		haAllRankToScoreMap.put(69,0.0051398);
		haAllRankToScoreMap.put(68,0.0049782);
		haAllRankToScoreMap.put(67,0.0048221);
		haAllRankToScoreMap.put(66,0.0047228);
		haAllRankToScoreMap.put(65,0.0046643);
		haAllRankToScoreMap.put(64,0.0046035);
		haAllRankToScoreMap.put(63,0.0045208);
		haAllRankToScoreMap.put(62,0.0044262);
		haAllRankToScoreMap.put(61,0.0042822);
		haAllRankToScoreMap.put(60,0.0041855);
		haAllRankToScoreMap.put(59,0.0040995);
		haAllRankToScoreMap.put(58,0.0039508);
		haAllRankToScoreMap.put(57,0.0037773);
		haAllRankToScoreMap.put(56,0.0036318);
		haAllRankToScoreMap.put(55,0.0034309);
		haAllRankToScoreMap.put(54,0.0032704);
		haAllRankToScoreMap.put(53,0.0032704);
		haAllRankToScoreMap.put(52,0.0032704);
		haAllRankToScoreMap.put(51,0.0032299);
		haAllRankToScoreMap.put(50,0.0032299);
		haAllRankToScoreMap.put(49,0.0032299);
		haAllRankToScoreMap.put(48,0.0032299);
		haAllRankToScoreMap.put(47,0.0031898);
		haAllRankToScoreMap.put(46,0.0031898);
		haAllRankToScoreMap.put(45,0.0031503);
		haAllRankToScoreMap.put(44,0.0031112);
		haAllRankToScoreMap.put(43,0.0030711);
		haAllRankToScoreMap.put(42,0.0029968);
		haAllRankToScoreMap.put(41,0.0029230);
		haAllRankToScoreMap.put(40,0.0029230);
		haAllRankToScoreMap.put(39,0.0029230);
		haAllRankToScoreMap.put(38,0.0028513);
		haAllRankToScoreMap.put(37,0.0027280);
		haAllRankToScoreMap.put(36,0.0026272);
		haAllRankToScoreMap.put(35,0.0025545);
		haAllRankToScoreMap.put(34,0.0024359);
		haAllRankToScoreMap.put(33,0.0023176);
		haAllRankToScoreMap.put(32,0.0021992);
		haAllRankToScoreMap.put(31,0.0020741);
		haAllRankToScoreMap.put(30,0.0019220);
		haAllRankToScoreMap.put(29,0.0018409);
		haAllRankToScoreMap.put(28,0.0018409);
		haAllRankToScoreMap.put(27,0.0018409);
		haAllRankToScoreMap.put(26,0.0018409);
		haAllRankToScoreMap.put(25,0.0018409);
		haAllRankToScoreMap.put(24,0.0018409);
		haAllRankToScoreMap.put(23,0.0018409);
		haAllRankToScoreMap.put(22,0.0018409);
		haAllRankToScoreMap.put(21,0.0018409);
		haAllRankToScoreMap.put(20,0.0018181);
		haAllRankToScoreMap.put(19,0.0018181);
		haAllRankToScoreMap.put(18,0.0018181);
		haAllRankToScoreMap.put(17,0.0018181);
		haAllRankToScoreMap.put(16,0.0018181);
		haAllRankToScoreMap.put(15,0.0018181);
		haAllRankToScoreMap.put(14,0.0018181);
		haAllRankToScoreMap.put(13,0.0018181);
		haAllRankToScoreMap.put(12,0.0017955);
		haAllRankToScoreMap.put(11,0.0017955);
		haAllRankToScoreMap.put(10,0.0017955);
		haAllRankToScoreMap.put(9,0.0017732);
		haAllRankToScoreMap.put(8,0.0017512);
		haAllRankToScoreMap.put(7,0.0017294);
		haAllRankToScoreMap.put(6,0.0017080);
		haAllRankToScoreMap.put(5,0.0016658);
		haAllRankToScoreMap.put(4,0.0016451);
		haAllRankToScoreMap.put(3,0.0016451);
		haAllRankToScoreMap.put(2,0.0016451);
		haAllRankToScoreMap.put(1,0.0000000);

		haCookRankToScoreMap = new HashMap<Integer,Double>();
		haCookRankToScoreMap.put(100,0.0201874);
		haCookRankToScoreMap.put(99,0.0140853);
		haCookRankToScoreMap.put(98,0.0111994);
		haCookRankToScoreMap.put(97,0.0094564);
		haCookRankToScoreMap.put(96,0.0082712);
		haCookRankToScoreMap.put(95,0.0074150);
		haCookRankToScoreMap.put(94,0.0067081);
		haCookRankToScoreMap.put(93,0.0061456);
		haCookRankToScoreMap.put(92,0.0056877);
		haCookRankToScoreMap.put(91,0.0052407);
		haCookRankToScoreMap.put(90,0.0049624);
		haCookRankToScoreMap.put(89,0.0046704);
		haCookRankToScoreMap.put(88,0.0044195);
		haCookRankToScoreMap.put(87,0.0041938);
		haCookRankToScoreMap.put(86,0.0039663);
		haCookRankToScoreMap.put(85,0.0038113);
		haCookRankToScoreMap.put(84,0.0036301);
		haCookRankToScoreMap.put(83,0.0034667);
		haCookRankToScoreMap.put(82,0.0033545);
		haCookRankToScoreMap.put(81,0.0031946);
		haCookRankToScoreMap.put(80,0.0030798);
		haCookRankToScoreMap.put(79,0.0029429);
		haCookRankToScoreMap.put(78,0.0028291);
		haCookRankToScoreMap.put(77,0.0027115);
		haCookRankToScoreMap.put(76,0.0026081);
		haCookRankToScoreMap.put(75,0.0025101);
		haCookRankToScoreMap.put(74,0.0024209);
		haCookRankToScoreMap.put(73,0.0023492);
		haCookRankToScoreMap.put(72,0.0023422);
		haCookRankToScoreMap.put(71,0.0022534);
		haCookRankToScoreMap.put(70,0.0021727);
		haCookRankToScoreMap.put(69,0.0021020);
		haCookRankToScoreMap.put(68,0.0020263);
		haCookRankToScoreMap.put(67,0.0019567);
		haCookRankToScoreMap.put(66,0.0018940);
		haCookRankToScoreMap.put(65,0.0018499);
		haCookRankToScoreMap.put(64,0.0018016);
		haCookRankToScoreMap.put(63,0.0017406);
		haCookRankToScoreMap.put(62,0.0016927);
		haCookRankToScoreMap.put(61,0.0016347);
		haCookRankToScoreMap.put(60,0.0015827);
		haCookRankToScoreMap.put(59,0.0015290);
		haCookRankToScoreMap.put(58,0.0014827);
		haCookRankToScoreMap.put(57,0.0014465);
		haCookRankToScoreMap.put(56,0.0014127);
		haCookRankToScoreMap.put(55,0.0013794);
		haCookRankToScoreMap.put(54,0.0013430);
		haCookRankToScoreMap.put(53,0.0013159);
		haCookRankToScoreMap.put(52,0.0012910);
		haCookRankToScoreMap.put(51,0.0012809);
		haCookRankToScoreMap.put(50,0.0012758);
		haCookRankToScoreMap.put(49,0.0012422);
		haCookRankToScoreMap.put(48,0.0012051);
		haCookRankToScoreMap.put(47,0.0011739);
		haCookRankToScoreMap.put(46,0.0011467);
		haCookRankToScoreMap.put(45,0.0011218);
		haCookRankToScoreMap.put(44,0.0010922);
		haCookRankToScoreMap.put(43,0.0010634);
		haCookRankToScoreMap.put(42,0.0010354);
		haCookRankToScoreMap.put(41,0.0010083);
		haCookRankToScoreMap.put(40,0.0009832);
		haCookRankToScoreMap.put(39,0.0009595);
		haCookRankToScoreMap.put(38,0.0009310);
		haCookRankToScoreMap.put(37,0.0009060);
		haCookRankToScoreMap.put(36,0.0008865);
		haCookRankToScoreMap.put(35,0.0008622);
		haCookRankToScoreMap.put(34,0.0008350);
		haCookRankToScoreMap.put(33,0.0008162);
		haCookRankToScoreMap.put(32,0.0007888);
		haCookRankToScoreMap.put(31,0.0007582);
		haCookRankToScoreMap.put(30,0.0007306);
		haCookRankToScoreMap.put(29,0.0007044);
		haCookRankToScoreMap.put(28,0.0006779);
		haCookRankToScoreMap.put(27,0.0006549);
		haCookRankToScoreMap.put(26,0.0006374);
		haCookRankToScoreMap.put(25,0.0006231);
		haCookRankToScoreMap.put(24,0.0006115);
		haCookRankToScoreMap.put(23,0.0005956);
		haCookRankToScoreMap.put(22,0.0005793);
		haCookRankToScoreMap.put(21,0.0005674);
		haCookRankToScoreMap.put(20,0.0005552);
		haCookRankToScoreMap.put(19,0.0005422);
		haCookRankToScoreMap.put(18,0.0005289);
		haCookRankToScoreMap.put(17,0.0005165);
		haCookRankToScoreMap.put(16,0.0005054);
		haCookRankToScoreMap.put(15,0.0004940);
		haCookRankToScoreMap.put(14,0.0004848);
		haCookRankToScoreMap.put(13,0.0004757);
		haCookRankToScoreMap.put(12,0.0004669);
		haCookRankToScoreMap.put(11,0.0004600);
		haCookRankToScoreMap.put(10,0.0004528);
		haCookRankToScoreMap.put(9,0.0004457);
		haCookRankToScoreMap.put(8,0.0004383);
		haCookRankToScoreMap.put(7,0.0004280);
		haCookRankToScoreMap.put(6,0.0004183);
		haCookRankToScoreMap.put(5,0.0004057);
		haCookRankToScoreMap.put(4,0.0003962);
		haCookRankToScoreMap.put(3,0.0003686);
		haCookRankToScoreMap.put(2,0.0003158);
		haCookRankToScoreMap.put(1,0.0000000);

    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		logger.info("The time it enters inside Score Publish Bolt execute method"+System.currentTimeMillis());
        //System.out.println(" %%% scorepublishbolt :" + input);
        String l_id = input.getStringByField("l_id");
        DBObject row = memberZipCollection.findOne(new BasicDBObject("l_id", l_id));
        DBObject oldScoreResult = memberScoreCollection.findOne(new BasicDBObject("l_id", l_id));
        String modelName = modelIdToModelNameMap.get(input.getStringByField("model"));
        String oldScore = new String();
        
        if(oldScoreResult != null && modelName != null) {
        	oldScore = oldScoreResult.get(modelName).toString();
        }
        else {
        	oldScore = "0";
        }
        
        int oldRank = 0;
        int newRank = 0;
        String modelDescription = new String();
        if(modelName.equals("S_SCR_HA_ALL")) {
        	modelDescription="HOME APPLIANCE";
        	if(Double.valueOf(oldScore)==0) {
        		oldRank=1;
        	}
        	else {
	        	for(int i=1; i<=100; i++) {
	        		if(Double.valueOf(oldScore)>=haAllRankToScoreMap.get(i)) {
	        			oldRank=i;
	        		}
	        	}
        	}
        	if(oldRank==0) oldRank=1;
        	
        	if(Double.valueOf(input.getDoubleByField("newScore"))==0) {
        		newRank=1;
        	}
        	else {
	        	for(int i=1; i<=100; i++) {
	        		if(Double.valueOf(input.getDoubleByField("newScore"))>=haAllRankToScoreMap.get(i)) {
	        			newRank=i;
	        		}
	        	}
        	}
        	if(newRank==0) newRank=1;
        }
        else if (modelName.equals("S_SCR_HA_COOK")) {
        	modelDescription="COOK TOPS";
        	if(Double.valueOf(oldScore)==0) {
        		oldRank=1;
        	}
        	else {
	        	for(int i=1; i<=100; i++) {
	        		if(Double.valueOf(oldScore)>=haCookRankToScoreMap.get(i)) {
	        			oldRank=i;
	        		}
	        	}
        	}
        	if(oldRank==0) oldRank=1;
        	
        	if(Double.valueOf(input.getDoubleByField("newScore"))==0) {
        		newRank=1;
        	}
        	else {
	        	for(int i=1; i<=100; i++) {
	        		if(Double.valueOf(input.getDoubleByField("newScore"))>=haCookRankToScoreMap.get(i)) {
	        			newRank=i;
	        		}
	        	}
        	}
        	if(newRank==0) newRank=1;
        }
        else {
        	modelDescription="MODEL NOT FOUND";
        }
        
        String dataSource = new String();
        if(input.getStringByField("source").equals("NPOS")) {
        	dataSource = "In-store Sales";
        }
        else {
        	dataSource = "Web Browse";
        }
        Object zip = row==null?"00000":row.get("z");
        //System.out.println(" %%% zip zip :" + ObjectUtils.toString(zip));
        //System.out.println(" %%% old score: " + oldScore + "  new score: " + input.getDoubleByField("newScore"));

        if (row != null && StringUtils.isNotEmpty(ObjectUtils.toString(zip)))
            {
                String message = new StringBuffer(l_id).append(",")
                        .append(String.valueOf(oldRank)).append(",")
                        .append(String.valueOf(newRank)).append(",")
                        .append(modelDescription).append(",")
                        .append(dataSource).append(",")
                        .append(zip.toString()).toString();
                //System.out.println(" %%% message : " + message);

                jedis.publish(pattern, message);
            }
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
	}

}
