package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.spout.TwitterRedisSpout;
import analytics.util.dao.FacebookLoyaltyIdDao;
import analytics.util.dao.SocialVariableDao;
import analytics.util.dao.TwitterLoyaltyIdDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class SocialBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SocialBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private FacebookLoyaltyIdDao facebookLoyaltyIdDao;
	private TwitterLoyaltyIdDao twitterLoyaltyIdDao;
	private SocialVariableDao socialVariableDao;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		twitterLoyaltyIdDao = new TwitterLoyaltyIdDao();
		facebookLoyaltyIdDao = new FacebookLoyaltyIdDao();
		socialVariableDao = new SocialVariableDao();
	}

	@Override
	public void execute(Tuple input) {
		String source = (String)input.getValueByField("source");
		String message = (String) input.getValueByField("message");
		String[] messageArray = message.split(",");
		String id = messageArray[1];
		String positive = messageArray[2];
		String negative = messageArray[3];
		Double score = Double.parseDouble(positive.substring(1,
				positive.indexOf(']')))
				- Double.parseDouble(negative.substring(1,
						negative.indexOf(']')));
		String topic = messageArray[4];

		String lId = null;
		if("facebook".equals(source))
			lId = facebookLoyaltyIdDao.getLoyaltyIdFromID(id);
		else
			lId = twitterLoyaltyIdDao.getLoyaltyIdFromID(id);
		if (lId != null) {
			LOGGER.debug("Received " + score + " for " + lId);
			// Find the list of variables that are affected by the model
			// mentioned - We need the FB string to variable mapping
			// How is model name to variable mapping?? - BOOST vars are added to
			// the respective models
			Gson gson = new Gson();
			Map<String, String> variableValueMap = new HashMap<String, String>();
			variableValueMap.put(
					socialVariableDao.getVariableFromTopic(topic),
					score.toString());
			Type varValueType = new TypeToken<Map<String, String>>() {
			}.getType();
			String varValueString = gson.toJson(variableValueMap, varValueType);
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lId);
			listToEmit.add(varValueString);
			listToEmit.add(source);
			LOGGER.debug(" @@@ FB PARSING BOLT EMITTING: " + listToEmit);
			this.collector.emit(listToEmit);
		} else {
			LOGGER.warn("Unable to find l_id for " + id);
		}
		// Create a boost. new variable in model variables colelction
		// BOOST_*
		// Insert the variable into Variables
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "fbScores", "source"));
	}

}
