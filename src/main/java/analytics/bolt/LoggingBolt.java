/**
 * 
 */
package analytics.bolt;

import analytics.util.dao.MemberScoreDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingBolt extends EnvironmentBolt {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(LoggingBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private MemberScoreDao memberScoreDao;
	public LoggingBolt() {
	}
	 public LoggingBolt(String systemProperty){
		 super(systemProperty);
		
	 }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
   		this.outputCollector = collector;
		memberScoreDao = new MemberScoreDao();
	}
		/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		//countMetric.scope("incoming_tuples").incr();
		redisCountIncr("incoming_tuples");
		//System.out.println(" %%% scorepublishbolt :" + input);
		String l_id = input.getStringByField("l_id");
		String modelId = input.getStringByField("model");
		String oldScore = memberScoreDao.getMemberScores(l_id).get(modelId);
		String source = input.getStringByField("source");
		Double newScore = input.getDoubleByField("newScore");
		String minExpiry = input.getStringByField("minExpiry");
		String maxExpiry = input.getStringByField("maxExpiry");

		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering logging bolt-" + System.currentTimeMillis());
		LOGGER.info("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+", maxExpiry: "+maxExpiry+", source: " + source);
		//System.out.println("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+", maxExpiry: "+maxExpiry+", source: " + source);

		//countMetric.scope("score_logged").incr();
		redisCountIncr("score_logged");
		outputCollector.ack(input);	}

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
