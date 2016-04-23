/**
 * 
 */
package analytics.bolt;

import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.objects.ChangedMemberScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
	private ModelPercentileDao modelPercentileDao;
	
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
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
   		this.outputCollector = collector;
		memberScoreDao = new MemberScoreDao();
		modelPercentileDao = new ModelPercentileDao();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		
		redisCountIncr("incoming_tuples");
		List<ChangedMemberScore> changedMemberScores = null;
		if(input.contains("changedMemberScoresList")){
			changedMemberScores = (List<ChangedMemberScore>)input.getValueByField("changedMemberScoresList");
		}
	
		if(changedMemberScores != null && changedMemberScores.size() > 0){
			Map<Integer,TreeMap<Integer,Double>> modelScorePercentileMap = modelPercentileDao.getModelScorePercentilesMap();
			for(ChangedMemberScore changedMemberScore : changedMemberScores){
				
				if(changedMemberScore != null){
					try{
						String l_id = changedMemberScore.getlId();
						String modelId = changedMemberScore.getModelId();
						String oldScore = memberScoreDao.getMemberScores(l_id).get(modelId);
						String source = changedMemberScore.getSource();
						Double newScore = changedMemberScore.getScore();
						String minExpiry = changedMemberScore.getMinDate();
						String maxExpiry = changedMemberScore.getMaxDate();
	
						Integer newPercentile = getPercentileForScore(modelScorePercentileMap, newScore,Integer.parseInt(modelId));
						Integer oldPercentile = (oldScore == null ? null : getPercentileForScore(modelScorePercentileMap, new Double (oldScore),Integer.parseInt(modelId)));
	
						String messageID = "";
						if (input.contains("messageID")) {
							messageID = input.getStringByField("messageID");
						}
						LOGGER.info("TIME:" + messageID + "-Entering logging bolt-" + System.currentTimeMillis());
						LOGGER.info("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +
								", newScore: "+newScore+", minExpiry: "+minExpiry+", maxExpiry: "+maxExpiry+", source: " + source+", "
										+ "oldPercentile: " + oldPercentile+", newPercentile: " + newPercentile);
						redisCountIncr("score_logged");
						outputCollector.ack(input);	
					}
					catch(Exception e){
						LOGGER.error("Exception in Logging bolt for " + changedMemberScore.getlId() + " model " + changedMemberScore.getModelId());
					}
				}
			}
		}
		outputCollector.ack(input);
	}

	/**
	private HashMap<Integer,TreeMap<Integer,Double>> getModelScorePercentileMap(){
		HashMap<Integer,TreeMap<Integer,Double>> modelScorePercentileMap = modelPercentileDao.getModelScorePercentilesMap();
		return modelScorePercentileMap;
	}*/
	
	private int getPercentileForScore(Map<Integer,TreeMap<Integer,Double>> modelScorePercentileMap, double score, int modelId){
		TreeMap<Integer,Double> percMap = modelScorePercentileMap.get(modelId);
		int p = 0;
		if (percMap != null && percMap.size() > 0) {
			for (int i = percMap.size() - 1; i >= 1; i--) {
				if (score > percMap.get(i)) {
					p = i + 1;
					break;
				}
			}
			if (p == 0) {
				p = 1;
			}
		}
		return p;
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
