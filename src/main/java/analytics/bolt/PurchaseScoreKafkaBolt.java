/**
 * 
 */
package analytics.bolt;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import analytics.util.ScoringUtils;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.ChangedMemberScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PurchaseScoreKafkaBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PurchaseScoreKafkaBolt.class);
	private static final long serialVersionUID = 1L;
	// private static final String KAFKA_MSG="message";
	private OutputCollector outputCollector;
	private String currentTopic;
	private KafkaUtil kafkaUtil;
	private TagVariableDao tagVariableDao;
	private Set<Integer> models;
	private ScoringUtils scoringUtils;

	// private String env;

	public PurchaseScoreKafkaBolt(String environment, String topic) {
		super(environment);
		this.currentTopic = topic;
	}
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		tagVariableDao = new TagVariableDao();
		models = tagVariableDao.getModels();
		scoringUtils = new ScoringUtils();
		this.outputCollector = collector;
		kafkaUtil = new KafkaUtil(
				System.getProperty(MongoNameConstants.IS_PROD));
		// KafkaUtil.initiateKafkaProperties(System.getProperty(MongoNameConstants.IS_PROD));
		LOGGER.info("PurchaseScoreKafkaBolt Preparing to Launch");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		try{
			JSONObject mainJsonObj = new JSONObject();
			if (input.contains("loyaltyId")
					&& input.getValueByField("loyaltyId") != null && input.contains("cpsScoreMessage")) {
				List<ChangedMemberScore> changedMemberScoreList = (List<ChangedMemberScore>) input
						.getValueByField("cpsScoreMessage");
				String loyId = input.getStringByField("loyaltyId");
				if (changedMemberScoreList != null
						&& !changedMemberScoreList.isEmpty()) {
					String topology = input.getStringByField("topology");
					mainJsonObj.put("memberId", loyId);
					mainJsonObj.put("topology", topology);
					JSONArray jsonArray = new JSONArray();
						for (ChangedMemberScore changedMemScore : changedMemberScoreList) {
								if (models.contains(Integer
										.parseInt(changedMemScore.getModelId()))) {
									JSONObject jsonObj = new JSONObject();
									jsonObj.put("modelId",
											changedMemScore.getModelId());
									jsonObj.put("score", changedMemScore.getScore());
									jsonObj.put("percentile", scoringUtils
											.getPercentileForScore(changedMemScore
													.getScore(), Integer
													.parseInt(changedMemScore
															.getModelId())));
									jsonArray.add(jsonObj);
								}
							}
							mainJsonObj.put("scoresInfo", jsonArray);
	
							if (mainJsonObj != null
									&& !"".equals(mainJsonObj.toJSONString())) {
								try{
									kafkaUtil.sendKafkaMSGs(mainJsonObj.toJSONString(),
											currentTopic);
									redisCountIncr("tag_to_kafka");
									LOGGER.info("PERSIST: " +  loyId + " published to kafka from " + topology);
								}
								catch (ConfigurationException e) {
									LOGGER.error(e.getMessage(), e);
									outputCollector.fail(input);
								}
							}
						}
						else {
							LOGGER.error("No data to send to kafka for this member "
									+ loyId);
						}
					}
				}
				catch(Exception e){
					LOGGER.error("Exception in PurchaseScoreKafkaBolt " + e.getMessage());
				}
				outputCollector.ack(input);
			}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
