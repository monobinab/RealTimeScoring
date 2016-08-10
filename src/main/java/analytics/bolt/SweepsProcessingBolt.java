package analytics.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import analytics.util.SecurityUtils;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.objects.TagVariable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class SweepsProcessingBolt extends EnvironmentBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TagProcessingBolt.class);
	private OutputCollector outputCollector;
	Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
	Map<String, String> modelScoreMap = new HashMap<String, String>();
	private MemberMDTags2Dao memberMDTags2Dao;
	private CpsOccasionsDao cpsOccasion;

	public SweepsProcessingBolt(String systemProperty) {
		super(systemProperty);

	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberMDTags2Dao = new MemberMDTags2Dao();
		cpsOccasion = new CpsOccasionsDao();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		redisCountIncr("SweepsProcessingBolt_input_count");	
		try {
			LOGGER.info("~~~~~~~~~~~Incoming tuple in SweepsProcessingBolt: " + input);
			System.out.println("~~~~~~~~~~~Incoming tuple in SweepsProcessingBolt: " + input);
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = (JsonElement) parser.parse((String)input.getValue(0));
			if(jsonElement != null){
				JsonElement lylIdElement = jsonElement.getAsJsonObject().get("lyl_id_no");
				if(lylIdElement != null && StringUtils.isNotEmpty(lylIdElement.getAsString())){
					String l_id = SecurityUtils.hashLoyaltyId(lylIdElement.getAsString());
					JsonArray tagsArray =  (JsonArray) jsonElement.getAsJsonObject().get("tags");
					if(tagsArray != null && tagsArray.size() > 0){
						StringBuilder tagsBuilder = new StringBuilder();
						for (int i = 0; i < tagsArray.size(); i++) {
							tagsBuilder.append(tagsArray.get(i).getAsString());
							if (i != tagsArray.size() - 1)
								tagsBuilder.append(",");
						}
						if(tagsBuilder != null && tagsBuilder.length() > 0){
							String[] tagsStrArray = tagsBuilder.toString().split(",");
							if(tagsStrArray != null && tagsStrArray.length > 0){
							memberMDTags2Dao.addRtsMemberTags(l_id, Arrays.asList(tagsStrArray), cpsOccasion.getcpsOccasionDurations(), cpsOccasion.getcpsOccasionPriority());
							}
						}
					}
				}
			}
			redisCountIncr("SweepsProcessingBolt_output_count");	
		} catch (Exception e) {
			LOGGER.error("exception in SweepsProcessingBolt: " + e);
		} 
		outputCollector.ack(input);
	}

	
	public void buildTagsString(String l_id,
			StringBuilder tagsString, JsonArray tags) {
		for (int i = 0; i < tags.size(); i++) {
			tagsString.append(tags.get(i).getAsString());
			if (i != tags.size() - 1)
				tagsString.append(",");
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no", "l_id"));
	}
}

