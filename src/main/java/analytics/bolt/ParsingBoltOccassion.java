package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.OccasionVariableDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingBoltOccassion extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private TagMetadataDao tagMetadataDao;
	private TagVariableDao tagVariableDao;
	private OccasionVariableDao occasionVariableDao;
	private MultiCountMetric countMetric;
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		tagMetadataDao = new TagMetadataDao();
		tagVariableDao = new TagVariableDao();
		occasionVariableDao = new OccasionVariableDao();
		initMetrics(context);
	}

	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		Map<String, String> variableValueTagsMap = new HashMap<String, String>();
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = parser.parse(input
				.getStringByField("message"));
		JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
		if (lyl_id_no == null) {
			countMetric.scope("null_lid").incr();
			outputCollector.ack(input);
			return;
		} 
		String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
		System.out.println(l_id);
		
		JsonArray tags = (JsonArray) jsonElement.getAsJsonObject().get("tags");
		if (tags != null && tags.size() != 0) {
			for (JsonElement tag : tags) {
				TagMetadata tagMetaData = tagMetadataDao.getDetails(tag
						.getAsString());
				if (tagMetaData != null) {
					String tagVariableValue = occasionVariableDao
							.getValue(tagMetaData);
					String tagVariable = tagVariableDao.getTagVariable(tag
							.getAsString());
					variableValueTagsMap.put(tagVariable, tagVariableValue);
				}
			}
		} else{
			outputCollector.ack(input);
			return;
		}
		List<Object> listToEmit = new ArrayList<Object>();
    	listToEmit.add(l_id);
    	listToEmit.add(JsonUtils.createJsonFromStringStringMap(variableValueTagsMap));
    	listToEmit.add("PurchaseOccasion");
    	this.outputCollector.emit(listToEmit);
    	outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source"));
	}

}
