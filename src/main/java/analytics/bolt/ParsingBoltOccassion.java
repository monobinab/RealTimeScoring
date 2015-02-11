package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import analytics.util.dao.MemberDCDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ParsingBoltOccassion extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltOccassion.class);
	private OutputCollector outputCollector;
	private TagMetadataDao tagMetadataDao;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		tagMetadataDao = new TagMetadataDao();
	}

	@Override
	public void execute(Tuple input) {
		List<String> tagsArray = new ArrayList<String>();
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = parser.parse(input
				.getStringByField("message"));
		JsonElement lid = jsonElement.getAsJsonObject().get("l_id");
		String l_id = lid.getAsString();
		JsonElement tags = jsonElement.getAsJsonObject().get("tags").getAsJsonArray();
		JsonArray tagsJsonArray = tags.getAsJsonArray();
		for (JsonElement tag : tagsJsonArray) {
			tagsArray.add(tag.toString());
			//JsonObject stringTag = tag.getAsJsonObject();
			TagMetadata tagData = tagMetadataDao.getDetails(tag.toString());
		}
		System.out.println(tagsArray);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
