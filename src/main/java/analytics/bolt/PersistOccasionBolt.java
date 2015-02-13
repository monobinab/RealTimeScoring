package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.TagMetadataDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PersistOccasionBolt extends BaseRichBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistOccasionBolt.class);
	private MemberMDTagsDao memberMDTagsDao;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		memberMDTagsDao = new MemberMDTagsDao();
	}

	@Override
	public void execute(Tuple input) {
		List<String> tags = new ArrayList<String>();
		String l_id = input.getString(0);
		try {
			JSONArray tagsArray = new JSONArray(input.getString(1));
			for(int i=0; i<tagsArray.length(); i++){
				tags.add(tagsArray.getString(i));
			}
			memberMDTagsDao.addMemberMDTags(l_id, tags);
		} catch (JSONException e) {
			LOGGER.error("Json Exception ", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
