package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.TagMetadataDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PersistOccasionBolt extends BaseRichBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistOccasionBolt.class);
	private MemberMDTagsDao memberMDTagsDao;
	private MultiCountMetric countMetric;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		memberMDTagsDao = new MemberMDTagsDao();
		initMetrics(context);
	}
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }


	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		List<String> tags = new ArrayList<String>();
		String l_id = input.getString(0);
		try {
			String tag = input.getString(1);
			if(tag != null && !tag.isEmpty()){
			String[] tagsArray = tag.split(",");
			tags = Arrays.asList(tagsArray);
			memberMDTagsDao.addMemberMDTags(l_id, tags);
			countMetric.scope("persisted_occasionTags").incr();
			}
			else{
				memberMDTagsDao.deleteMemberMDTags(l_id);
			}
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("persist_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
