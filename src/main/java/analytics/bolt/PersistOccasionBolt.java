package analytics.bolt;

import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PersistOccasionBolt extends EnvironmentBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistOccasionBolt.class);
	private MemberMDTagsDao memberMDTagsDao;
	private MemberMDTags2Dao memberMDTags2Dao;
	private OutputCollector outputCollector;
	 public PersistOccasionBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberMDTagsDao = new MemberMDTagsDao();
		memberMDTags2Dao = new MemberMDTags2Dao();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
	//	System.out.println("IN PERSIST BOLT: " + input);
		redisCountIncr("PersistOccasionBolt_begin_count");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.debug("TIME:" + messageID + "-Entering PersistOccasionbolt-" + System.currentTimeMillis());
		
		//LOGGER.info("~~~~~~~~~~Incoming tuple in PersistOccasionbolt: " + input);
		countMetric.scope("incoming_tuples").incr();
		List<String> tags = new ArrayList<String>();
		String l_id = input.getString(0);
		try {
			String tag = input.getString(1);
			if(tag != null && !tag.isEmpty()){
				String[] tagsArray = tag.split(",");
				tags = Arrays.asList(tagsArray);
				memberMDTagsDao.addMemberMDTags(l_id, tags);
				
				//Write to the new collection as well...
				//memberMDTags2Dao.addMemberMDTags(l_id, tags);
				
				LOGGER.info("PERSIST OCCATION UPDATE: " + l_id + "~"+tags);
				countMetric.scope("persisted_occasionTags").incr();
			}
			else{
				memberMDTagsDao.deleteMemberMDTags(l_id);
				LOGGER.info("PERSIST OCCATION DELETE: " + l_id);
			}
			
			//Emit the tuples to scoring bolt only if there are changes to the Variables
			/*if(input.getString(2)!=null && input.getString(2).trim().length()>0){
				List<Object> listToEmit = new ArrayList<Object>();
					listToEmit.add(l_id);
					listToEmit.add(input.getString(2));
			    	listToEmit.add(input.getString(3));
			    	listToEmit.add(input.getString(4));
			    	listToEmit.add(messageID);
			    	outputCollector.emit(listToEmit);
			    	countMetric.scope("tags_with_rescoring").incr();
			}else{*/
				
				if (input.contains("lyl_id_no")) {
					String lyl_id_no = input.getStringByField("lyl_id_no");
					List<Object> listToEmit = new ArrayList<Object>();
					listToEmit = new ArrayList<Object>();
					listToEmit.add(lyl_id_no);
					listToEmit.add(messageID);
					this.outputCollector.emit("response_stream_from_persist", listToEmit);
					countMetric.scope("tags_without_rescoring").incr();
				}
				else{
					countMetric.scope("no_lyl_id_no").incr();
				}
				
			//}
			LOGGER.debug("TIME:" + messageID + "-Exiting PersistOccasionbolt-" + System.currentTimeMillis());
			outputCollector.ack(input);
			countMetric.scope("persisted_successfully").incr();
			redisCountIncr("PersistOccasionBolt_end_count");
			
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("persist_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//declarer.declare(new Fields("l_id", "lineItemAsJsonString","source","lyl_id_no", "messageID"));
			declarer.declareStream("response_stream_from_persist", new Fields("lyl_id_no","messageID"));
		}

}
