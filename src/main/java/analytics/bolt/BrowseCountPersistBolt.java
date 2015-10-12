package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.dao.MemberBrowseDao;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BrowseCountPersistBolt extends EnvironmentBolt{
	
	static final Logger LOGGER = LoggerFactory
			.getLogger(BrowseCountPersistBolt.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
	Map<String, String> sourceMap = new HashMap<String, String>();
	private String source;
	private String todayDate;
	MemberBrowseDao memberBrowseDao;
	
	public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
	
	public BrowseCountPersistBolt(String systemProperty, String source) {
		super(systemProperty);
		this.source = source;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
        sourceMap.put("SB", "SG");
        memberBrowseDao = new MemberBrowseDao();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        todayDate = dateFormat.format(new Date());
	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, Integer> incomingTagsMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		System.out.println(incomingTagsMap);
		String l_id = input.getStringByField("l_id");
		MemberBrowse memberBrowse = new MemberBrowse();
		
		//get the browseTags for today 
		memberBrowse = memberBrowseDao.getMemberBrowse(l_id, todayDate);
	
		/*BrowseTags are created from the incoming feed, if the member does not have browseTags for the required date (today ideally)*/
		if(memberBrowse.getTags() == null){
			 Map<String, Map<String, Integer>> browseTagfeedCountsMap = new HashMap<String, Map<String,Integer>>();
			 for(String browseTag : incomingTagsMap.keySet()){
				 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
				 newFeedCountsMap.put(sourceMap.get(source), incomingTagsMap.get(browseTag));
				 browseTagfeedCountsMap.put(browseTag, newFeedCountsMap);
			 }
			 memberBrowse.setTags(browseTagfeedCountsMap);
		}
		
		//BrowseTags are updated for the incoming feed, if the member has browseTags for the required date (today)
		else{
			for(String browseTag : incomingTagsMap.keySet()){
				
				//if the member already had the incoming tag, check whether the current source has a count
				if(memberBrowse.getTags().keySet().contains(browseTag)){
					Map<String, Integer> feedCountsMap = memberBrowse.getTags().get(browseTag);
					
					//if the current source has a count, add the incoming tag count to the existing one 
					if(feedCountsMap.containsKey(sourceMap.get(source))){
						int count = feedCountsMap.get(sourceMap.get(source)) + incomingTagsMap.get(browseTag);
						feedCountsMap.put(sourceMap.get(source), count);
					}
					
					//if the current source does not have a count, create a map for it
					else{
						feedCountsMap.put(sourceMap.get(source), incomingTagsMap.get(browseTag));
					}
				}
				
				//if the member does not have the incoming tag, create a document for the tag
				else{
					Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
					newFeedCountsMap.put(sourceMap.get(source), incomingTagsMap.get(browseTag));
					memberBrowse.getTags().put(browseTag, newFeedCountsMap);
				}
			}
		}	
			memberBrowseDao.updateMemberBrowse( memberBrowse);
		}
	}
