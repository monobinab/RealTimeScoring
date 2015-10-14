package analytics.bolt;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.dao.MemberBrowseDao;
import analytics.util.objects.DateSpecificMemberBrowse;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BrowseCountPersistBolt extends EnvironmentBolt{
	
	static final Logger LOGGER = LoggerFactory
			.getLogger(BrowseCountPersistBolt_Old.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
	Map<String, String> sourceMap = new HashMap<String, String>();
	private String source;
	private String todayDate;
	MemberBrowseDao memberBrowseDao;
	private LocalDate date;
	SimpleDateFormat dateFormat;
	
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
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        todayDate = dateFormat.format(new Date());
        date = new LocalDate(new Date());
 	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		Map<String, Integer> incomingBuSubBuMap = JsonUtils.restoreTagsListFromJson(input.getString(1));
		System.out.println(incomingBuSubBuMap);
		String l_id = input.getStringByField("l_id");
			
		Map<String, Integer> buSubBuCountsMap = new HashMap<String, Integer>();
	 
		MemberBrowse memberBrowse = memberBrowseDao.getEntireMemberBrowse(l_id);
		if(memberBrowse == null ){
		
			insertMemberBrowseToday(l_id, incomingBuSubBuMap);
			
			/*DateSpecificMemberBrowse dateSpeMemberBrowse = new DateSpecificMemberBrowse();
			Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
			 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
				 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
				 newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
				 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
			 }
			 dateSpeMemberBrowse.setBuSubBu(browseBuSubBufeedCountsMap);
			 updateMemberBrowse( dateSpeMemberBrowse, l_id);*/
		}
		else{
			
			Map<String, DateSpecificMemberBrowse> memberBrowseMap = memberBrowse.getMemberBrowse();
		
			for(int i=0; i<5; i++){
				Date currentdate = date.minusDays(i).toDateMidnight().toDate();
				String stringDate = dateFormat.format(currentdate);
				
				if(memberBrowseMap.containsKey(stringDate)){
					DateSpecificMemberBrowse dateSpecificMemberBrowse = memberBrowseMap.get(stringDate);
					
					for(String buSubBu : incomingBuSubBuMap.keySet()){
						int pc = 0;
						if(dateSpecificMemberBrowse.getBuSubBu().containsKey(buSubBu)){
							Map<String, Integer> feedCountsMap = dateSpecificMemberBrowse.getBuSubBu().get(buSubBu);
							for(String feed : feedCountsMap.keySet()){
								pc = pc + feedCountsMap.get(feed);
							}
							if(!buSubBuCountsMap.containsKey(buSubBu)){
								buSubBuCountsMap.put(buSubBu, pc);
							}
							else{
								int count = buSubBuCountsMap.get(buSubBu) + pc;
								buSubBuCountsMap.put(buSubBu, count);
							}
						}
						
						else{
							if(!buSubBuCountsMap.containsKey(buSubBu)){
								buSubBuCountsMap.put(buSubBu, 0);
							}
						}
					}
				}
			}
				emitToKafka(incomingBuSubBuMap, buSubBuCountsMap);
				if(memberBrowseMap.containsKey(todayDate)){
					updateMemberBrowseToday(incomingBuSubBuMap, l_id, memberBrowseMap);
				}
				else{
					insertMemberBrowseToday(l_id, incomingBuSubBuMap);
				}
				
		}
	}

	private void updateMemberBrowseToday(Map<String, Integer> incomingBuSubBuMap,
			String l_id, Map<String, DateSpecificMemberBrowse> memberBrowseMap) {
		if(memberBrowseMap.containsKey(todayDate)){
		DateSpecificMemberBrowse dateSpecificMemberBrowse = memberBrowseMap.get(todayDate);
		for(String browseTag : incomingBuSubBuMap.keySet()){
			//if the member already had the incoming tag, check whether the current source has a count
			if(dateSpecificMemberBrowse.getBuSubBu().keySet().contains(browseTag)){
				Map<String, Integer> feedCountsMap = dateSpecificMemberBrowse.getBuSubBu().get(browseTag);
				
				//if the current source has a count, add the incoming tag count to the existing one 
				if(feedCountsMap.containsKey(sourceMap.get(source))){
					int count = feedCountsMap.get(sourceMap.get(source)) + incomingBuSubBuMap.get(browseTag);
					feedCountsMap.put(sourceMap.get(source), count);
				}
				
				//if the current source does not have a count, create a map for it
				else{
					feedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseTag));
				}
			}
			
			//if the member does not have the incoming tag, create a document for the tag
			else{
				Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
				newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseTag));
				dateSpecificMemberBrowse.getBuSubBu().put(browseTag, newFeedCountsMap);
			}
		}
			updateMemberBrowse( dateSpecificMemberBrowse, l_id);
		}
		/*else{
			DateSpecificMemberBrowse dateSpecificMemberBrowse = new DateSpecificMemberBrowse();
			Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
			 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
				 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
				 newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
				 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
			 }
		
			 dateSpecificMemberBrowse.setBuSubBu(browseBuSubBufeedCountsMap);
			 updateMemberBrowse( dateSpecificMemberBrowse, l_id);
		}*/
	}
	
	public void insertMemberBrowseToday(String l_id, Map<String, Integer> incomingBuSubBuMap){
		DateSpecificMemberBrowse dateSpecificMemberBrowse = new DateSpecificMemberBrowse();
		Map<String, Map<String, Integer>> browseBuSubBufeedCountsMap = new HashMap<String, Map<String,Integer>>();
		 for(String browseBuSubBu : incomingBuSubBuMap.keySet()){
			 Map<String, Integer> newFeedCountsMap = new HashMap<String, Integer>();
			 newFeedCountsMap.put(sourceMap.get(source), incomingBuSubBuMap.get(browseBuSubBu));
			 browseBuSubBufeedCountsMap.put(browseBuSubBu, newFeedCountsMap);
		 }
	
		 dateSpecificMemberBrowse.setBuSubBu(browseBuSubBufeedCountsMap);
		 updateMemberBrowse( dateSpecificMemberBrowse, l_id);
	}
	
	public void updateMemberBrowse(DateSpecificMemberBrowse dateSpecificMemberBrowse, String l_id){
		dateSpecificMemberBrowse.setDate(todayDate);
		memberBrowseDao.updateMemberBrowse(l_id, dateSpecificMemberBrowse);
	}
	
	public void emitToKafka(Map<String, Integer> incomingBuSubBuMap, Map<String, Integer> buSubBuCountsMap){
		List<Object> listToEmitToKafka = new ArrayList<Object>(); 
		if(!buSubBuCountsMap.isEmpty()){
			for(String buSubBu : buSubBuCountsMap.keySet()){
				int PC = buSubBuCountsMap.get(buSubBu);
				int IC = incomingBuSubBuMap.get(buSubBu);
				if(PC < 4 && (PC + IC) >= 4){
					listToEmitToKafka.add(buSubBu);
				}
			}
			
		}
		
		else{
			for(String buSubBu : incomingBuSubBuMap.keySet()){
				if(incomingBuSubBuMap.get(buSubBu) >= 4){
					listToEmitToKafka.add(buSubBu);
				}
			}
		}
		
		if(!listToEmitToKafka.isEmpty()){
			//code to kafka
		}
		
	}

}
