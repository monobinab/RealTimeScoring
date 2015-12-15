package analytics.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import analytics.util.dao.MemberBrowseDao;
import analytics.util.objects.MemberBrowse;

public class BrowseUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(BrowseUtils.class);
	
	SimpleDateFormat dateFormat;
	MemberBrowseDao memberBrowseDao;
	String systemProperty;
	private KafkaUtil kafkaUtil;
	private String browseKafkaTopic;
	
	public BrowseUtils(String systemProperty, String browseKafkaTopic){
		this.systemProperty = systemProperty;
		this.browseKafkaTopic = browseKafkaTopic;
		kafkaUtil= new KafkaUtil(systemProperty);
	}
	
	public MemberBrowse getEntireMemberBrowse(String l_id){
		MemberBrowse memberBrowse = memberBrowseDao.getEntireMemberBrowse(l_id);
		return memberBrowse;
	}
	
	public MemberBrowse getEntireMemberBrowse7DaysInHistory(String l_id){
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		return memberBrowse;
	}
	
	public Map<String, Integer> getPreviousBoostCounts(String l_id, String loyalty_id, Map<String, String> incomingBuSubBuMap, int noOfDays, MemberBrowse memberBrowse){
		LocalDate  date = new LocalDate(new Date());
		Map<String, Integer> prevBoostCountsMap = new HashMap<String, Integer>();
		
		/*If the member has records, iterate through the required number of dates and 
		 *aggregate the counts for every incoming buSubBu from all feedTypes and publish to kafka*/
		if(memberBrowse != null){
	
			Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
			
			for(int i=0; i<noOfDays; i++){
				Date currentdate = date.minusDays(i).toDateMidnight().toDate();
				String stringDate = dateFormat.format(currentdate);
				
				if(dateSpeBuSubBuMap.containsKey(stringDate)){
					Map<String, Map<String, Integer>> buSubBuMap = dateSpeBuSubBuMap.get(stringDate);
					
					for(String buSubBu : incomingBuSubBuMap.keySet()){
 						int pc = 0;
						if(buSubBuMap.containsKey(buSubBu)){
							Map<String, Integer> feedCountsMap = buSubBuMap.get(buSubBu);
							for(String feed : feedCountsMap.keySet()){
								pc = pc + feedCountsMap.get(feed);
							}
							if(!prevBoostCountsMap.containsKey(buSubBu)){
								prevBoostCountsMap.put(buSubBu, pc);
							}
							else{
								int count = prevBoostCountsMap.get(buSubBu) + pc;
								prevBoostCountsMap.put(buSubBu, count);
							}
						}
						
						else{
							if(!prevBoostCountsMap.containsKey(buSubBu)){
								prevBoostCountsMap.put(buSubBu, 0);
							}
						}
					}
				}
			}
		}
		return prevBoostCountsMap;		
	}
	
	
	public void publishToKafka(Set<String> buSubBuListToKafka, String loyalty_id, String l_id, String source, String browseKafkaTopic){
		//publish to kafka
		if(buSubBuListToKafka != null && !buSubBuListToKafka.isEmpty()){
			Map<String, Object> mapToBeSend = new HashMap<String, Object>();
			mapToBeSend.put("memberId", loyalty_id);
			mapToBeSend.put("topology", source);
			mapToBeSend.put("buSubBu", buSubBuListToKafka);
			String jsonToBeSend = new Gson().toJson(mapToBeSend );
			try {
				kafkaUtil.sendKafkaMSGs(jsonToBeSend.toString(), browseKafkaTopic);
			//	System.out.println("to kafka " + buSubBuListToKafka + ", " + source);
				LOGGER.info( "PERSIST " + l_id + " --" + loyalty_id + " published to kafka from " + source + ": " + buSubBuListToKafka);
			} catch (ConfigurationException e) {
				LOGGER.error("Exception in kakfa " + ExceptionUtils.getFullStackTrace(e));
			}
		}
				
	}

}