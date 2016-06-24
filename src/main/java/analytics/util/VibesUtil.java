/**
 * 
 */
package analytics.util;

import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.EventsVibesActiveDao;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.TagMetadata;
import redis.clients.jedis.Jedis;

/**
 * @author spannal
 *
 */
public class VibesUtil {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VibesUtil.class);

	private static VibesUtil instance = null;
	private EventsVibesActiveDao eventsVibesActiveDao;
	
	public static VibesUtil getInstance() throws ConfigurationException {
		if (instance == null) {
			synchronized (VibesUtil.class) {
				if (instance == null)
					instance = new VibesUtil();
			}
		}
		return instance;
	}
	
	public VibesUtil() {
		
		eventsVibesActiveDao = new EventsVibesActiveDao();
	}
	
	
	public TagMetadata getTagMetaDataInfo(com.google.gson.JsonObject obj){
		TagMetadata tagMetaData = null;

		if(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).has("mdTag") && ((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).has("occassion") &&
				((com.google.gson.JsonObject)obj.getAsJsonArray("scoresInfo").get(0)).get("occassion").toString().equalsIgnoreCase("Unknown"))
			return null;
		
		tagMetaData = new TagMetadata();
		tagMetaData.setPurchaseOccassion(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("occassion")!= null ? 
				((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("occassion").getAsString() : null);
		tagMetaData.setBusinessUnit(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("businessUnit")!= null ? 
				((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("businessUnit").getAsString() : null);
		tagMetaData.setSubBusinessUnit(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("subBusinessUnit")!= null ? 
				((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("subBusinessUnit").getAsString() : null);
		tagMetaData.setMdTag(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("mdTag")!= null ? 
				((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("mdTag").getAsString() : null);
		tagMetaData.setFirst5CharMdTag(((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("mdTag")!= null ? 
				((com.google.gson.JsonObject) obj.getAsJsonArray("scoresInfo").get(0)).get("mdTag").getAsString().substring(0,5) : null);

		return tagMetaData;
	}
	
	public boolean publishVibesToRedis(TagMetadata tagMetadata, String host, int port, String lyl_id_no){
		
		Jedis jedis = null;
		StringBuilder custVibesEvent = new StringBuilder();
		
		if(tagMetadata!=null && tagMetadata.getPurchaseOccasion()!=null && 
				tagMetadata.getTextOptIn()!=null && tagMetadata.getTextOptIn().equals("Y") && 
				isVibesActiveWithEvent(tagMetadata.getPurchaseOccasion(),tagMetadata.getFirst5CharMdTag(),custVibesEvent)){
			
			jedis = new Jedis(host, port, 1800);
			jedis.connect();
			jedis.set("Vibes:"+lyl_id_no, custVibesEvent.toString());
			jedis.disconnect();
			custVibesEvent = null;
			return true;
		}
		custVibesEvent = null;
		return false;
	}
	
	private boolean isVibesActiveWithEvent(String occasion, String bussUnit, StringBuilder custVibesEvent){
		HashMap<String, HashMap<String, String>> eventVibesActiveMap = eventsVibesActiveDao.getVibesActiveEventsList();
		if(eventVibesActiveMap.get(occasion)!= null){
			if(eventVibesActiveMap.get(occasion).get(bussUnit)!=null)
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(bussUnit));
			else
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(null));
		}
		
		//Log the info incase Vibes isn;t ready with the occasion and BU
		if(custVibesEvent.toString().isEmpty() || custVibesEvent.toString().equals("null"))
			LOGGER.info("Vibes is not ready for Occasion "+occasion+ " for BU "+bussUnit);
		
		return (!custVibesEvent.toString().equals("null") && !custVibesEvent.toString().isEmpty());
	}

	public void getTextInfo(MemberInfo memberInfo, TagMetadata tagMetaData){
		
		if(tagMetaData!=null && memberInfo!=null)
			tagMetaData.setTextOptIn(memberInfo.getText_opt_in());
	}
}
