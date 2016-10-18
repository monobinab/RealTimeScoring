package analytics.bolt;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import analytics.util.CalendarUtil;
import analytics.util.SingletonJsonParser;
import analytics.util.dao.IsKeyWordModelDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.DivLn;
import analytics.util.objects.KeyWordModelCode;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParsingBoltAAM_InternalSearch extends ParseAAMFeeds {
	private static final long serialVersionUID = 1L;
	private PidDivLnDao pidDivLnDao;
	private IsKeyWordModelDao isKeyWordModelDao;

	public ParsingBoltAAM_InternalSearch (String systemProperty, String topic) {
		super(systemProperty, topic);
	
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		pidDivLnDao = new PidDivLnDao();
		isKeyWordModelDao = new IsKeyWordModelDao();
    }

	private String[] splitKeyWords(String keyWords) {
        String split[]=StringUtils.split(keyWords,"+");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}
    
    public static boolean isJSONValid(String str) {
        try {
        	Gson gson = SingletonJsonParser.getInstance().getGsonInstance();
            gson.fromJson(str, Object.class);
            return true;
        } catch(com.google.gson.JsonSyntaxException ex) { 
        	LOGGER.info("String too big: " + str);
        	LOGGER.info(ex.getMessage());
            return false;
        }
    }
        	
	@Override
	protected Map<String, String> processList(String current_l_id,
			 Map<String, Collection<String>> l_idToCurrentPidCollectionMap) {
		
		Map<String, String> incomingModelCodeMap = new HashMap<String, String>();
	   /*	String queryResultsDoc = new String();
    	Set<String> pidSet = new HashSet<String>();*/ //MOVED INSIDE THE FOR LOOP FOR EVERY SEARCH TERM
    	Collection<String> searchStringsCollection = l_idToCurrentPidCollectionMap.get(current_l_id); 
    	if(searchStringsCollection==null || searchStringsCollection.isEmpty()|| (searchStringsCollection.toArray())[0].toString().trim().equalsIgnoreCase(""))
    		return null;
    	
    	LOGGER.info(current_l_id + " has " + searchStringsCollection.size() + " searchTerms");
	    	try{
	    		
		    	for(String searchString : searchStringsCollection) { 
		    		KeyWordModelCode keyWordModelCode = isKeyWordModelDao.getModelCodeFromSearchString(searchString.toLowerCase());
		    		Date todayDate = new Date();
		    		Date oneMonthDate = null;
		    		if(keyWordModelCode != null ){
		    			oneMonthDate = CalendarUtil.getRequiredDate(keyWordModelCode.getDate(), 2); //make it as 2 months
		    		}
	    			if(oneMonthDate != null && (todayDate.before(oneMonthDate) || todayDate.equals(oneMonthDate)) && keyWordModelCode.getModelCodesList() != null && keyWordModelCode.getModelCodesList().size() > 0){
		    			for(String modelCode : keyWordModelCode.getModelCodesList()){ //modelCodelsList null check
			    				populateIncomingModelCodeMap(incomingModelCodeMap, modelCode);
			    		}
		    		}
		    	   	else{
		    	   		Thread.sleep(10);
		    	   		String queryResultsDoc = new String();
		    	    	Set<String> pidSet = new HashSet<String>();
		    	   		getPidsFromSolr(queryResultsDoc, pidSet, searchString);
		    	   		Set<String> modelCodeSet = new HashSet<String> ();
						if(pidSet != null && !pidSet.isEmpty()){
					    	for(String pid: pidSet) {
					    		DivLn divLnObj = pidDivLnDao.getDivLnFromPid(pid);
					    		if(divLnObj != null) {
						    		String div = divLnObj.getDiv();
						    		String divLn = divLnObj.getDivLn();
						    	    getIncomingModelCodeMapIS(div, incomingModelCodeMap, modelCodeSet);
						    		getIncomingModelCodeMapIS(divLn, incomingModelCodeMap, modelCodeSet);
					    		}
					    	}
					    	 if(modelCodeSet != null && modelCodeSet.size() > 0){
					    		 LOGGER.info("PERSIST: " + searchString + " updated in isKeyWordmodel coll " +  new  Date());
					    		 isKeyWordModelDao.addModelCodesForSearchStrings(searchString.toLowerCase(), modelCodeSet, CalendarUtil.getDateFormat().format(todayDate));
					    	 }
					    }
		    	   	}
		    	}
			}
			catch(Exception e){
				LOGGER.error("Exception in processList of ParsingBoltIS: " + e);
			}
	    	return incomingModelCodeMap;
	    }

	private Set<String> getPidsFromSolr(String queryResultsDoc, Set<String> pidSet,
			String searchString) {
		String[] search = splitKeyWords(searchString);
		//CONSTRUCT URL - queries Solr
		//String URL1 = "http://solrx308p.stress.ch3.s.com:8180/search/select?qt=search&wt=json&q=";
		//String URL1 = "http://solrx-prod.prod.ch4.s.com:80/search/select?qt=search&wt=json&clientID=sywAnalytics&q=";
		String URL1 = "http://sears-solrx.prod.global.s.com:80/search/select?qt=search&wt=json&clientID=sywAnalytics&q=";
		String URL2 = "&start=0&rows=10&fq=catalogs:%28%2212605%22%29&sort=instock%20desc,score%20desc,revenue%20desc&sortPrefix=L6;S4;10153&globalPrefix=L6,S4,10153&spuAvailability=S4&lmpAvailability=L6&fvCutoff=22&fqx=!%28storeAttributes:%28%2210153_DEFAULT_FULFILLMENT=SPU%22%29%20AND%20storeOrigin:%28%22Kmart%22%29%29&site=prod";
		String query = new String();
		query = URL1;
		StringBuilder sb_query = new StringBuilder(query);
		int countKeyWords=0;
		for(int i=0; search!=null&& i< search.length; i++){
			
			//check if the search key is null
			if(search[i] == null)
				continue;
			
			if(!search[i].equalsIgnoreCase("N/A")) {
				countKeyWords++;
				if(countKeyWords==1) {
					sb_query.append(search[i]);
				}
				else {
					sb_query.append("%20");
					sb_query.append(search[i]);
				}
			}
		}
		query = sb_query.toString();
		query = query + URL2;
		
		if(countKeyWords>0) {
		
			try {
				//System.out.println(query);
				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					LOGGER.debug("Unable to wait",e);
				}
				long t1 = System.currentTimeMillis();
				Document doc = Jsoup.connect(query).get();
				long t2 = System.currentTimeMillis() - t1;
				LOGGER.debug(" @@@ Query time: " + t2);
				doc.body().wrap("<pre></pre>");
				String text = doc.text();
				// Converting nbsp entities
				text = text.replaceAll("\u00A0", " ");
				
				queryResultsDoc = text;
			} catch (IOException e) {
				LOGGER.debug("Unable to process keywords",e);
			}
			if(queryResultsDoc==null) {
				LOGGER.debug("query results null");
			}
			
			else {
				if(isJSONValid(queryResultsDoc)) {
					if(new JsonParser().parse(queryResultsDoc).isJsonObject()) {
						JsonObject queryResultsToJson = new JsonParser().parse(queryResultsDoc).getAsJsonObject();
						if(queryResultsToJson.get("response")!=null && queryResultsToJson.get("response").isJsonObject()) {
							JsonObject response = queryResultsToJson.get("response").getAsJsonObject();
							if(response.get("docs").isJsonArray()) {
								JsonArray docs = response.getAsJsonArray("docs").getAsJsonArray();
								for(JsonElement doc:docs){
									if(doc.isJsonObject() && doc.getAsJsonObject().get("partnumber") != null) {
										pidSet.add(doc.getAsJsonObject().get("partnumber").toString().replace("\"", ""));
									}
								}
							}
						}
					}
				}
			}
		}
		return pidSet;
	}
	
	@Override
	protected String[] splitRec(String webRec) {
		//webRec = webRec.replaceAll("[']","");
        String split[]=StringUtils.split(webRec,",");
        if(split !=null && split.length>0) {
        	return split;
		}
		else {
			return null;
		}
	}
	
	protected void getIncomingModelCodeMapIS(String div, Map<String, String> incomingModelCodeMap, Set<String> modelCodeSet){
		Map<String, List<String>> divLnModelCodeMap = divLnModelCodeDao.getDivLnModelCode();
		if (divLnModelCodeMap.containsKey(div)) {
			modelCodeSet.addAll(divLnModelCodeMap.get(div));
			for (String modelCode : divLnModelCodeMap.get(div)) {
				populateIncomingModelCodeMap(incomingModelCodeMap, modelCode);
			}
		}
	}
}
