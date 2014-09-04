package analytics.bolt;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.MongoUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingBoltAAM_InternalSearch extends ParseAAMFeeds {
	/**
	 * Created by Rock Wasserman 6/24/2014
	 */

	DBCollection pidDivLnCollection;
    DBCollection divLnVariableCollection;

    private Map<String,Collection<String>> divLnVariablesMap;
    

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */

		sourceTopic = "InternalSearch";
        pidDivLnCollection = db.getCollection("pidDivLn");
        divLnVariableCollection = db.getCollection("divLnVariable");

        
        // populate divLnVariablesMap
        divLnVariablesMap = new HashMap<String, Collection<String>>();
        DBCursor divLnVarCursor = divLnVariableCollection.find();
        for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnVariablesMap.get(divLnDBObject.get("d")) == null)
            {
                Collection<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get("v").toString());
                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
            }
            else
            {
                Collection<String> varColl = divLnVariablesMap.get(divLnDBObject.get("d").toString());
                varColl.add(divLnDBObject.get("v").toString().toUpperCase());
                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
            }
        }
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


    private boolean isJSONValid(String test) {
    	 
        try {
            new JSONObject(test);
            return true;
        } catch (JSONException ex) {
            try {
                new JSONArray(test);
                return true;
            } catch (JSONException ex1) {
                //System.out.println(test);
                return false;
            }
        }
    }
        	
	private boolean isNumbers(String s) {
		for(int i=0;i<s.length();i++) {
			if(!s.substring(i, i+1).equals("0")
					&& !s.substring(i, i+1).equals("1")
					&& !s.substring(i, i+1).equals("2")
					&& !s.substring(i, i+1).equals("3")
					&& !s.substring(i, i+1).equals("4")
					&& !s.substring(i, i+1).equals("5")
					&& !s.substring(i, i+1).equals("6")
					&& !s.substring(i, i+1).equals("7")
					&& !s.substring(i, i+1).equals("8")
					&& !s.substring(i, i+1).equals("9")
			) return false;
		}
		return true;
	}


	/*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
     * topology.OutputFieldsDeclarer)
     */

	@Override
	protected Map<String, String> processList(String current_l_id) {
	    	
    	String queryResultsDoc = new String();
    	Set<String> pidSet = new HashSet<String>();
    	Collection<String> searchStringsCollection = l_idToValueCollectionMap.get(current_l_id);
    	
    	for(String searchString : searchStringsCollection) {
	    	String[] search = splitKeyWords(searchString);
	    	
	    	//CONSTRUCT URL - queries Solr
			String URL1 = "http://solrx308p.stress.ch3.s.com:8180/search/select?qt=search&wt=json&q=";
			String URL2 = "&start=0&rows=50&fq=catalogs:%28%2212605%22%29&sort=instock%20desc,score%20desc,revenue%20desc&sortPrefix=L6;S4;10153&globalPrefix=L6,S4,10153&spuAvailability=S4&lmpAvailability=L6&fvCutoff=22&fqx=!%28storeAttributes:%28%2210153_DEFAULT_FULFILLMENT=SPU%22%29%20AND%20storeOrigin:%28%22Kmart%22%29%29&site=prod";
			String query = new String();
			
			query = URL1;
			int countKeyWords=0;
			for(String keyWord:search) {
				if(!keyWord.equalsIgnoreCase("N/A")) {
					countKeyWords++;
					if(countKeyWords==1) {
						query = query + keyWord;
					}
					else {
						query = query + "%20" + keyWord;
					}
				}
			}
			query = query + URL2;
			
			if(countKeyWords>0) {
			
				try {
					//System.out.println(query);
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long t1 = System.currentTimeMillis();
					Document doc = Jsoup.connect(query).get();
					long t2 = System.currentTimeMillis() - t1;
					System.out.println(" @@@ Query time: " + t2);
					doc.body().wrap("<pre></pre>");
					String text = doc.text();
					// Converting nbsp entities
					text = text.replaceAll("\u00A0", " ");
					
					queryResultsDoc = text;
				} catch (IOException e) {
					e.printStackTrace();
				}
				if(queryResultsDoc==null) {
					System.out.println("query results null");
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
    	}
    	if(pidSet.isEmpty()) {
    		return new HashMap<String,String>();
    	}
    	
    	Map<String,String> variableValueMap = new HashMap<String,String>();
    	
    	for(String pid: pidSet) {
    		//System.out.println(pid);
    		DBObject divLnDBO = pidDivLnCollection.findOne(new BasicDBObject().append("pid", pid));
    		if(divLnDBO != null) {
	    		String div = divLnDBO.get("d").toString();
	    		String divLn = divLnDBO.get("l").toString();
	    		Collection<String> var = new ArrayList<String>();
	    		if(divLnVariablesMap.containsKey(div)) {
	    			var = divLnVariablesMap.get(div);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
	    		if(divLnVariablesMap.containsKey(divLn)) {
	    			var = divLnVariablesMap.get(divLn);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
    		}
    	}
    	
    	return variableValueMap;
    }
    
}
