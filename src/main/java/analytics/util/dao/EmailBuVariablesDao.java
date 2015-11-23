package analytics.util.dao;

/**
 * @author spannal
 * Fetches all the BU variable-models from emailBUVariable collection.
 * Used by EmailFeedBackTopology
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.VariableModel;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class EmailBuVariablesDao extends AbstractDao {

    private DBCollection emailBUVariableCollection;
    private Cache cache = null;
    
    public EmailBuVariablesDao(){
    	super();
    	emailBUVariableCollection = db.getCollection("emailBUVariable");
    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_EMAIL_BU_VARIABLE_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_EMAIL_BU_VARIABLE_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    
    @SuppressWarnings("unchecked")
	public HashMap<String, List<VariableModel>> getEmailBUVariables(){
    	String cacheKey = CacheConstant.RTS_EMAIL_BU_VARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<String, List<VariableModel>>) element.getObjectValue();
		}else{
    	HashMap<String, List<VariableModel>> emailBUVariablesMap = new HashMap<String, List<VariableModel>>();
		DBCursor emailBUVarCursor = emailBUVariableCollection.find();
    	for(DBObject emailBUDBObject: emailBUVarCursor) {
    		
    		String bu = emailBUDBObject.get(MongoNameConstants.EMAIL_BU).toString().toUpperCase();
    		String variable =  emailBUDBObject.get(MongoNameConstants.EMAIL_VAR).toString().toUpperCase();
    		String format =  emailBUDBObject.get(MongoNameConstants.STORE_FORMAT).toString().toUpperCase();
    		Integer modelId =  (Integer) emailBUDBObject.get(MongoNameConstants.EMAIL_MODEL_ID);
    		String key = bu+"~"+format;
    		VariableModel variableModel = null;
    		
    		if (emailBUVariablesMap.get(key) == null)
            {
                List<VariableModel> varColl = new ArrayList<VariableModel>();
                variableModel = new VariableModel();
                variableModel.setVariable(variable);
                variableModel.setModelId(modelId);
                varColl.add(variableModel);
                emailBUVariablesMap.put(key, varColl);
            }
            else
            {
                List<VariableModel> varColl = emailBUVariablesMap.get(key);
                variableModel = new VariableModel();
                variableModel.setVariable(variable);
                variableModel.setModelId(modelId);
                varColl.add(variableModel);
                emailBUVariablesMap.put(key, varColl);
            }    		
    		
        }
    	if(emailBUVariablesMap != null && emailBUVariablesMap.size() > 0){
			cache.put(new Element(cacheKey, (HashMap<String, List<VariableModel>>) emailBUVariablesMap));
		}
    	return emailBUVariablesMap;
		}
    }


}
