package analytics.util.dao;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Variable;

import com.mongodb.*;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BoostsDao extends AbstractDao {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(BoostsDao.class);
    DBCollection boostsCollection;
    private Cache cache = null;

    public BoostsDao(){
        super();
        boostsCollection = db.getCollection("Boosts_sep");
        cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_BOOST_CACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_BOOST_CACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    @SuppressWarnings("unchecked")
	public List<Variable> getBoosts() {
		
		String cacheKey = CacheConstant.RTS_CACHE_BOOST_CACHE;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<Variable>) element.getObjectValue();
		}else{
		DBCursor vCursor = boostsCollection.find();
		List<Variable> boosts = new ArrayList<Variable>();
		for (DBObject variable : vCursor) {
			boosts.add(new Variable(
					((DBObject) variable).get(MongoNameConstants.V_NAME).toString().toUpperCase(),
					((DBObject) variable).get(MongoNameConstants.V_ID).toString(),
					((DBObject) variable).get(MongoNameConstants.V_STRATEGY).toString()));
		}
			return boosts;
		}
	}
    
    
    
    
    public Map<String,Variable> getBoostNameToVariableMap() {
        Map<String,Variable> boostNameToVariableMap = new HashMap<String, Variable>();
        DBCursor boostCursor = boostsCollection.find();
        while(boostCursor.hasNext()){
            DBObject boostDBO = boostCursor.next();
            boostNameToVariableMap
                    .put(boostDBO.get(MongoNameConstants.BOOST_NAME).toString()
                            ,new Variable(boostDBO.get(MongoNameConstants.BOOST_NAME).toString()
                                ,boostDBO.get(MongoNameConstants.BOOST_VID).toString()
                                ,boostDBO.get(MongoNameConstants.BOOST_STRATEGY).toString()));
        }
        if(boostNameToVariableMap.isEmpty()) {
            LOGGER.error("Boost name to variable map is empty");
            return boostNameToVariableMap;
        } else {
            return boostNameToVariableMap;
        }
    }

    public Map<String,Variable> getBoostVidToVariableMap() {
        Map<String,Variable> boostVidToVariableMap = new HashMap<String, Variable>();
        DBCursor boostCursor = boostsCollection.find();
        while(boostCursor.hasNext()){
            DBObject boostDBO = boostCursor.next();
            boostVidToVariableMap
                    .put(boostDBO.get(MongoNameConstants.BOOST_VID).toString()
                            , new Variable(boostDBO.get(MongoNameConstants.BOOST_NAME).toString()
                            , boostDBO.get(MongoNameConstants.BOOST_VID).toString()
                            , boostDBO.get(MongoNameConstants.BOOST_STRATEGY).toString()));
        }
        if(boostVidToVariableMap.isEmpty()) {
            LOGGER.error("Boost VID to variable map is empty");
            return boostVidToVariableMap;
        } else {
            return boostVidToVariableMap;
        }
    }
    
    
    public Set<String> getBoostsStrategyList() {
		Set<String> boostsStrategyList = new HashSet<String>();
		DBCursor boostsCollCursor = boostsCollection.find();
		String strategy;
		for(DBObject boostsDBO: boostsCollCursor) {
			strategy = boostsDBO.get("strategy").toString();
			if(!"NONE".equals(strategy)) {
				boostsStrategyList.add(strategy);
			}
		}
		return boostsStrategyList;
	}
}