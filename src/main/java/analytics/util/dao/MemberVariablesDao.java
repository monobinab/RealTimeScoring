package analytics.util.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberVariablesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);
    DBCollection memberVariablesCollection;
    public MemberVariablesDao(){
    	//Connect to static collections replicaset
    	super("mongo2");
    	if(db != null){
    		memberVariablesCollection = db.getCollection("memberVariables");
    	}
    }
   
    public Map<String,Object> getMemberVariablesFiltered(String l_id, Set<String> variableFilter){
 	   	BasicDBObject variableFilterDBO = new BasicDBObject(MongoNameConstants.ID, 0);
    	for(String var:variableFilter){
    		variableFilterDBO.append(var, 1);
    	}
    	Map<String, Object> memberVariablesMap = null;
    	DBObject mbrVariables = memberVariablesCollection.findOne(
				new BasicDBObject("l_id", l_id), variableFilterDBO);
     	
    	LOGGER.info("memberVariables connected from " +  db.getMongo().getAllAddress() + " mongo " + db.getName() + "db");
    	if (mbrVariables != null) {
    		memberVariablesMap = new HashMap<String, Object>();;
    		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
			Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
			while (mbrVariablesIter.hasNext()) {
				String key = mbrVariablesIter.next();
				if (!key.equals(MongoNameConstants.L_ID) && !key.equals(MongoNameConstants.ID)) {
					memberVariablesMap.put(key, mbrVariables.get(key));
				}
			}
    	}
		return memberVariablesMap;
	}
    
    public DBObject getMemVars(String lid){
    	DBObject mbrVariables = memberVariablesCollection.findOne(
				new BasicDBObject("l_id", lid));
    	return mbrVariables;
    	
    }
}
