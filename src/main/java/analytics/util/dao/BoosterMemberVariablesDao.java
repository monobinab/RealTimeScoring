package analytics.util.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class BoosterMemberVariablesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(BoosterMemberVariablesDao.class);
    DBCollection boosterMemberVariablesCollection;
    public BoosterMemberVariablesDao(){
    	//Connect to secondary server
    	super("server2");
    	boosterMemberVariablesCollection = db.getCollection("boosterMemberVariables");
    }
   
    public Map<String,Object> getBoosterMemberVariablesFiltered(String l_id, List<String> variableFilter){
    	BasicDBObject variableFilterDBO = new BasicDBObject(MongoNameConstants.ID, 0);
    	for(String var:variableFilter){
    		variableFilterDBO.append(var, 1);
    	}
    	
    	DBObject mbrVariables = boosterMemberVariablesCollection.findOne(
				new BasicDBObject("l_id", l_id), variableFilterDBO);

		if (mbrVariables == null) {
			return null;
		}

		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Object> boosterMemberVariablesMap = new HashMap<String, Object>();
		Iterator<String> boosterMbrVariablesIter = mbrVariables.keySet().iterator();
		while (boosterMbrVariablesIter.hasNext()) {
			String key = boosterMbrVariablesIter.next();
			if (!key.equals(MongoNameConstants.L_ID) && !key.equals(MongoNameConstants.ID)) {
				boosterMemberVariablesMap.put(key, mbrVariables.get(key));
			}
		}
		return boosterMemberVariablesMap;

	}
}