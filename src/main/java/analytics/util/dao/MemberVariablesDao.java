package analytics.util.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberVariablesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);
    DBCollection memberVariablesCollection;
    public MemberVariablesDao(){
    	//Connect to secondary server
    	super("server2_2");
		memberVariablesCollection = db.getCollection("memberVariables");
    }
   
    public Map<String,Object> getMemberVariablesFiltered(String l_id, List<String> variableFilter){
    	BasicDBObject variableFilterDBO = new BasicDBObject(MongoNameConstants.ID, 0);
    	for(String var:variableFilter){
    		variableFilterDBO.append(var, 1);
    	}
    	
    	DBObject mbrVariables = memberVariablesCollection.findOne(
				new BasicDBObject("l_id", l_id), variableFilterDBO);

		if (mbrVariables == null) {
			return null;
		}

		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Object> memberVariablesMap = new HashMap<String, Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while (mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if (!key.equals(MongoNameConstants.L_ID) && !key.equals(MongoNameConstants.ID)) {
				memberVariablesMap.put(key, mbrVariables.get(key));
			}
		}
		return memberVariablesMap;
				
	}
}
