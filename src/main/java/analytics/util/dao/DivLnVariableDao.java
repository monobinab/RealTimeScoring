package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnVariableDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnVariableDao.class);
    DBCollection divLnVariableCollection;
    DBCollection divLnBoostCollection;
    public DivLnVariableDao(){
    	super();
		divLnVariableCollection = db.getCollection("divLnVariable");
		divLnBoostCollection = db.getCollection("divLnBoost");
    }
    public HashMap<String, List<String>> getDivLnVariable(){
    	HashMap<String, List<String>> divLnVariablesMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnVariableCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV)) == null)
            {
                List<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get(MongoNameConstants.DLV_VAR).toString());
                divLnVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
            else
            {
                List<String> varColl = divLnVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString());
                varColl.add(divLnDBObject.get(MongoNameConstants.DLV_VAR).toString().toUpperCase());
                divLnVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
        }
    	return divLnVariablesMap;
    }
    
    public HashMap<String, List<String>> getDivLnBoostVariable(){
    	HashMap<String, List<String>> divLnBoostVariablesMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnBoostCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnBoostVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV)) == null)
            {
                List<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString());
                divLnBoostVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
            else
            {
                List<String> varColl = divLnBoostVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString());
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString().toUpperCase());
                divLnBoostVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
        }
    	return divLnBoostVariablesMap;
    }
}
