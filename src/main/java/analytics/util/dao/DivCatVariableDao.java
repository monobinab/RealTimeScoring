package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivCatVariableDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivCatVariableDao.class);
	static DB db;
    DBCollection divCatVariableCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public DivCatVariableDao(){
		divCatVariableCollection = db.getCollection("divCatVariable");
    }
    public HashMap<String, List<String>> getDivCatVariable(){
    	HashMap<String, List<String>> divLnVariablesMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divCatVariableCollection.find();
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
}
