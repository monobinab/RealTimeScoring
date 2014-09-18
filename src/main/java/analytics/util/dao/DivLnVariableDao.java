package analytics.util.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnVariableDao extends AbstractDao{
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnVariableDao.class);
    DBCollection divLnVariableCollection;
    public DivLnVariableDao(){
    	super();
		divLnVariableCollection = db.getCollection("divLnVariable");
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
}
