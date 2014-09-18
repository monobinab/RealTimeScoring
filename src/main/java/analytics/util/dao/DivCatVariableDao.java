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

public class DivCatVariableDao extends AbstractDao{
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivCatVariableDao.class);
    DBCollection divCatVariableCollection;
    public DivCatVariableDao(){
    	super();
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
