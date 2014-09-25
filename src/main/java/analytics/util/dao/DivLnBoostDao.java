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

public class DivLnBoostDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnBoostDao.class);
    DBCollection divLnBoostCollection;
    public DivLnBoostDao(){
    	super();
		divLnBoostCollection = db.getCollection("divLnBoost");
    }
    public HashMap<String, List<String>> getDivLnBoost(){
    	HashMap<String, List<String>> divLnBoostMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnBoostCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnBoostMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV)) == null)
            {
                List<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString());
                divLnBoostMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), varColl);
            }
            else
            {
                List<String> varColl = divLnBoostMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString());
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString().toUpperCase());
                divLnBoostMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), varColl);
            }
        }
    	return divLnBoostMap;
    }
}
