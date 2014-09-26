package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelSywBoostDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ModelSywBoostDao.class);
    DBCollection modelBoostCollection;
    
    public ModelSywBoostDao(){
    	super();
    	modelBoostCollection = db.getCollection("modelSywBoosts");
    }
    public Map<String, Integer> getVarModelMap(){
    	Map<String, Integer> varModelMap = new HashMap<String, Integer>();
    	DBCursor varModelCursor = modelBoostCollection.find();
    	for(DBObject varModelObj: varModelCursor) {
    		varModelMap.put((String) varModelObj.get("b"), (Integer) varModelObj.get("m"));
    	}
    	return varModelMap;
    }
    
	public Integer getModelId(String variable) {
		DBObject feedObject = modelBoostCollection.findOne(new BasicDBObject("b",variable));
		return (Integer)feedObject.get("m");
	}

}
