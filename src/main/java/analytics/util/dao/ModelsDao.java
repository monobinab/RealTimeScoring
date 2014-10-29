package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelsDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ModelsDao.class);
    DBCollection modelsCollection;
    
    public ModelsDao() {
    	super();
    	modelsCollection = db.getCollection("Models");
    }
	public Map<Integer, String> getModelNames(){
		Map<Integer, String> modelsMap = new HashMap<Integer, String>();
		DBCursor modelsCursor = modelsCollection.find();
		for(DBObject modelEntry:modelsCursor){
			Integer modelId = Integer.parseInt(modelEntry.get("modelId").toString());
			modelsMap.put(modelId,(String)modelEntry.get("modelName"));
			
		}
		return modelsMap;
	}
}
