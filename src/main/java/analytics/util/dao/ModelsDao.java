package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.Model;

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
	public Map<Integer, Model> getModelNames(){
		Map<Integer, Model> modelsMap = new HashMap<Integer, Model>();
		DBCursor modelsCursor = modelsCollection.find();
		for(DBObject modelEntry:modelsCursor){
			Integer modelId = Integer.parseInt(MongoNameConstants.MODEL_ID);
			Model model = new Model(Integer.parseInt(modelEntry.get(MongoNameConstants.MODEL_ID).toString()), (String)modelEntry.get(MongoNameConstants.MODEL_NAME), (String)MongoNameConstants.MODEL_CODE);
			modelsMap.put(modelId, model);
		}
		return modelsMap;
	}
}
