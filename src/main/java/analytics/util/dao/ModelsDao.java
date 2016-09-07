package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import analytics.util.MongoNameConstants;
import analytics.util.objects.Model;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelsDao extends AbstractDao{
    DBCollection modelsCollection;
    
    public ModelsDao() {
    	super();
    	modelsCollection = db.getCollection("Models");
    }
    
	public Map<Integer, Model> getModelNames(){
		Map<Integer, Model> modelsMap = new HashMap<Integer, Model>();
		DBCursor modelsCursor = modelsCollection.find();
		for(DBObject modelEntry:modelsCursor){
			if(modelEntry != null){
				int modelId = (Integer)modelEntry.get(MongoNameConstants.MODEL_ID);
				Model model = new Model(modelId,(String)modelEntry.get(MongoNameConstants.MODEL_NAME),(String)modelEntry.get(MongoNameConstants.MODEL_CODE));
				modelsMap.put(modelId, model);
			}
		}
		return modelsMap;
	}
}
