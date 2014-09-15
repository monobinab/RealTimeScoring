package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelVariablesDao {
	DB db;
    DBCollection modelVariablesCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		modelVariablesCollection = db.getCollection("modelVariables");
    }
    public List<String> getVariableList(){
    	List<String> modelVariablesList = new ArrayList<String>();
    	DBCursor modelVariablesCursor = modelVariablesCollection.find();
		for(DBObject modelDBO:modelVariablesCursor) {
			BasicDBList variablesDBList = (BasicDBList) modelDBO.get(MongoNameConstants.MODELV_VARIABLE);
			for(Object var:variablesDBList) {
				if(!modelVariablesList.contains(var.toString())) {
					modelVariablesList.add(((BasicDBObject) var).get(MongoNameConstants.MODELV_NAME).toString());
				}
			}
		}
    	return modelVariablesList;
    }
}
