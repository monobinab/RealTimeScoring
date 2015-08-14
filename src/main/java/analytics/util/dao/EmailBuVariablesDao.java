package analytics.util.dao;

/**
 * @author spannal
 * Fetches all the BU variable-models from emailBUVariable collection.
 * Used by EmailFeedBackTopology
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.VariableModel;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class EmailBuVariablesDao extends AbstractDao {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(EmailBuVariablesDao.class);
    DBCollection emailBUVariableCollection;
   // DBCollection divLnBoostCollection;
    public EmailBuVariablesDao(){
    	super();
    	emailBUVariableCollection = db.getCollection("emailBUVariable");		
    }
    
    public HashMap<String, List<VariableModel>> getEmailBUVariables(){
    	HashMap<String, List<VariableModel>> emailBUVariablesMap = new HashMap<String, List<VariableModel>>();
	
		DBCursor emailBUVarCursor = emailBUVariableCollection.find();
    	for(DBObject emailBUDBObject: emailBUVarCursor) {
    		
    		String bu = emailBUDBObject.get(MongoNameConstants.EMAIL_BU).toString().toUpperCase();
    		String variable =  emailBUDBObject.get(MongoNameConstants.EMAIL_VAR).toString().toUpperCase();
    		String format =  emailBUDBObject.get(MongoNameConstants.STORE_FORMAT).toString().toUpperCase();
    		Integer modelId =  (Integer) emailBUDBObject.get(MongoNameConstants.EMAIL_MODEL_ID);
    		String key = bu+"~"+format;
    		VariableModel variableModel = null;
    		
    		if (emailBUVariablesMap.get(key) == null)
            {
                List<VariableModel> varColl = new ArrayList<VariableModel>();
                variableModel = new VariableModel();
                variableModel.setVariable(variable);
                variableModel.setModelId(modelId);
                varColl.add(variableModel);
                emailBUVariablesMap.put(key, varColl);
            }
            else
            {
                List<VariableModel> varColl = emailBUVariablesMap.get(key);
                variableModel = new VariableModel();
                variableModel.setVariable(variable);
                variableModel.setModelId(modelId);
                varColl.add(variableModel);
                emailBUVariablesMap.put(key, varColl);
            }    		
    		
        }
    	return emailBUVariablesMap;
    }


}
