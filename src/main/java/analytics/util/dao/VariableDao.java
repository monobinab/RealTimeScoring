package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.Variable;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class VariableDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VariableDao.class);
    DBCollection variablesCollection;
    public VariableDao(){
    	super();
		variablesCollection = db.getCollection("Variables");
    }
	public List<Variable> getVariables() {
		List<Variable> variables = new ArrayList<Variable>();
		DBCursor vCursor = variablesCollection.find();
		for (DBObject variable : vCursor) {
			variables.add(new Variable(
					((DBObject) variable).get(MongoNameConstants.V_NAME).toString().toUpperCase(),
					((DBObject) variable).get(MongoNameConstants.V_ID).toString(),
					((DBObject) variable).get(MongoNameConstants.V_STRATEGY).toString()));
		}
		return variables;
			
		}
}
