package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;
import analytics.util.objects.Variable;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class VariableDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(VariableDao.class);
	static DB db;
    DBCollection variablesCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public VariableDao(){
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
