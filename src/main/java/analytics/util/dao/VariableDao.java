package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
	
	public List<String> getVariableNames() {
		List<String> variables = new ArrayList<String>();
		DBCursor vCursor = variablesCollection.find();
		for (DBObject variable : vCursor) {
			variables.add(((DBObject) variable).get(MongoNameConstants.V_NAME).toString().toUpperCase());
		}
		return variables;
			
	}
	
	public Set<String> getStrategyList() {
		Set<String> strategyList = new HashSet<String>();
		DBCursor varCollCursor = variablesCollection.find();
		String strategy;
		for(DBObject varDBO: varCollCursor) {
			strategy = varDBO.get("strategy").toString();
			if(!"NONE".equals(strategy)) {
				strategyList.add(strategy);
			}
		}
		return strategyList;
	}
}
