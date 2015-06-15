package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.BoosterVariable;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class BoosterVariableDao extends AbstractDao {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(VariableDao.class);
    DBCollection boosterVariablesCollection;
    public BoosterVariableDao(){
    	super();
		boosterVariablesCollection = db.getCollection("boosterVariables");
    }
	public List<BoosterVariable> getBoosterVariables() {
		List<BoosterVariable> variables = new ArrayList<BoosterVariable>();
		DBCursor vCursor = boosterVariablesCollection.find();
		for (DBObject variable : vCursor) {
			BoosterVariable boosterVariable = new BoosterVariable();
			boosterVariable.setBvid(String.valueOf( variable.get(MongoNameConstants.B_VID)));
			boosterVariable.setName((String) variable.get(MongoNameConstants.V_NAME));
			boosterVariable.setStrategy((String) variable.get(MongoNameConstants.V_STRATEGY));
			variables.add(boosterVariable);
		}
		return variables;
	}
}