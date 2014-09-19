package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TraitVariablesDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TraitVariablesDao.class);
	static DB db;
    DBCollection traitVariablesCollection;
    public TraitVariablesDao(){
    	super();
		traitVariablesCollection = db.getCollection("traitVariables");
    }
    public HashMap<String, List<String>> getTraitVariableList(){
    	HashMap<String, List<String>> traitVariablesMap = new HashMap<String, List<String>>();
    	DBCursor traitVarCursor = traitVariablesCollection.find();

    	for(DBObject traitVariablesDBO: traitVarCursor) {
    		BasicDBList variables = (BasicDBList) traitVariablesDBO.get(MongoNameConstants.TV_VARIABLES);
    		for(Object v:variables) {
    			String variable = v.toString().toUpperCase();
    			if(traitVariablesMap.containsKey(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT))) {
    				if(!traitVariablesMap.get(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT)).contains(variable)) {
    					traitVariablesMap.get(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT)).add(variable);
    				}
    			}
    			else {
    				List<String> newTraitVariable = new ArrayList<String>();
    				traitVariablesMap.put(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT).toString(), newTraitVariable);
    				traitVariablesMap.get(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT)).add(variable);
    			}
    		}
    	}
    	return traitVariablesMap;
    }
	
    public HashMap<String, List<String>> getVariableTraitList(){
    	HashMap<String, List<String>> variableTraitsMap = new HashMap<String, List<String>>();
    	DBCursor traitVarCursor = traitVariablesCollection.find();

    	for(DBObject traitVariablesDBO: traitVarCursor) {
    		BasicDBList variables = (BasicDBList) traitVariablesDBO.get(MongoNameConstants.TV_VARIABLES);
    		for(Object v:variables) {
    			String variable = v.toString().toUpperCase();
    			if(variableTraitsMap.containsKey(variable)) {
    				if(!variableTraitsMap.get(variable).contains(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT))) {
    					variableTraitsMap.get(variable).add(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT).toString());
    				}
    			}
    			else {
    				List<String> newVariableTraits = new ArrayList<String>();
    				variableTraitsMap.put(variable, newVariableTraits);
    				variableTraitsMap.get(variable).add(traitVariablesDBO.get(MongoNameConstants.TV_TRAIT).toString());
    			}
    		}
    	}
    	return variableTraitsMap;
    }
	
}
