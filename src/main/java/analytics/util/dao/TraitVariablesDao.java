package analytics.util.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.DBConnection;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TraitVariablesDao {
	DB db;
    DBCollection traitVariablesCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		traitVariablesCollection = db.getCollection("traitVariables");
    }
    public HashMap<String, List<String>> getTraitVariableList(){
    	HashMap<String, List<String>> traitVariablesMap = new HashMap<String, List<String>>();
    	DBCursor traitVarCursor = traitVariablesCollection.find();

    	for(DBObject traitVariablesDBO: traitVarCursor) {
    		BasicDBList variables = (BasicDBList) traitVariablesDBO.get("v");
    		for(Object v:variables) {
    			String variable = v.toString().toUpperCase();
    			if(traitVariablesMap.containsKey(traitVariablesDBO.get("t"))) {
    				if(!traitVariablesMap.get(traitVariablesDBO.get("t")).contains(variable)) {
    					traitVariablesMap.get(traitVariablesDBO.get("t")).add(variable);
    				}
    			}
    			else {
    				List<String> newTraitVariable = new ArrayList<String>();
    				traitVariablesMap.put(traitVariablesDBO.get("t").toString(), newTraitVariable);
    				traitVariablesMap.get(traitVariablesDBO.get("t")).add(variable);
    			}
    		}
    	}
    	return traitVariablesMap;
    }
	
    public HashMap<String, List<String>> getVariableTraitList(){
    	HashMap<String, List<String>> variableTraitsMap = new HashMap<String, List<String>>();
    	DBCursor traitVarCursor = traitVariablesCollection.find();

    	for(DBObject traitVariablesDBO: traitVarCursor) {
    		BasicDBList variables = (BasicDBList) traitVariablesDBO.get("v");
    		for(Object v:variables) {
    			String variable = v.toString().toUpperCase();
    			if(variableTraitsMap.containsKey(variable)) {
    				if(!variableTraitsMap.get(variable).contains(traitVariablesDBO.get("t"))) {
    					variableTraitsMap.get(variable).add(traitVariablesDBO.get("t").toString());
    				}
    			}
    			else {
    				List<String> newVariableTraits = new ArrayList<String>();
    				variableTraitsMap.put(variable, newVariableTraits);
    				variableTraitsMap.get(variable).add(traitVariablesDBO.get("t").toString());
    			}
    		}
    	}
    	return variableTraitsMap;
    }
	
}
