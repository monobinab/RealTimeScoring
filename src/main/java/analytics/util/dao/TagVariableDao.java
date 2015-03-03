package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagVariableDao extends AbstractDao{
		private static final Logger LOGGER = LoggerFactory
				.getLogger(TraitVariablesDao.class);
	    DBCollection tagVariablesCollection;
	    public TagVariableDao(){
	    	super();
			tagVariablesCollection = db.getCollection("tagVariable");
	    }
	    public Map<String, String> getTagVariable(String tag){
	    	DBObject tagVariable = tagVariablesCollection.findOne(new BasicDBObject(MongoNameConstants.TAG_VAR_MDTAG,tag.substring(0, 5)));
	    	if(tagVariable!=null && tagVariable.containsField(MongoNameConstants.TAG_VAR_VAR) && tagVariable.containsField(MongoNameConstants.TAG_VAR_MODEL)){
	    		Map<String, String> tagVarMap = new HashMap<String, String>();
	    		tagVarMap.put((String)tagVariable.get(MongoNameConstants.TAG_VAR_VAR), tagVariable.get(MongoNameConstants.TAG_VAR_MODEL)+"");
	    		return tagVarMap;
	    	}
	    	return null;
	    }
	    
	    public List<String> getTagVariablesList(List<String> tagsList){
	    	BasicDBList list = new BasicDBList();
	    	for(String tag:tagsList){
	    	list.add(tag);
	    	}
	    	BasicDBObject query = new BasicDBObject();
	    	query.put(MongoNameConstants.TAG_VAR_MDTAG, new BasicDBObject("$in", list));
	    	DBCursor tagVariablesCursor = tagVariablesCollection.find(query);
	    	List<String> tagVariablesList = new ArrayList<String>();
	    	while(tagVariablesCursor.hasNext()){
	    		tagVariablesList.add((String) tagVariablesCursor.next().get(MongoNameConstants.TAG_VAR_VAR));
	    	}
	    	return tagVariablesList;
	    }
}
