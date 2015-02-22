package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;

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
	    public String getTagVariable(String tag){
	    	DBObject tagVariable = tagVariablesCollection.findOne(new BasicDBObject(Constants.TAG_MDTAG,tag));
	    	if(tagVariable!=null && tagVariable.containsField(Constants.TAG_VAR)){
	    		return (String)tagVariable.get(Constants.TAG_VAR);
	    	}
	    	return null;
	    }
	    
	    public List<String> getTagVariablesList(List<String> tagsList){
	    	BasicDBList list = new BasicDBList();
	    	for(String tag:tagsList){
	    	list.add(tag);
	    	}
	    	BasicDBObject query = new BasicDBObject();
	    	query.put("t", new BasicDBObject("$in", list));
	    	DBCursor tagVariablesCursor = tagVariablesCollection.find(query);
	    	List<String> tagVariablesList = new ArrayList<String>();
	    	while(tagVariablesCursor.hasNext()){
	    		tagVariablesList.add((String) tagVariablesCursor.next().get("v"));
	    	}
	    	return tagVariablesList;
	    }
}
