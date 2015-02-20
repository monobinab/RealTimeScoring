package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	    	DBObject tagVariable = tagVariablesCollection.findOne(new BasicDBObject("t",tag));
	    	if(tagVariable!=null && tagVariable.containsField("v")){
	    		return (String)tagVariable.get("v");
	    	}
	    	return null;
	    }
	    
	    public List<String> getTagVariablesList(List<String> tagsList){
	    	BasicDBList list = new BasicDBList();
	    	list.add(tagsList);
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
