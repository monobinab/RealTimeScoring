package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
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
}