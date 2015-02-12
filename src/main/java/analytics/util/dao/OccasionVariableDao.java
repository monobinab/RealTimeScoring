package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class OccasionVariableDao extends AbstractDao{
		private static final Logger LOGGER = LoggerFactory
				.getLogger(TraitVariablesDao.class);
	    DBCollection occasionVariablesCollection;
		/**
		 * v,b,s,po
		 */
	    public OccasionVariableDao(){
	    	super();
			occasionVariablesCollection = db.getCollection("occasionVariable");
	    }
	    public String getValue(TagMetadata tagMetadata){
	    	BasicDBObject query = new BasicDBObject("b",tagMetadata.getBusinessUnit());
	    	query.append("s", tagMetadata.getSubBusinessUnit());
	    	query.append("po", tagMetadata.getPurchaseOccasion());
	    	DBObject occasionVariable = occasionVariablesCollection.findOne(query);
	    	
	    	if(occasionVariable!=null && occasionVariable.containsField("v")){
	    		return (String)occasionVariable.get("v");
	    	}
	    	return null;
	    }
}
