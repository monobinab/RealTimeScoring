package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
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
			occasionVariablesCollection = db.getCollection("occasionValue");
	    }
	    public String getValue(TagMetadata tagMetadata){
	    	BasicDBObject query = new BasicDBObject(Constants.OCC_BU,tagMetadata.getBusinessUnit());
	    	query.append(Constants.OCC_SUB, tagMetadata.getSubBusinessUnit());
	    	query.append(Constants.OCC_PO, tagMetadata.getPurchaseOccasion());
	    	DBObject occasionVariable = occasionVariablesCollection.findOne(query);
	    	
	    	if(occasionVariable!=null && occasionVariable.containsField(Constants.OCC_VAR)){
	    		return (String.valueOf(occasionVariable.get(Constants.OCC_VAR)));
	    	}
	    	return null;
	    }

}
