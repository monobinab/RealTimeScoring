package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberMDTags2Dao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberMDTags2Dao.class);
	DBCollection memberMDTagsCollection;
	
	public MemberMDTags2Dao() {
		//super("server2");
		super();
		memberMDTagsCollection = db.getCollection("memberMdTagsWithDates");
		LOGGER.info("colelction in tagMetadataDao: " + memberMDTagsCollection.getFullName());
	
	}

	public List<String> getMemberMDTagsForVariables(String l_id) {
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if(dbObj != null){
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			for(Object tag:dbListTags){
				if(tag instanceof String){
					mdTags.add(tag.toString().substring(0, 5));
				}
			}
			//List<String> mdTags = (List<String>) dbObj.get("tags");
			return mdTags;
		}
		else
			return null;
	}
	
	public List<String> getMemberMDTags(String l_id) {
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if(dbObj != null){
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			for(Object tag:dbListTags){
				if(tag instanceof String){
					mdTags.add(tag.toString());
				}
			}
			//List<String> mdTags = (List<String>) dbObj.get("tags");
			return mdTags;
		}
		else
			return null;
	}

	/*public void addMemberMDTags(String l_id, List<String> tags) {
		
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put(MongoNameConstants.L_ID, l_id);
		BasicDBList mdTagsList = new BasicDBList();
		for (String tag : tags) {
			mdTagsList.add(tag);
		}
		tagstoUpdate.put("tags", mdTagsList);
		LOGGER.info("tags are getting updated in " +  memberMDTagsCollection.getDB().getName());
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	
	}*/
	public void deleteMemberMDTags(String l_id){
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(MongoNameConstants.L_ID,l_id)); //get first document
		if(doc != null)
			memberMDTagsCollection.remove(doc);
	}
	/*public static void main(String[] args) {
		MemberMDTagsDao dao = new MemberMDTagsDao();
		List<String> sample = dao.getMemberMDTags("hzuzVKVINbBBen+WGYQT/VJVdwI=");
		System.out.println(sample);
	}*/
	
	public void addMemberMDTags(String l_id, List<String> tags) {
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd");
		
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(MongoNameConstants.L_ID,l_id));
		BasicDBList mdTagsList = null;
		BasicDBList newMdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		
		BasicDBList rtsTagsList = new BasicDBList();
		
		//If there is a document already in the Collection for that Lid
		if(doc != null){
			mdTagsList = (BasicDBList) doc.get("tags");
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			for(String tag : tags){
				//Check of the Tag is already there in the document.
				//If yes, retaing the effective and expiration dates. Else create a new sub-document
				newObj = isTagExists(tag, mdTagsList);
				if(newObj!=null)
					newMdTagsList.add(newObj);
				else{
						newObj = new BasicDBObject();
						newObj.append("t", tag);
						newObj.append("f", ft.format(dNow));
						newObj.append("e", ft.format(dNow));
						newMdTagsList.add(newObj);
					}
				}
		}
		//If there is NO document in the Collection for that Lid
		else{
			for(String tag : tags){
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(dNow));
				newMdTagsList.add(newObj);
			}
		}
	
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		tagstoUpdate.put("tags", newMdTagsList);
		if(rtsTagsList.size()>0)
			tagstoUpdate.put("rtsTags", rtsTagsList);
		LOGGER.info("tags are getting updated in " +  memberMDTagsCollection.getDB().getName());
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	private BasicDBObject isTagExists(String tag, BasicDBList mdTagsList){
		
		BasicDBObject obj = null;
		for (Object tagObj : mdTagsList) {
			BasicDBObject obj1 = (BasicDBObject) tagObj;
			if(obj1.containsValue(tag)){
				return obj1;
			}
		}
		return obj;
	}
	
public void addRtsMemberTags(String l_id, List<String> tags) {
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd");
		
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(MongoNameConstants.L_ID,l_id));
		BasicDBList rtsTagsList = null;
		BasicDBList newRtsTagsList = new BasicDBList();
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		
		//If there is a document already in the Collection for that Lid
		if(doc != null){
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			mdTagsList = (BasicDBList) doc.get("tags");
			for(String tag : tags){
				//Check of the Tag is already there in the document.
				//If yes, retaing the effective and expiration dates. Else create a new sub-document
				newObj = isTagExists(tag, rtsTagsList);
				if(newObj!=null)
					newRtsTagsList.add(newObj);
				else{
						newObj = new BasicDBObject();
						newObj.append("t", tag);
						newObj.append("f", ft.format(dNow));
						newObj.append("e", ft.format(dNow));
						newRtsTagsList.add(newObj);
					}
				}
		}
		//If there is NO document in the Collection for that Lid
		else{
			for(String tag : tags){
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(dNow));
				newRtsTagsList.add(newObj);
			}
		}
	
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		tagstoUpdate.put("rtsTags", newRtsTagsList);
		if(mdTagsList.size()>0)
			tagstoUpdate.put("tags", mdTagsList);
		LOGGER.info("tags are getting updated in " +  memberMDTagsCollection.getDB().getName());
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

}
