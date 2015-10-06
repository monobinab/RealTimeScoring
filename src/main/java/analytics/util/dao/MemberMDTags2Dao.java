package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
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
		// super("server2");
		super();
		memberMDTagsCollection = db.getCollection("memberMdTagsWithDates");
		LOGGER.info("collection in tagMetadataDao: "
				+ memberMDTagsCollection.getFullName());
	}

	public List<String> getMemberMDTagsBuSubBuList(String l_id) {
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (dbObj != null) {
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			for (Object tag : dbListTags) {
				if (tag instanceof String) {
					mdTags.add(tag.toString().substring(0, 5));
				}
			}
			// List<String> mdTags = (List<String>) dbObj.get("tags");
			return mdTags;
		} else
			return null;
	}

	public List<String> getMemberMDTags(String l_id) {
		BasicDBObject field = new BasicDBObject();
		field.put("tags.t", 1);
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), field);
		if (dbObj != null) {
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			for (Object tag : dbListTags) {
				if (tag instanceof BasicDBObject) {
					mdTags.add(((BasicDBObject) tag).getString("t").toString());
				}
			}
			// List<String> mdTags = (List<String>) dbObj.get("tags");
			return mdTags;
		} else
			return null;
	}

	/*
	 * public void addMemberMDTags(String l_id, List<String> tags) { DBObject
	 * tagstoUpdate = new BasicDBObject();
	 * tagstoUpdate.put(MongoNameConstants.L_ID, l_id); BasicDBList mdTagsList =
	 * new BasicDBList(); for (String tag : tags) { mdTagsList.add(tag); }
	 * tagstoUpdate.put("tags", mdTagsList);
	 * LOGGER.info("tags are getting updated in " +
	 * memberMDTagsCollection.getDB().getName());
	 * memberMDTagsCollection.update(new BasicDBObject( MongoNameConstants.L_ID,
	 * l_id), tagstoUpdate, true, false); }
	 */
	public void deleteMemberMDTags(String l_id) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id)); // get first document
		if (doc != null){
			BasicDBList rtsTagsList = (BasicDBList) doc.get("rtsTags");
			if(rtsTagsList == null || rtsTagsList.size() == 0)
				memberMDTagsCollection.remove(doc);
			else{
				DBObject tagstoUpdate = new BasicDBObject();
				tagstoUpdate.put("l_id", l_id);
				tagstoUpdate.put("rtsTags", rtsTagsList);
				
				LOGGER.info("Tags are getting updated in "
						+ memberMDTagsCollection.getDB().getName()
						+ " for memberId : '" + l_id + "'");
				memberMDTagsCollection.update(new BasicDBObject(
						MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
			}
		}
	}

	/*
	 * public static void main(String[] args) { MemberMDTagsDao dao = new
	 * MemberMDTagsDao(); List<String> sample =
	 * dao.getMemberMDTags("hzuzVKVINbBBen+WGYQT/VJVdwI=");
	 * System.out.println(sample); }
	 */
	public void addMemberMDTags(String l_id, List<String> tags) {
		Date dNow = new Date();
		Date newDate = DateUtils.addMonths(new Date(), 6);
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject doc = memberMDTagsCollection.findOne(query);
		BasicDBList mdTagsList = null;
		BasicDBList newMdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		BasicDBList rtsTagsList = new BasicDBList();
		// If there is a document already in the Collection for that Lid
		if (doc != null) {
			mdTagsList = (BasicDBList) doc.get("tags");
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			for (String tag : tags) {
				// Check if the Tag is already there in the document.
				// If yes, retain the effective and expiration dates. Else
				// create a new sub-document

				newObj = isTagExists(tag, mdTagsList);
				if (newObj == null) {
					newObj = new BasicDBObject();
					newObj.append("t", tag);
					newObj.append("f", ft.format(dNow));
					newObj.append("e", ft.format(newDate));					
				}
				newMdTagsList.add(newObj);
			}
		}
		// If there is NO document in the Collection for that Lid
		else {
			for (String tag : tags) {
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(newDate));
				newMdTagsList.add(newObj);
			}
		}
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		if (newMdTagsList != null && newMdTagsList.size() > 0)
			tagstoUpdate.put("tags", newMdTagsList);
		if (rtsTagsList != null && rtsTagsList.size() > 0)
			tagstoUpdate.put("rtsTags", rtsTagsList);
		LOGGER.info("Tags are getting updated in "
				+ memberMDTagsCollection.getDB().getName()
				+ " for memberId : '" + l_id + "'");
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	private BasicDBObject isTagExists(String tag, BasicDBList tagsList) {
		BasicDBObject obj = null;
		if (tagsList != null && tagsList.size() > 0) {
			for (Object tagObj : tagsList) {
				BasicDBObject obj1 = (BasicDBObject) tagObj;
				if (obj1.containsValue(tag)) {
					return obj1;
				}
			}
		}
		return obj;
	}

	public void addRtsMemberTags(String l_id, List<String> tags) {
		Date dNow = new Date();
		Date tomorrow = new Date(dNow.getTime() + (1000 * 60 * 60 * 24));
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		BasicDBList rtsTagsList = null;
		BasicDBList newRtsTagsList = new BasicDBList();
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		// If there is a document already in the Collection for that Lid
		if (doc != null) {
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			mdTagsList = (BasicDBList) doc.get("tags");
			for (String tag : tags) {
				// Check if the Tag is already there in the document.
				// If yes, retain the effective and expiration dates. Else
				// create a new sub-document
				newObj = isTagExists(tag, rtsTagsList);
				if (newObj == null) {
					newObj = new BasicDBObject();
					newObj.append("t", tag);
					newObj.append("f", ft.format(dNow));
					newObj.append("e", ft.format(tomorrow));
					if (rtsTagsList == null)
						rtsTagsList = new BasicDBList();
					rtsTagsList.add(newObj);
				}
			}
		}
		// If there is NO document in the Collection for that Lid
		else {
			rtsTagsList = new BasicDBList();
			for (String tag : tags) {
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(tomorrow));
				rtsTagsList.add(newObj);
			}
		}
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		tagstoUpdate.put("rtsTags", rtsTagsList);
		if (mdTagsList != null && mdTagsList.size() > 0)
			tagstoUpdate.put("tags", mdTagsList);
		LOGGER.info("rtsTags are getting updated in "
				+ memberMDTagsCollection.getDB().getName()
				+ " for memberId : '" + l_id + "'");
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	public void deleteMemberMDTag(String l_id, String mdtag) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (doc != null) {
			if (doc.containsField("rtsTags")) {
				List<String> rtsTagsToBeRemoved = new ArrayList<String>();
				rtsTagsToBeRemoved.add(mdtag);
				updateTags(l_id, rtsTagsToBeRemoved, doc, "rtsTags");
			}

			else if (doc.containsField("tags")) {
				List<String> tagsToBeRemoved = new ArrayList<String>();
				tagsToBeRemoved.add(mdtag);
				updateTags(l_id, tagsToBeRemoved, doc, "tags");
			}
		}
	}

	/**
	 * @param l_id
	 * @param mdtag
	 * @param doc
	 */
	private void updateTags(String l_id, List<String> tagsToBeRemoved,
			DBObject doc, String docName) {
		BasicDBList rtsTagsList;
		BasicDBObject tagObjToBeDeleted;
		// rtsTagsList = (BasicDBList) doc.get("rtsTags");
		rtsTagsList = (BasicDBList) doc.get(docName);
		for (String tagToBeRemoved : tagsToBeRemoved) {
			tagObjToBeDeleted = isTagExists(tagToBeRemoved, rtsTagsList);
			if (tagObjToBeDeleted != null) {

				rtsTagsList.remove(tagObjToBeDeleted);
			}
		}

		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		tagstoUpdate.put(docName, rtsTagsList);
		LOGGER.info("tags are getting updated in "
				+ memberMDTagsCollection.getDB().getName() + "for memberId : '"
				+ l_id + "'");
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	public void deleteMemberMDTags(String l_id, List<String> inactiveTop5TagsLst) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (doc != null) {
			if (doc.containsField("rtsTags")) {

				updateTags(l_id, inactiveTop5TagsLst, doc, "rtsTags");
			}

			else if (doc.containsField("tags")) {

				updateTags(l_id, inactiveTop5TagsLst, doc, "tags");
			}
		}
	}

}
