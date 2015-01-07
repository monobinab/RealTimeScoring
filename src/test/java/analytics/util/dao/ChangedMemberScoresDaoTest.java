package analytics.util.dao;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.objects.ChangedMemberScore;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class ChangedMemberScoresDaoTest {
	static String lId = "oI8ko3pdaHrhdlI3MJIXMPgSCX";
    DB db;
	@Before
	public void initialize() throws ConfigurationException {
		// DO NOT REMOVE BELOW LINE
		System.setProperty("rtseprod", "test");
		//Ensure we have an empty DB
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		db = DBConnection.getDBConnection();
	}
	@After
	public void cleanUp(){
		db.dropDatabase();
	}
	@Test
	public void testValidScoresCanBeRetrieved() {
		
		//Empty before we start
		String date = "09/18/2014";
		
		ChangedMemberScore score1= new ChangedMemberScore(0.1, date, date, date,"test");
		ChangedMemberScore score2= new ChangedMemberScore(0.999, date, date, date,"test");
		ChangedMemberScore score3= new ChangedMemberScore(0.2181, date, date, date,"test");
		ChangedMemberScoresDao  changedMemberScoresDao= new ChangedMemberScoresDao();
		 Map<Integer, ChangedMemberScore> myChangedScores = new HashMap<Integer, ChangedMemberScore>();
		 myChangedScores.put(1, score1);
		 myChangedScores.put(2, score2);
		 myChangedScores.put(3, score3);
		changedMemberScoresDao.upsertUpdateChangedScores(lId, myChangedScores);
		//There is only one doc
		DBObject obj = changedMemberScoresDao.changedMemberScoresCollection.findOne();
		Assert.assertEquals(lId, obj.get(MongoNameConstants.L_ID));
		compareScoreObject(score1, (DBObject) obj.get("1"));
		compareScoreObject(score2, (DBObject) obj.get("2"));
		compareScoreObject(score3, (DBObject) obj.get("3"));
		
	}

	private void compareScoreObject(ChangedMemberScore score, DBObject scoreObj){
		Assert.assertEquals(score.getScore(), scoreObj.get(MongoNameConstants.CMS_SCORE));
		Assert.assertEquals(score.getEffDate(), scoreObj.get(MongoNameConstants.CMS_EFFECTIVE_DATE));
		Assert.assertEquals(score.getMinDate(), scoreObj.get(MongoNameConstants.CMS_MIN_EXPIRY_DATE));
		Assert.assertEquals(score.getMaxDate(), scoreObj.get(MongoNameConstants.CMS_MAX_EXPIRY_DATE));
	}
	
	@Test
	public void testEmptyScoreHasOnlyLidPresent(){
		ChangedMemberScoresDao  changedMemberScoresDao= new ChangedMemberScoresDao();
		Map<Integer, ChangedMemberScore> myChangedScores = new HashMap<Integer, ChangedMemberScore>();
		myChangedScores.put(1, null);
		changedMemberScoresDao.upsertUpdateChangedScores(lId, myChangedScores);
		DBObject obj = changedMemberScoresDao.changedMemberScoresCollection.findOne();
		Assert.assertNull(obj);
	}
}
