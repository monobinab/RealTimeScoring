package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;

public class MemberUUIDDaoTest {
	private static MemberUUIDDao memberUUIDao;
	private static List<String> seedDataLIds;
	private static List<String> seedDataUuids;

	@BeforeClass
	public static void initialize() {
		// DO NOT REMOVE BELOW LINE
		System.setProperty("rtseprod", "test");

		
		seedDataLIds = new ArrayList<String>();
		seedDataUuids = new ArrayList<String>();
		seedDataLIds.add("oI8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("79477233159864516052651937987747896979");
		seedDataLIds.add("oI8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("69477233159864516052651937987747896979");
		seedDataLIds.add("pI8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("79477233159864516052651937987747896979");
		seedDataLIds.add("pI8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("69477233159864516052651937987747896979");
		seedDataLIds.add("9I8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("49477233159864516052651937987747896979");
		seedDataLIds.add("8I8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("39477233159864516052651937987747896979");
		seedDataLIds.add("8I8ko3pdaHrhdlI3MJIXMPgSCX=");
		seedDataUuids.add("29477233159864516052651937987747896979");
		memberUUIDao = new MemberUUIDDao();
		for (int i = 0; i < seedDataLIds.size(); i++) {
			memberUUIDao.memberUuidCollection.insert(new BasicDBObject(
					MongoNameConstants.L_ID, seedDataLIds.get(i)).append(
					MongoNameConstants.MUUID_UUID, seedDataUuids.get(i)));
		}
	}

	@Before
	public void setupTest() {
		//Ensure we have an empty DB
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
	}

	@Test
	public void testEmptyUUIDForNullLoyaltyId() {

		List<String> Uuids = memberUUIDao.getLoyaltyIdsFromUUID(null);
		Assert.assertTrue(Uuids.isEmpty());
	}

	@Test
	public void testNoForLoyaltyId() {
		List<String> Uuids = memberUUIDao
				.getLoyaltyIdsFromUUID("59477233159864516052651937987747896979");
		Assert.assertTrue(Uuids.isEmpty());
	}

	@Test
	public void testUniqueUUIDForLoyaltyId() {

		List<String> Uuids = memberUUIDao
				.getLoyaltyIdsFromUUID("39477233159864516052651937987747896979");
		Assert.assertEquals(1, Uuids.size());
	}

	@Test
	public void testMultipleUUIDsForLoyaltyId() {

		List<String> Uuids = memberUUIDao
				.getLoyaltyIdsFromUUID("79477233159864516052651937987747896979");
		Assert.assertEquals(2, Uuids.size());
	}
}
