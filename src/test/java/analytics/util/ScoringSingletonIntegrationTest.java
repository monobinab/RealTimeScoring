package analytics.util;
import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.MemberRTSChanges;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ScoringSingletonIntegrationTest {
	
	private static ScoringSingleton scoringSingletonObj;
	private static DB db;
	private static FakeMongoStaticCollection fakeMongoStaticCollection;
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
			
		SystemPropertyUtility.setSystemProperty();
		fakeMongoStaticCollection = new FakeMongoStaticCollection();
		db = SystemPropertyUtility.getDb();
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		scoringSingletonObj = constructor.newInstance();
	}
	

	private void getChangedMemberVarColl(String l_id)
			throws ParseException {
		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("10", 1.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())).append(
								"10",
								new BasicDBObject("v", expected2.getValue()).append("e",
										expected2.getExpirationDateAsString()).append("f",
										expected2.getEffectiveDateAsString())));
	}
	private void getMemberInfoColl(String l_id) {
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y").append("st_cd", "TN"));
	}
	private void getMemberVarCollection(String l_id) {
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1).append("10",0.4));
	}
	private Map<String, String> newChangesVarValueMap() {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		return newChangesVarValueMap;
	}

	private Map<String, String> getNewChangesBoostVarValueMap() {
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
		newChangesVarValueMap.put("BOOST_SYW_VARIABLE7", "0.1");
		return newChangesVarValueMap;
	}
	
	//a positive case for modelId 35 (for topology call)
	// the member has non-expired variables VARIABLE4 AND VARIABLE10, which are associated with modelId 35 
	//so, their values and exp dates are set by their corresponding strategy, and used in scoring
	@Test
	public void calcRTSChangesTest() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		
		String l_id = "SearsIntegrationTesting";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
	//	getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
	
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
		
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer(1));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double(1.1));
		int compareVal3 = new Double(0.9937568022504835).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(date, actualVar4Date);
		Assert.assertEquals(date, actualVar10Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}

	/*
	 * 	same test as calcRTSChangesTest except that the member has no state in memberInfo coll or does not have record in the collection
	 */
	@Test
	public void calcRTSChangesTestWithNoState() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting2";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
	
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 1.1));
		int compareVal3 = new Double(0.9937568022504835).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(date, actualVar4Date);
		Assert.assertEquals(date, actualVar10Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * a positive case for modelId 35 (for topology call)
		the incoming feed corresponds to a BOOST variable which boosts the score
		same data as calcRTSChanges with an additional Boost_syw_variable7 as the incoming var
		so, the score got boosted from 0.09937568022504835 (by calcRTSChanges test) to 0.10037568022504835
	 */
	@Test
	public void calcRTSChangesTestWithBoostScore() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting3";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
	//	getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = getNewChangesBoostVarValueMap();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date minDate = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		Date maxDate = new LocalDate(new Date()).plusDays(7).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Object actualBoostSywVar7Value = actualAllChanges.get("BOOST_SYW_VARIABLE7").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
		Date actualBoostSywVar7Date = actualAllChanges.get("BOOST_SYW_VARIABLE7").getExpirationDate();
	
		int compareVarVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVarVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 1.1));
		int compareVarVal3 = new Double(Double.valueOf( actualBoostSywVar7Value.toString())).compareTo(new Double(0.1));
		int compareScoreVal3 = new Double(1.0).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVarVal, 0);
		Assert.assertEquals(compareVarVal2, 0);
		Assert.assertEquals(compareVarVal3, 0);
		Assert.assertEquals(minDate, actualVar4Date);
		Assert.assertEquals(minDate, actualVar10Date);
		Assert.assertEquals(maxDate, actualBoostSywVar7Date);
		Assert.assertEquals(compareScoreVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(minDate), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(maxDate), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * if the score gets boosted to be greater than 1, it is set to 1.0 again 
	 */
	@Test
	public void calcRTSChangesTestWithScoreBoostedGT1() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting3_2";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		Map<String, String> newChangesVarValueMap = getNewChangesBoostVarValueMap();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date minDate = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		Date maxDate = new LocalDate(new Date()).plusDays(7).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Object actualBoostSywVar7Value = actualAllChanges.get("BOOST_SYW_VARIABLE7").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
		Date actualBoostSywVar7Date = actualAllChanges.get("BOOST_SYW_VARIABLE7").getExpirationDate();
	
		int compareVarVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVarVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 1.1));
		int compareVarVal3 = new Double(Double.valueOf( actualBoostSywVar7Value.toString())).compareTo(new Double(0.1));
		int compareScoreVal3 = new Double(1.0).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVarVal, 0);
		Assert.assertEquals(compareVarVal2, 0);
		Assert.assertEquals(compareVarVal3, 0);
		Assert.assertEquals(minDate, actualVar4Date);
		Assert.assertEquals(minDate, actualVar10Date);
		Assert.assertEquals(maxDate, actualBoostSywVar7Date);
		Assert.assertEquals(compareScoreVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(minDate), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(maxDate), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 *if the member does not have variables of interest, i.e. memberVariablesMap is empty
	 *in this case, the member does not have variables 4 and 10 which are associated with model 35
	 *but these variables, because of previous rts scoring are non-expired in changedMemVariables and hence used for the current scoring 
	 */
	@Test
	public void calcRTSChangesTestEmptyMemVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting4";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("14", 1).append("20",0.4));
		getChangedMemberVarColl(l_id);
//		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
					
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
	
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 1.1));
		int compareVal3 = new Double(0.9937568022504835).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(date, actualVar4Date);
		Assert.assertEquals(date, actualVar10Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 *  if a member does not have record in memberVariables collection i.e. memberVariablesMap is null,
	 *  then the member will be scored from incoming changes
	 */
	@Test
	public void calcRTSChangesTestNullMemVarIncChangesScoring() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting5";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
	
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 0.1));
		int compareVal3 = new Double(0.9934388068659837).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(date, actualVar4Date);
		Assert.assertEquals(date, actualVar10Date);
		Assert.assertEquals(0, compareVal3);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * If the member does not have record in memberVariables collection, and one of the variables associated with the model to be scored has defaultValue
	 * scoring will be based on incoming changes and defaultValue
	 */
	@Test  
	public void calcRTSChangesTestNullMemVarIncDefaultValueAndIncChangesScoring() throws ParseException{
		String l_id = "SearsIntegrationTesting16";
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-08-23"),
				simpleDateFormat.parse("2015-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE42", "71");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar42Value = actualAllChanges.get("VARIABLE42").getValue();
		Date actualVar42Date = actualAllChanges.get("VARIABLE42").getExpirationDate();
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoreList.get(0);
		int compareVal = new Double((Double)actualVar42Value).compareTo(new Double((Double)71.0));
		int compareVal3 = new Double(0.9999999598241609).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(date, actualVar42Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*to test a black out model
	 *BLACKOUT_VARIABLE is associated with modelId 46 and is the new incoming variable for this member
	 *even before getting into the logic of scoring, it should be blacked out with a score of 0.0 
	 *with expiration dates set based on StrategyBlackout (30 days for now)*/
	@Test
	public void calcRTSChangesTestBlackoutVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting6";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("BLACKOUT_VARIABLE", "0.01");
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore changedMemberScore = changedMemberScoreList.get(0);
		
		Date expDate = new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate();
		String expirationDate = simpleDateFormat.format(expDate);
		
		Assert.assertEquals("Expecting score of 0 as this model is blacked out", 0.0, changedMemberScore.getScore());
		Assert.assertEquals(expirationDate, changedMemberScore.getMinDate());
		Assert.assertEquals(expirationDate, changedMemberScore.getMaxDate());
	}
	
	/*
	 * to test if a variable (INVALIDVARIABLE)which is associated with a model 48 is there in our modelVariables collection )
	 * but not in variables collection at all, calcBaseScore throws exception which get caught in calcRTSChanges
	 * and the corresponding model will not be populated in changedMemberScore list from calcRTSChanges() method
	 * Here, we have only one model to be scored and the variable is not in variables coll, so we get changedMemberScore list with size 0
	 */
	
	@Test
	public void calcRTSChangesTestInvalidVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting7";
		
		getMemberVarCollection(l_id);

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
						
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("INVALIDVARIABLE", "0.01");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		List<ChangedMemberScore> changedMemScoresList = memberRTSChanges.getChangedMemberScoreList();
		Assert.assertEquals(0, changedMemScoresList.size());
	}
	
	/*
	 * If changedMemvar collection contains non-expired invalid variable for this member
	 * i.e. a variable NOT in Variables collection, faced this problem and got exception in PRODUCTION
	 * code handles it by having  a check in createChangedMemberVariablesMap method to discard that variable to get populated in the map
	*/
	@Test
	public void calcRTSChangesTestInvalidVarInChangedMemVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting7_1";
		
		getMemberVarCollection(l_id);

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4000", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4000",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
						
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		List<ChangedMemberScore> changedMemScoresList = memberRTSChanges.getChangedMemberScoreList();
		Assert.assertEquals(3, changedMemScoresList.size());
	}
	
	/*
	 * if a variable "variable4" is expired in changedMemberVariable, 
	 * its value should be picked up from MemberVariable collection, if exists
	 */
	@Test
	public void calcRTSChangesTestWithOneVarExp() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting8";
		
		getMemberVarCollection(l_id);

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2014-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("10", 1.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())).append(
								"10",
								new BasicDBObject("v", expected2.getValue()).append("e",
										expected2.getExpirationDateAsString()).append("f",
										expected2.getEffectiveDateAsString())));
		
	//	getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
	
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) 1));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) 1.1));
		int compareVal3 = new Double(0.9937568022504835).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(date, actualVar4Date);
		Assert.assertEquals(date, actualVar10Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	@Test
	public void calcRTSChangesWithEmptyNewChangesMapTest() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting9";
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, new HashMap<String, String>(), null, "TEST");
		Assert.assertEquals("no_vars_ofinterest", memberRTSChanges.getMetricsString());;
	}
	
	/*
	 * api's call to scoring, a positive case
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void executeTest() throws ParseException{
		
		String l_id = "apiLid";
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);

		//fake changedMemberScore collection
		//empty changedMemberScore collection before update
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
	
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		
		//this method updates the changedMemberScore collection
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScoresUpdated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("35");
		Assert.assertEquals(0.9946749823142578, changedMemScoresUpdated.get("s"));
		Assert.assertEquals("2999-09-23", changedMemScoresUpdated.get("minEx"));
		Assert.assertEquals("2999-09-23", changedMemScoresUpdated.get("maxEx"));
		Assert.assertEquals(0.9946749823142578, actuaModelIdStringScoreMap.get("35"));
		changedMemberScore.remove(new BasicDBObject("l_id", l_id));
		
	}
	
	@Test
	public void executeWithEmptyModelListsTest() throws ParseException{
		
		String l_id = "apiLid2";
		ArrayList<String> modelLists = new ArrayList<String>();
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		Assert.assertTrue("Expecting an emptymodelIdStringScoreMap as modelList passed is empty", actuaModelIdStringScoreMap.isEmpty());
	}
	
	@Test
	public void executeWithNullModelListsTest() throws ParseException{
		
		String l_id = "apiLid3";
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, null,  "TEST");
		Assert.assertTrue("Expecting an emptymodelIdStringScoreMap as modelList passed is empty", actuaModelIdStringScoreMap.isEmpty());
	}
	
	/**
	 *if allChanges is null, calcScore will throw exception, which gets caught in calcRTSChanges method
	 *so, changedMemberScoreList will be empty
	 ***/
	
	@Test
	public void executeWithNullAllChangesTest() throws ParseException{
		
		String l_id = "apiLid4";
		getMemberVarCollection(l_id);
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists, "TEST");
		Assert.assertEquals("Expecting an empty map as List of ChangedMemScore is empty returned by calcRTSChanges", new HashMap<String, Double>(), actuaModelIdStringScoreMap);
	}
	
	/*
	 * If allChanges (allChanges = changedMemVarMap as it is from api) does not contain any variable associated with modelId to be scored
	 * min max Expiry will be set with null
	 * so, changedMemberScore should be updated with current date for their expiration
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void executeWithAllChangesNotHavingVarsOfInterestForModelToBeScoredTest() throws ParseException{
		
		String l_id = "apiLid5";
		getMemberVarCollection(l_id);
		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"40",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
			
		//fake changedMemberScore collection
		//empty changedMemberScore collection before update
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
	
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		
		//this method updates the changedMemberScore collection
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScoresUpdated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("35");
		Assert.assertEquals(0.9935358588660986, changedMemScoresUpdated.get("s"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScoresUpdated.get("minEx"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScoresUpdated.get("maxEx"));
		Assert.assertEquals(0.9935358588660986, actuaModelIdStringScoreMap.get("35"));
		changedMemberScore.remove(new BasicDBObject("l_id", l_id));
	}
	
	/*
	 * If all newChangesVarValueMap variables are of NONE strategy, no models will be populated for scoring
	 * memberRTSChanges will be instantiated with no_vars_ofinterest as metricsString
	 */
	@Test
	public void calcRTSChangesWithAllVarsOfNONEStrategy() throws ParseException{
		
		String l_id = "SearsIntegrationTesting10";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("12", 1).append("40",0.4).append("4", .1).append("10", 0.1));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE12", "0.01");
		newChangesVarValueMap.put("VARIABLE40", "0.1");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
	
		Assert.assertEquals("no_vars_ofinterest", memberRTSChanges.getMetricsString());
	}
	

	/*
	 * variable 40 and variable 12 associated with model 50 are of NONE strategy, so these variables will be filtered out in modelLists
	 * model 50 will not be populated for scoring
	 */
	@Test
	public void calcRTSChangesWithSomeVarsOfNONEStrategy() throws ParseException{
		
		String l_id = "SearsIntegrationTesting11";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("12", 1).append("40",0.4).append("4", .1).append("10", 0.1));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE12", "0.01");
		newChangesVarValueMap.put("VARIABLE40", "0.1");
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		for(ChangedMemberScore changedMemScore : changedMemberScoresList){
			if(changedMemScore.getMinDate() == null){
				fail("Got null minDate");
			}
		}
		Assert.assertEquals(3, changedMemberScoresList.size());
	}
	/*
	 * variable4 = [35, 55, 65], variable10 = [50, 65]
	 * variable10 has NONE strategy, will be discarded
	 * model 50 has only variable10 associated and hence will not be populated for scoring
	 */
	@Test
	public void calcRTSChangesWithSharedVarsOfNONEStrategyForOneModel() throws ParseException{
		
		String l_id = "SearsIntegrationTesting12";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("12", 1).append("40",0.4).append("4", .1).append("10", 0.1));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2014-08-23"),
				simpleDateFormat.parse("2014-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		for(ChangedMemberScore changedMemScore : changedMemberScoresList){
			if(changedMemScore.getMinDate() == null){
				fail("Got null minDate");
			}
		}
		Assert.assertEquals(3, changedMemberScoresList.size());
	}
	
	/*
	 * If the incoming changes contain variables with default value, the defaultValue will be picked up from allChanges map itself
	 */
	@Test  
	public void calcRTSChangesWithDefaultValueInNewChanges() throws ParseException{
		String l_id = "SearsIntegrationTesting13";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2014-08-23"),
				simpleDateFormat.parse("2014-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE41", "731");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar41Value = actualAllChanges.get("VARIABLE41").getValue();
		Date actualVar41Date = actualAllChanges.get("VARIABLE41").getExpirationDate();
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoreList.get(0);
		int compareVal = new Double((Double) actualVar41Value).compareTo(new Double((Double) 731.0));
		int compareVal3 = new Double(0.9999998834563687).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(date, actualVar41Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * If the variable with defaultValue is un-expired in CMV, it will be picked up from CMV for scoring
	 */
	@Test  
	public void calcRTSChangesWithDefaultValueInCMV() throws ParseException{
		String l_id = "SearsIntegrationTesting14";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("41", 731,
				simpleDateFormat.parse("2999-08-23"),
				simpleDateFormat.parse("2015-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"41",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE42", "71");
		Date maxDate = simpleDateFormat.parse("2999-08-23");
		Date minDate = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar41Value = actualAllChanges.get("VARIABLE41").getValue();
		Date actualVar41Date = actualAllChanges.get("VARIABLE41").getExpirationDate();
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoreList.get(0);
		int compareVal = new Integer((Integer)actualVar41Value).compareTo(new Integer((Integer)731));
		int compareVal3 = new Double(0.9999999598241609).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(maxDate, actualVar41Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(minDate), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(maxDate), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * If the variable with defaultValue is there in memberVariables collection, it will be picked up from mbrVar for scoring
	 */
	@Test  
	public void calcRTSChangesWithDefaultValueInmbrVariables() throws ParseException{
		String l_id = "SearsIntegrationTesting15";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("41", 731));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-08-23"),
				simpleDateFormat.parse("2015-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE42", "71");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar42Value = actualAllChanges.get("VARIABLE42").getValue();
		Date actualVar42Date = actualAllChanges.get("VARIABLE42").getExpirationDate();
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoreList.get(0);
		int compareVal = new Double((Double)actualVar42Value).compareTo(new Double((Double)71.0));
		int compareVal3 = new Double(0.9999999598241609).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(date, actualVar42Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	/*
	 * If the variable with defaultValue is NOT there in memberVariables, CMV and incoming new changes, it will be picked up from variables collection for scoring
	 */
	@Test  
	public void calcRTSChangesWithDefaultValueNotInmbrVarCMVandIncnewChanges() throws ParseException{
		String l_id = "SearsIntegrationTesting16";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("40", 731));
		
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-08-23"),
				simpleDateFormat.parse("2015-08-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE42", "71");
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar42Value = actualAllChanges.get("VARIABLE42").getValue();
		Date actualVar42Date = actualAllChanges.get("VARIABLE42").getExpirationDate();
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoreList.get(0);
		int compareVal = new Double((Double)actualVar42Value).compareTo(new Double((Double)71.0));
		int compareVal3 = new Double(0.9999999598241609).compareTo(new Double(actualChangedMemberScore.getScore()));
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(date, actualVar42Date);
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(simpleDateFormat.format(date), actualChangedMemberScore.getMaxDate());
	}
	
	@AfterClass
	public static void cleanUp(){
		SystemPropertyUtility.dropDatabase();
	}
}
