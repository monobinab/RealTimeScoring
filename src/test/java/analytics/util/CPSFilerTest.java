package analytics.util;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import analytics.exception.RealTimeScoringException;
import analytics.util.CPSFiler;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.OccasionDao;
import analytics.util.dao.OutboxDao;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.OccasionInfo;
import analytics.util.objects.TagMetadata;

@RunWith(MockitoJUnitRunner.class)
public class CPSFilerTest {

	@Mock
	private OutboxDao outboxDao;
	
	@Mock 
	private MemberInfoDao memberInfoDao;
	
	@Mock
	private OccasionDao occasionDao;
	
	@InjectMocks
	private CPSFiler cpsFiler;
	
	private static TagMetadata tagMetadata;
	private static List<TagMetadata> occasionsList;
	private static EmailPackage emailPackage;
	private static EmailPackage inProgressEmailPackage;
	private static List<EmailPackage> emailPackages;
	private static MemberInfo memberInfo;
	private static Date addedDateTime;
	private static Date sendDate;
	private static Date sentDateTime;
	private static List<String> activeTags;
	private static List<OccasionInfo> occasionInfos;
	private static OccasionInfo occasionInfo;
	private SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	
	@BeforeClass
	public static void setUp(){
		
		/** TagMetaData Constructor parameters
		 * @param mdTag
		 * @param businessUnit
		 * @param subBusinessUnit
		 * @param purchaseOccasion
		 * @param first5CharMdTag
		 * @param percentile
		 * @param emailOptIn
		 * @param divLine
		 * @param priority
		 * @param sendDuration
		 */
		
		/** EmailPackage Constructor parameters
		 * @param mdTagMetaData
		 * @param addedDateTime
		 * @param sendDate
		 * @param sentDateTime
		 * @param status
		 * @param memberId
		 * @param memberInfo
		 * @param custEventNm
		 * @param topologyName
		 */
		
		/** MemeberInfo Constructor parameters
		 * @param eid
		 * @param emailOptIn
		 * @param state
		 * @param srs_opt_in
		 * @param kmt_opt_in
		 * @param syw_opt_in
		 */
		
		occasionsList = new ArrayList<TagMetadata>();
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Duress",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 1, 8, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Replacement",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 2, 8, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Pre-move",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 3, 60, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Post-move",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 4, 60, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Remodel",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 5, 8, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Replace_by_age",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 6, 3, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Browse",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 7, 8, 30);
		occasionsList.add(tagMetadata);
		tagMetadata = new TagMetadata(StringUtils.EMPTY,StringUtils.EMPTY,StringUtils.EMPTY,"Top 5% of MSM",StringUtils.EMPTY,null,StringUtils.EMPTY,StringUtils.EMPTY, 8, 3, 30);
		occasionsList.add(tagMetadata);
		
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Duress");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("CECAS823600153010","Electronics","Sears Camera","Top 5% of MSM","CECAS",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Unknown");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("CEGES823600153010","Electronics","Sears Electronics","Top 5% of MSM","CEGES",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Top 5% of MSM");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
			
		activeTags = new ArrayList<String>();
		activeTags.add("HARFS");
		activeTags.add("HALAS");
		activeTags.add("HADHS");
		activeTags.add("HAGAS");
		activeTags.add("HAARS");
		activeTags.add("HACKS");
		activeTags.add("HAVCS");
		
		occasionInfos = new ArrayList<OccasionInfo>();
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Duress");
		occasionInfo.setPriority("1");
		occasionInfo.setDuration("8");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(4442);
		occasionInfo.setIntCustEvent("RTS_Duress");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Replacement");
		occasionInfo.setPriority("2");
		occasionInfo.setDuration("8");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(5502);
		occasionInfo.setIntCustEvent("RTS_ReplacementEXPLICIT");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Pre-move");
		occasionInfo.setPriority("3");
		occasionInfo.setDuration("60");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(5562);
		occasionInfo.setIntCustEvent("RTS_PreMover");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Post-move");
		occasionInfo.setPriority("4");
		occasionInfo.setDuration("60");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(5542);
		occasionInfo.setIntCustEvent("RTS_PostMover");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Remodel");
		occasionInfo.setPriority("5");
		occasionInfo.setDuration("8");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(4402);
		occasionInfo.setIntCustEvent("RTS_Remodeling");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Replace_by_age");
		occasionInfo.setPriority("6");
		occasionInfo.setDuration("3");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(5522);
		occasionInfo.setIntCustEvent("RTS_ReplacementAGE");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Browse");
		occasionInfo.setPriority("7");
		occasionInfo.setDuration("8");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(4442);
		occasionInfo.setIntCustEvent("RTS_Browse");
		occasionInfos.add(occasionInfo);
		
		occasionInfo = new OccasionInfo();
		occasionInfo.setOccasion("Top 5% of MSM");
		occasionInfo.setPriority("8");
		occasionInfo.setDuration("3");
		occasionInfo.setDaysToCheckInHistory("30");
		occasionInfo.setCustEventId(4462);
		occasionInfo.setIntCustEvent("RTS_Unknown");
		occasionInfos.add(occasionInfo);
		
	}
	
	
	//Test Case 1 - Received two occasions - Duress & Replacement  & no occasion is in progress, 
	//Expected Result - queue both occasions with duress to be sent today and replacement after 8 days	
	@Test
	public void testDecideSendDatesWhenNoOccasionInProgress() throws SQLException, RealTimeScoringException{
		SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
		emailPackages = new ArrayList<EmailPackage>();
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");	
		
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Duress");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("HAGAS2610072010","Home Appliance","Sears appliance","Replacement","HAGAS",81.0,"Y","",2, 8, 30);
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Replacement");
		emailPackage.setMemberInfo(memberInfo);	
		emailPackages.add(emailPackage);
	
		List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,null);
		Assert.assertNotNull(retEmailPackages);
		assertEquals(retEmailPackages.size(),2);
		assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()),sdformat.format(Calendar.getInstance().getTime()));
		assertEquals(sdformat.format(retEmailPackages.get(1).getSendDate()),sdformat.format(new DateTime(retEmailPackages.get(0).getSendDate()).plusDays(8).toDate()));			
		
	}
	
	//Test Case 2 - Received two occasions - Duress & Replacement ; Duress is already in progress), 
	//Expected Result - queue replacement after 8 days of Duress's send date
	@Test
	public void testDecideSendDatesWhenOccasionInProgress() throws RealTimeScoringException, ParseException{
		SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
		emailPackages = new ArrayList<EmailPackage>();
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");	
		
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("HAGAS2610072010","Home Appliance","Sears appliance","Replacement","HAGAS",81.0,"Y","",2, 8, 30);
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
		inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);
		inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
	
		List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,inProgressEmailPackage);		
		Assert.assertNotNull(retEmailPackages);
		assertEquals(retEmailPackages.size(),1);			
		assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()),sdformat.format(new DateTime(inProgressEmailPackage.getSendDate()).plusDays(8).toDate()));			
		
	}
	
		//Test Case 3 - Received two occasions - Duress & Replacement & Browse ; Replacement is already in progress), 
		//Expected Result - Replacement is interrupted as duress is of higher priority. Duress is set to be sent tomorrow
		@Test
		public void testDecideSendDatesWhenHigherPriorityComesIn() throws RealTimeScoringException, ParseException{
			SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
			emailPackages = new ArrayList<EmailPackage>();
			memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");	
			
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HAGAS2610072010","Home Appliance","Sears appliance","Replacement","HAGAS",81.0,"Y","",2, 8, 30);
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HADHS723600153010","Home Appliance","Sears Dishwasher","Browse","HADHS",91.0,"Y","", 7, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HAGAS2610072010","Home Appliance","Sears appliance","Replacement","HAGAS",81.0,"Y","",2, 8, 30);
			inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);		
			inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
		
			List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,inProgressEmailPackage);	
			
			Assert.assertNotNull(retEmailPackages);
			assertEquals(retEmailPackages.size(),2);			
			assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()),sdformat.format(new DateTime().plusDays(1).toDate()));	//check if send date is set to be today's date		
			assertEquals(sdformat.format(retEmailPackages.get(1).getSendDate()),sdformat.format(new DateTime().plusDays(9).toDate()));	
		}
		
		
		//Test Case 4 - Received two occasions - Replacement & Browse ; Duress is already in progress; it didn't come in again today), 
		//Expected Result - Duress is interrupted; Replacement is set to be sent tomorrow ; Browse is set to be sent after 8 days
		@Test
		public void testDecideSendDatesWhenInProgressIsNotActive() throws RealTimeScoringException, ParseException{
			SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
			emailPackages = new ArrayList<EmailPackage>();
						
			tagMetadata = new TagMetadata("HAGAS2610072010","Home Appliance","Sears appliance","Replacement","HAGAS",81.0,"Y","",2, 8, 30);
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HADHS723600153010","Home Appliance","Sears Dishwasher","Browse","HADHS",91.0,"Y","", 7, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);
			inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);		
			inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
		
			List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,inProgressEmailPackage);	
			
			Assert.assertNotNull(retEmailPackages);			
			assertEquals(retEmailPackages.size(),2);			
			assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()),sdformat.format(new DateTime().plusDays(1).toDate()));//check if send date is set to be tomorrow's date	
			assertEquals(sdformat.format(retEmailPackages.get(1).getSendDate()),sdformat.format(new DateTime().plusDays(9).toDate()));	
			
		}
		
		//Test Case 5 - Received three occasions - CK Replacement, DH Replacement, FT Browse ;  DH Replacement is already in progress), 
		//Expected Result - DH Replacement continues, CK Replacement and DH Browse are queued
		@Test
		public void testDecideSendDatesWhenEqualPriorityComesIn() throws RealTimeScoringException, ParseException{
			SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
			emailPackages = new ArrayList<EmailPackage>();
			tagMetadata = new TagMetadata("HACKS2610072010","Home Appliance","Sears Cooktop","Replacement","HACKS",81.0,"Y","",2, 8, 30);
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);			
			
			tagMetadata = new TagMetadata("HADHS2610072010","Home Appliance","Sears Dishwasher","Replacement","HADHS",81.0,"Y","",2, 8, 30);
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("SPFTS723600153010","Sporting Goods","Sears Fitness","Browse","SPFTS",91.0,"Y","", 7, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HADHS2610072010","Home Appliance","Sears Dishwasher","Replacement","HADHS",81.0,"Y","",2, 8, 30);
			inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);		
			inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
		
			List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,inProgressEmailPackage);		
			
			Assert.assertNotNull(retEmailPackages);		
			assertEquals(retEmailPackages.size(),2);			
			assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()),sdformat.format(new DateTime(inProgressEmailPackage.getSendDate()).plusDays(8).toDate()));//check if send date is set to be today's date	
			assertEquals(sdformat.format(retEmailPackages.get(1).getSendDate()),sdformat.format(new DateTime(retEmailPackages.get(0).getSendDate()).plusDays(8).toDate()));	
			
		}
		
		//Test Case 6 - Received one occasion - Duress  ; Duress is already in progress), 
		//Expected Result - no need to queue again ; let the communication continue
		@Test
		public void testDecideSendDatesWhenNoOtherOccasionCameIn() throws RealTimeScoringException, ParseException{			
			emailPackages = new ArrayList<EmailPackage>();
						
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			emailPackages.add(emailPackage);			
			
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
			inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);
			inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
		
			List<EmailPackage> retEmailPackages = cpsFiler.decideSendDates(emailPackages,inProgressEmailPackage);	
			Assert.assertNotNull(retEmailPackages);				
			assertEquals(retEmailPackages.size(),0);			
		}
			
		
		public void testArePackagesSame(){
			SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
			List<EmailPackage> incomingEmailPackages = new ArrayList<EmailPackage>();
			List<EmailPackage> currentEmailPackages = new ArrayList<EmailPackage>();
			memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");	
			
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
			emailPackage = new EmailPackage("7081103948483127",tagMetadata);
			incomingEmailPackages.add(emailPackage);
			
			tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8,30);	
			inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);
			//inProgressEmailPackage.setSendDate(sdformat.parse("08/23/2015"));
			inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());//today - 3
			currentEmailPackages.add(emailPackage);
			Assert.assertTrue(cpsFiler.arePackagesSame(incomingEmailPackages, currentEmailPackages));
			
		}
	
	
	
	//Test - Incoming list has HADH duress & SPFT browse;  HADH duress was already sent in last 30 days
	//Expected Result -  HADH duress should not be sent again; SPFT is queued to send Today
	@Test
	public void testIgnorePackagesSentInHistoryWhenOccasionIsSentInLast30Days() throws SQLException, ParseException{
				
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS623600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 0, 0,30);	
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("SPFTS723600153010","Sporting Goods","Sears Fitness","Browse","SPFTS",91.0,"Y","", 7, 8,30);	
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		EmailPackage temp = emailPackage;
		
		when(outboxDao.getSentDate(emailPackages.get(0))).thenReturn(new DateTime().minusDays(25).toDate());
		when(outboxDao.getSentDate(emailPackages.get(1))).thenReturn(null);
		
		when(occasionDao.getOccasionInfo("Duress")).thenReturn(this.getOccasionInfo("Duress"));
		when(occasionDao.getOccasionInfo("Browse")).thenReturn(this.getOccasionInfo("Browse"));
		
		List<EmailPackage> retEmailPackages = cpsFiler.ignorePackagesSentInHistory(emailPackages);
		Assert.assertNotNull(retEmailPackages);	
		assertEquals(retEmailPackages.size(),1);		
		assertEquals(retEmailPackages.get(0), temp);
	}
	
	//Test - Incoming package is Duress which was sent beyond 30 days, so it should be sent again
	@Test
	public void testIgnorePackagesSentInHistoryWhenOccasionIsSentBeyond30Days()throws SQLException, ParseException{
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS623600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 0, 0,30);			
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		EmailPackage temp = emailPackage;
		
		when(outboxDao.getSentDate(emailPackage)).thenReturn(new DateTime().minusDays(100).toDate());
		when(occasionDao.getOccasionInfo("Duress")).thenReturn(this.getOccasionInfo("Duress"));
		
		List<EmailPackage> retEmailPackages = cpsFiler.ignorePackagesSentInHistory(emailPackages);
		Assert.assertNotNull(retEmailPackages);	
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0), temp);
	}
	
	//Test - Incoming package is Duress which was NOT sent in the past, so it should be sent 
	@Test
	public void testIgnorePackagesSentInHistoryWhenOccasionIsNotSentInLast30Days() throws SQLException, ParseException{
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS623600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 0, 0,30);		
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		EmailPackage temp = emailPackage;
		
		//when(outboxDao.getSentDate((EmailPackage)anyObject())).thenReturn(null);
		when(outboxDao.getSentDate(emailPackage)).thenReturn(null);
		when(occasionDao.getOccasionInfo("Duress")).thenReturn(this.getOccasionInfo("Duress"));
		
		List<EmailPackage> retEmailPackages = cpsFiler.ignorePackagesSentInHistory(emailPackages);
		Assert.assertNotNull(retEmailPackages);	
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0), temp);
	}
	
	//Test - Incoming package is Duress which is sent within last 8 days, so it's communication
	//should be in progress until its duration is exhausted or its communication is interrupted. 
	@Test
	public void testIgnorePackagesSentInHistoryWhenOccasionIsSentInLast2Days() throws SQLException, ParseException{
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS623600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 0, 0,30);		
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackages.add(emailPackage);
		EmailPackage temp = emailPackage;
		
		when(outboxDao.getSentDate(emailPackage)).thenReturn(new DateTime().minusDays(2).toDate());
		when(occasionDao.getOccasionInfo("Duress")).thenReturn(this.getOccasionInfo("Duress"));
		
		List<EmailPackage> retEmailPackages = cpsFiler.ignorePackagesSentInHistory(emailPackages);
		Assert.assertNotNull(retEmailPackages);	
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0), temp);
	}
	
	//TEST - response has duress, replace_by_age, replace_by_age,top 5% of MSM,  top 5% of MSM and other non mdtags
	//Expected - return list should have only duress, replace_by_age, replace_by_age, top 5% of MSM,  top 5% of MSM
	@Test 
	public void testGetValidOccasionsList() throws JSONException{
		String resp = "{\"status\":\"success\", \"statusCode\":\"200\", \"memberId\":\"7081057547176153\", \"lastUpdated\":\"2015-08-26 00:46:43\", \"scoresInfo\":"
				+ "[{\"modelId\":\"57\", \"modelName\":\"S_SCR_REGRIG\", \"format\":\"Sears\", "
				+ "\"category\":\"Refrigerator Model\", \"tag\":\"0102\",\"mdTag\":\"HARFS111700153010\","
				+ "\"occassion\":\"Duress\",\"brand\":\"Kenmore\",\"subBusinessUnit\":\"Sears Refrigerator\","
				+ "\"businessUnit\":\"Home Appliance\",\"scoreDate\":\"2015-08-24\",\"score\":0.075496672 ,"
				+ "\"percentile\":100, \"rank\":1},"
				+ "{\"modelId\":\"61\", \"modelName\":\"S_SCR_TV\", \"format\":\"Sears\", "
				+ "\"category\":\"TV Model\", \"tag\":\"0000\",\"mdTag\":\"CETVS623600153010\","
				+ "\"occassion\":\"Replace_by_age\",\"subBusinessUnit\":\"Sears Television\","
				+ "\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-24\",\"score\":0.0021435 ,"
				+ "\"percentile\":94, \"rank\":2},"
				+ "{\"modelId\":\"28\", \"modelName\":\"S_SCR_CE_CAMERA\", \"format\":\"Sears\","
				+ "\"category\":\"Consumer Electronics - camera\", \"tag\":\"0000\",\"mdTag\":\"CECAS623600153010\","
				+ "\"occassion\":\"Replace_by_age\",\"subBusinessUnit\":\"Sears Camera\","
				+ "\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-24\",\"score\":0.0012705932 ,"
				+ "\"percentile\":100, \"rank\":3},"
				+ "{\"modelId\":\"49\", \"modelName\":\"S_SCR_LG_PATIO\", \"format\":\"Sears\", "
				+ "\"category\":\"Patio Furniture Model\", \"tag\":\"0160\",\"mdTag\":\"ODPFS823600153010\","
				+ "\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Patio Furniture\","
				+ "\"businessUnit\":\"outdoor Living\",\"scoreDate\":\"2015-08-24\",\"score\":0.0053853 ,"
				+ "\"percentile\":100, \"rank\":7},"
				+ "{\"modelId\":\"30\", \"modelName\":\"S_SCR_FIT_EQUIP\", \"format\":\"Sears\", "
				+ "\"category\":\"Fitness Equipment Model\", \"tag\":\"0138\","
				+ "\"businessUnit\":\"Sporting Goods\",\"scoreDate\":\"2015-08-24\",\"mdTag\":\"SPFTS823600153010\","
				+ "\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Fitness\",\"score\":0.0046268 ,"
				+ "\"percentile\":100, \"rank\":8},"
				+ "{\"modelId\":\"16\", \"modelName\":\"K_SCR_ODL\", \"format\":\"Kmart\", "
				+ "\"category\":\"Outdoor Living\", \"tag\":\"2134\",\"score\":0.0008136887 ,"
				+ "\"percentile\":68, \"rank\":66},"
				+ "{\"modelId\":\"14\", \"modelName\":\"K_SCR_LG\", \"format\":\"Kmart\", "
				+ "\"category\":\"Lawn & Garden\", \"tag\":\"2133\",\"score\":0.0004264773 ,"
				+ "\"percentile\":54, \"rank\":67},"
				+ "{\"tag\":\"0177\", \"index\":0.5334645, \"rank\":68},"
				+ "{\"tag\":\"0089\", \"index\":1.1149705, \"rank\":69}]}";		
		List<String> validtags = new ArrayList<String>();
		validtags.add("Duress");
		validtags.add("Replace_by_age");
		validtags.add("Replace_by_age");
		validtags.add("Top 5% of MSM");
		validtags.add("Top 5% of MSM");
		List<TagMetadata> validOccasionsList = cpsFiler.getValidOccasionsList(resp);
		Assert.assertNotNull(validOccasionsList);	
		Assert.assertEquals(validOccasionsList.size(), validtags.size());
		
		for(int i=0; i<validOccasionsList.size(); i++){
			Assert.assertEquals(validtags.get(i), validOccasionsList.get(i).getPurchaseOccasion());			
		}
	}
	
	
	//TEST - Incoming occasions {HARF Duress, CETV Replace_by_age, CECA Replace_by_age, ODPF Top5%, SPFT Top5%}
	//Criteria - No occasion in Progress
	//Criteria - Nothing is queued
	//Expected - HARF Duress - send Date = today
	//			 CETV Replace_by_age - send Date = today+8
	//			 CECA Replace_by_age - send Date = today+11
	@Test
	public void testPrepareEmailPackages() throws JSONException, SQLException, RealTimeScoringException{
		 sdformat = new SimpleDateFormat("MM/dd/yyyy");
		
		//String resp = "{\"status\":\"success\", \"statusCode\":\"200\", \"memberId\":\"7081035007675781\", \"lastUpdated\":\"2015-07-17 18:40:06\", \"scoresInfo\":[{\"modelId\":\"61\", \"modelName\":\"S_SCR_TV\", \"format\":\"Sears\", \"category\":\"TV Model\", \"tag\":\"0000\",\"mdTag\":\"CETVS823600153010\",\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Television\",\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-18\",\"score\":0.002288 ,\"percentile\":95, \"rank\":1},{\"modelId\":\"28\", \"modelName\":\"S_SCR_CE_CAMERA\", \"format\":\"Sears\", \"category\":\"Consumer Electronics - camera\", \"tag\":\"0000\",\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-18\",\"mdTag\":\"CECAS823600153010\",\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Camera\",\"score\":0.0005054076 ,\"percentile\":100, \"rank\":2}]}";
		String resp = "{\"status\":\"success\", \"statusCode\":\"200\", \"memberId\":\"7081057547176153\", \"lastUpdated\":\"2015-08-26 00:46:43\", \"scoresInfo\":"
				+ "[{\"modelId\":\"57\", \"modelName\":\"S_SCR_REGRIG\", \"format\":\"Sears\", "
				+ "\"category\":\"Refrigerator Model\", \"tag\":\"0102\",\"mdTag\":\"HARFS111700153010\","
				+ "\"occassion\":\"Duress\",\"brand\":\"Kenmore\",\"subBusinessUnit\":\"Sears Refrigerator\","
				+ "\"businessUnit\":\"Home Appliance\",\"scoreDate\":\"2015-08-24\",\"score\":0.075496672 ,"
				+ "\"percentile\":100, \"rank\":1},"
				+ "{\"modelId\":\"61\", \"modelName\":\"S_SCR_TV\", \"format\":\"Sears\", "
				+ "\"category\":\"TV Model\", \"tag\":\"0000\",\"mdTag\":\"CETVS623600153010\","
				+ "\"occassion\":\"Replace_by_age\",\"subBusinessUnit\":\"Sears Television\","
				+ "\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-24\",\"score\":0.0021435 ,"
				+ "\"percentile\":94, \"rank\":2},"
				+ "{\"modelId\":\"28\", \"modelName\":\"S_SCR_CE_CAMERA\", \"format\":\"Sears\","
				+ "\"category\":\"Consumer Electronics - camera\", \"tag\":\"0000\",\"mdTag\":\"CECAS623600153010\","
				+ "\"occassion\":\"Replace_by_age\",\"subBusinessUnit\":\"Sears Camera\","
				+ "\"businessUnit\":\"Electronics\",\"scoreDate\":\"2015-08-24\",\"score\":0.0012705932 ,"
				+ "\"percentile\":100, \"rank\":3},"
				+ "{\"modelId\":\"49\", \"modelName\":\"S_SCR_LG_PATIO\", \"format\":\"Sears\", "
				+ "\"category\":\"Patio Furniture Model\", \"tag\":\"0160\",\"mdTag\":\"ODPFS823600153010\","
				+ "\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Patio Furniture\","
				+ "\"businessUnit\":\"outdoor Living\",\"scoreDate\":\"2015-08-24\",\"score\":0.0053853 ,"
				+ "\"percentile\":100, \"rank\":7},"
				+ "{\"modelId\":\"30\", \"modelName\":\"S_SCR_FIT_EQUIP\", \"format\":\"Sears\", "
				+ "\"category\":\"Fitness Equipment Model\", \"tag\":\"0138\","
				+ "\"businessUnit\":\"Sporting Goods\",\"scoreDate\":\"2015-08-24\",\"mdTag\":\"SPFTS823600153010\","
				+ "\"occassion\":\"Top 5% of MSM\",\"subBusinessUnit\":\"Sears Fitness\",\"score\":0.0046268 ,"
				+ "\"percentile\":100, \"rank\":8},"
				+ "{\"modelId\":\"16\", \"modelName\":\"K_SCR_ODL\", \"format\":\"Kmart\", "
				+ "\"category\":\"Outdoor Living\", \"tag\":\"2134\",\"score\":0.0008136887 ,"
				+ "\"percentile\":68, \"rank\":66},"
				+ "{\"modelId\":\"14\", \"modelName\":\"K_SCR_LG\", \"format\":\"Kmart\", "
				+ "\"category\":\"Lawn & Garden\", \"tag\":\"2133\",\"score\":0.0004264773 ,"
				+ "\"percentile\":54, \"rank\":67},"
				+ "{\"tag\":\"0177\", \"index\":0.5334645, \"rank\":68},"
				+ "{\"tag\":\"0089\", \"index\":1.1149705, \"rank\":69}]}";		
		memberInfo = new MemberInfo("Oj8kOFFTCkcrljYSO/srjUeGk3A=","N","AZ","N","N","N");
		when(memberInfoDao.getMemberInfo(anyString())).thenReturn(memberInfo);
		when(occasionDao.getOccasionsInfo()).thenReturn(occasionInfos);
		when(occasionDao.getOccasionInfo("Duress")).thenReturn(this.getOccasionInfo("Duress"));
		when(occasionDao.getOccasionInfo("Replacement")).thenReturn(this.getOccasionInfo("Replacement"));
		when(occasionDao.getOccasionInfo("Browse")).thenReturn(this.getOccasionInfo("Browse"));
		when(occasionDao.getOccasionInfo("Pre-move")).thenReturn(this.getOccasionInfo("Pre-move"));
		when(occasionDao.getOccasionInfo("Post-move")).thenReturn(this.getOccasionInfo("Post-move"));
		when(occasionDao.getOccasionInfo("Remodel")).thenReturn(this.getOccasionInfo("Remodel"));
		when(occasionDao.getOccasionInfo("Replace_by_age")).thenReturn(this.getOccasionInfo("Replace_by_age"));
		when(occasionDao.getOccasionInfo("Top 5% of MSM")).thenReturn(this.getOccasionInfo("Top 5% of MSM"));
		
		
		List<EmailPackage> validemails = new ArrayList<EmailPackage>();
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HARFS111700153010","Home Appliance","Sears Refrigerator","Duress","HARFS",100.0,"Y","", 1, 8,30);		
		emailPackage = new EmailPackage("7081057547176153",tagMetadata);
		emailPackage.setSendDate(new DateTime().toDate());
		validemails.add(emailPackage);
		tagMetadata = new TagMetadata("CETVS623600153010","Electronics","Sears Television","Replace_by_age","CETVS",94.0,"Y","", 7, 3,30);		
		emailPackage = new EmailPackage("7081057547176153",tagMetadata);
		emailPackage.setSendDate(new DateTime().plusDays(8).toDate());
		validemails.add(emailPackage);
		tagMetadata = new TagMetadata("CECAS623600153010","Electronics","Sears Camera","Replace_by_age","CECAS",100.0,"Y","", 7, 3,30);		
		emailPackage = new EmailPackage("7081057547176153",tagMetadata);
		emailPackage.setSendDate(new DateTime().plusDays(11).toDate());
		validemails.add(emailPackage);	
		
		OutboxDao spy = spy(new OutboxDao());
		doNothing().when(spy).deleteQueuedEmailPackages(anyString());
		EmailPackage inProgressEmailPackage = null;
		List<EmailPackage> queuedEmailPackages = new ArrayList<EmailPackage>();
		when(outboxDao.getInProgressPackage("7081057547176153", occasionInfos)).thenReturn(inProgressEmailPackage);
		when(spy.getQueuedEmailPackages("7081057547176153", occasionInfos)).thenReturn(queuedEmailPackages);
		List<EmailPackage> retEmailPackages = cpsFiler.prepareEmailPackages(resp, "7081057547176153","Oj8kOFFTCkcrljYSO/srjUeGk3A=");
		
		Assert.assertNotNull(retEmailPackages);
		Assert.assertEquals(retEmailPackages.size(), 3);
		for(int i=0; i<retEmailPackages.size(); i++){
			assertEquals(sdformat.format(retEmailPackages.get(i).getSendDate()), sdformat.format(validemails.get(i).getSendDate()));				
		}
		
	}
	
	private OccasionInfo getOccasionInfo(String occ){
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getOccasion().equalsIgnoreCase(occ)){
					return occasionInfo;
				}
			}
		}
		return null;
	}
	
	public OccasionInfo getOccasionInfoFromPriority(String priority){
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getPriority().equalsIgnoreCase(priority)){
					return occasionInfo;
				}
			}
		}
		return null;
	} 
	
	
	//TEST - Incoming Occasions - {Duress, Sears camera top 5%, Sears Electronics top 5%}
	//Criteria: Duress is in progress already
	//Expected - only Duress should be returned.
	@Test
	public void testFilterTop5PercentOccasionsWhenInProgressIsActive(){
		sdformat = new SimpleDateFormat("MM/dd/yyyy");
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Duress");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("CECAS823600153010","Electronics","Sears Camera","Top 5% of MSM","CECAS",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Unknown");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("CEGES823600153010","Electronics","Sears Electronics","Top 5% of MSM","CEGES",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Top 5% of MSM");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		
		TagMetadata tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);
		inProgressEmailPackage.setCustEventNm("RTS_Duress");
		inProgressEmailPackage.setMemberInfo(memberInfo);
		inProgressEmailPackage.setSendDate(new DateTime().minusDays(3).toDate());
		 
		List<EmailPackage> retEmailPackages = cpsFiler.filterTop5PercentOccasions(emailPackages, inProgressEmailPackage);
		Assert.assertNotNull(retEmailPackages);
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0).getMdTagMetaData().getMdTag(), inProgressEmailPackage.getMdTagMetaData().getMdTag());
		//assertEquals(sdformat.format(retEmailPackages.get(0).getSendDate()), sdformat.format(inProgressEmailPackage.getSendDate()));
		
	}
	
	//TEST - Incoming Occasions - {Sears camera top 5%, Sears Electronics top 5%}
	//Criteria: Duress is in progress already but is not active anymore
	//Expected - Sears camera top 5% should be the only one returned.
	@Test
	public void testFilterTop5PercentOccasionsWhenInProgressIsNotActive(){
		emailPackages = new ArrayList<EmailPackage>();
		tagMetadata = new TagMetadata("CECAS823600153010","Electronics","Sears Camera","Top 5% of MSM","CECAS",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Unknown");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		EmailPackage temp = emailPackage;
		
		tagMetadata = new TagMetadata("CEGES823600153010","Electronics","Sears Electronics","Top 5% of MSM","CEGES",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Top 5% of MSM");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		inProgressEmailPackage = new EmailPackage("7081103948483127",tagMetadata);
		inProgressEmailPackage.setCustEventNm("RTS_Duress");
		inProgressEmailPackage.setMemberInfo(memberInfo);	
		
		List<EmailPackage> retEmailPackages = cpsFiler.filterTop5PercentOccasions(emailPackages, inProgressEmailPackage);
		Assert.assertNotNull(retEmailPackages);
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0), temp);
		
	}

	//TEST - Incoming Occasions - {Duress,Sears camera top 5%, Sears Electronics top 5%}
	//Criteria: No occasion is in progress
	//Expected - Duress should be the only one returned with today's date as the send date.
	@Test
	public void testFilterTop5PercentOccasionsWhenNoOccasionInProgress(){
		
		List<EmailPackage>emailPackages = new ArrayList<EmailPackage>();
		TagMetadata tagMetadata = new TagMetadata("HADHS123600153010","Home Appliance","Sears Dishwasher","Duress","HADHS",91.0,"Y","", 1, 8, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		EmailPackage emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Duress");
		emailPackage.setMemberInfo(memberInfo);
		EmailPackage temp = emailPackage;
		emailPackages.add(emailPackage);
				
		tagMetadata = new TagMetadata("CECAS823600153010","Electronics","Sears Camera","Top 5% of MSM","CECAS",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Unknown");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		tagMetadata = new TagMetadata("CEGES823600153010","Electronics","Sears Electronics","Top 5% of MSM","CEGES",91.0,"Y","", 8, 3, 30);
		memberInfo = new MemberInfo("hiBSAglnyr3kI6kYrBXHmMy5WPE=","N","AZ","N","N","N");
		emailPackage = new EmailPackage("7081103948483127",tagMetadata);
		emailPackage.setCustEventNm("RTS_Top 5% of MSM");
		emailPackage.setMemberInfo(memberInfo);		
		emailPackages.add(emailPackage);
		
		List<EmailPackage> retEmailPackages = cpsFiler.filterTop5PercentOccasions(emailPackages, null);
		Assert.assertNotNull(retEmailPackages);
		Assert.assertEquals(retEmailPackages.size(), 1);
		assertEquals(retEmailPackages.get(0), temp);
		
	}

	
}
