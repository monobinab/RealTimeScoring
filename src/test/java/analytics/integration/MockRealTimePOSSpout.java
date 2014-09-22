package analytics.integration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.mockito.Mockito;

import com.ibm.jms.JMSMessage;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MockRealTimePOSSpout extends BaseRichSpout{
	SpoutOutputCollector collector;
	List<Message> testData;
	//Tuple tuple = mock(Tuple.class);
	/*
	 * 
	 */
    

    private void populateTestData() throws JMSException{
    	String text = "{\"1-Transaction Transfer Info Segment-F2\":{\"segmentType\":\"F2\",\"segmentDescription\":\"Transaction Transfer Info Segment\""
    			+ ",\"Indicator\":\"F2\",\"Segment length\":\"0037\",\"Selling Store Number\":\"09300\",\"Register Number\":\"118\",\"Transaction Number\":"
    			+ "\"6970\",\"Transaction Date\":\"2014-09-19\",\"Transaction Time\":\"10:27:10\",\"Receiving Store Number\":\"01264\",\"Selling Store Type\":"
    			+ "\"E\",\"Receiving Store Type\":\"1\",\"Pend Flag\":\"N\",\"Transaction Service Type\":\"R\",\"IP Time Id\":\"200001111901\"},"
    			+ "\"2-Transaction Header Record Segment-B1\":{\"segmentType\":\"B1\",\"segmentDescription\":\"Transaction Header Record Segment\","
    			+ "\"Indicator\":\"B1\",\"Segment Length\":\"0066\",\"Version Number\":\"\u0004\u0000\",\"Store Number\":\"09300\","
    			+ "\"Register Number\":\"118\",\"Transaction Number\":\"6970\",\"Transaction Date\":\"2014-09-19\",\"Transaction Time\":"
    			+ "\"10:27:10\",\"Credited Store Number\":\"01264\",\"Ringing Associate Number\":\"000075\",\"Purchasing Customer ID\":\"00000000000\","
    			+ "\"Purchasing Address ID\":\"00000000000\",\"Customer ID Status Code\":\"2\",\"Transaction Source Code\":\"5\",\"Transaction Type Code\":"
    			+ "\"1\",\"Transaction Status Code\":\"2\",\"Transaction Error Code\":\"00\",\"Transaction Reason Code\":\"E\",\"Transaction Total Discount\":"
    			+ "\"0000000 \",\"Transaction Total Tax\":\"0001046 \",\"Transaction Tax Code\":\"6\",\"Transaction Total\":\"0020520 \",\"Transaction Flags\":"
    			+ "\"\u0000\u0000\"},\"3-Transaction Comment Identifier Segment-BA\":{\"segmentType\":\"BA\",\"segmentDescription\":"
    			+ "\"Transaction Comment Identifier Segment\",\"Indicator\":\"BA\",\"Segment length\":\"0008\",\"Comment Type Code\":\"48\","
    			+ "\"Comment sequence number\":\"01\",\"Comment action code\":\" \"},\"4-Transaction Comment Segment - (Type 48)Transfer To Store Type-B2\""
    			+ ":{\"segmentType\":\"B2\",\"segmentDescription\":\"Transaction Comment Segment - (Type 48)Transfer To Store Type\","
    			+ "\"Indicator\":\"B2\",\"Segment Length\":\"0007\",\"Length of Comment\":\"02\",\"Comment Text      Selling Store Type\":"
    			+ "\"E\",\"Receiving Store  Type\":\"1\"},\"5-Transaction Comment Identifier Segment-BA\":{\"segmentType\":\"BA\","
    			+ "\"segmentDescription\":\"Transaction Comment Identifier Segment\",\"Indicator\":\"BA\",\"Segment length\":\"0008\","
    			+ "\"Comment Type Code\":\"08\",\"Comment sequence number\":\"01\",\"Comment action code\":\" \"},\"6-Transaction Comment Segment -"
    			+ " (Type 8) Craftsman Club, Pulse Club, or Sears Your Way Rewards Number-B2\":{\"segmentType\":\"B2\",\"segmentDescription\":"
    			+ "\"Transaction Comment Segment - (Type 8) Craftsman Club, Pulse Club, or Sears Your Way Rewards Number\",\"Indicator\":\"B2\","
    			+ "\"Segment Length\":\"001D\",\"Length of Comment\":\"24\",\"Comment Text    Craftsman Club Number or Sears Your Way Rewards\":"
    			+ "\"7081016256288583\"},\"7-DOTCOM Transaction Reference-F9\":{\"segmentType\":\"F9\",\"segmentDescription\":\"DOTCOM Transaction Reference\","
    			+ "\"Indicator\":\"F9\",\"Segment Length\":\"001E\",\"DOTCOM Order Number\":\"012324958900\"},\"8-Line Item Format-C1\""
   				+ ":{\"segmentType\":\"C1\",\"segmentDescription\":\"Line Item Format\",\"Indicator\":\"C1\",\"Segment Length\":\"0054\","
   				+ "\"Selling Associate\":\"000075\",\"Division Number\":\"071\",\"Item Number\":\"77380\",\"SKU Number\":\"000\",\"Miscellaneous"
   				+ " Account Number\":\"000000\",\"Quantity\":\"0001\",\"Selling Amount\":\"0020499 \",\"Item Flags\":\" "
   				+ "\u0010\u0000\u0000\u0000\u0000\",\"Line Item Type Code 1\":\"1\",\"Line Item Type Code 2\":\"1\",\"Line Item Reason Code\":\"00\","
   				+ "\"Line Item Status Code\":\"6\",\"Line Item Status Reason Code\":\"1\",\"Promised Date\":\"2014-09-24\",\"Regular Price\":\"0020520\","
   				+ "\"PLU Amount\":\"0020499\",\"PLU Amount Type Code\":\"2\",\"Tax Amount\":\"0001046 \",\"Tax Code\":\"1\"},\"9-Transaction Comment Identifier "
   				+ "Segment-BA\":{\"segmentType\":\"BA\",\"segmentDescription\":\"Transaction Comment Identifier Segment\",\"Indicator\":\"BA\",\"Segment"
 				+ " length\":\"0008\",\"Comment Type Code\":\"AI\",\"Comment sequence number\":\"01\",\"Comment action code\":\" \"},"
 				+ "\"10-Transaction Comment Segment - (Type AI) Best Seller Item Description-B2\":{\"segmentType\":\"B2\",\"segmentDescription\":"
 				+ "\"Transaction Comment Segment - (Type AI) Best Seller Item Description\",\"Indicator\":\"B2\",\"Segment length\":\"004B\","
 				+ "\"Length of Comment\":\"70\",\"Item Description\":\"29cc Wheeled Edger w/ Speed Start\u0026trade;                              \"},"
 				+ "\"11-Line Item Status Format-C2\":{\"segmentType\":\"C2\",\"segmentDescription\":\"Line Item Status Format\",\"Indicator\":\"C2\","
 				+ "\"Segment Length\":\"0013\",\"Quantity\":\"0001\",\"Status Type Code\":\"4\",\"Line Item Effective Date\":\"2014-09-19\","
 				+ "\"Line Item Bin Location\":\" \"},\"12-Line Item Status Format-C2\":{\"segmentType\":\"C2\",\"segmentDescription\":\"Line Item Status Format\","
 				+ "\"Indicator\":\"C2\",\"Segment Length\":\"0013\",\"Quantity\":\"0001\",\"Status Type Code\":\"5\",\"Line Item Effective Date\":\"2014-09-19\","
 				+ "\"Line Item Bin Location\":\"0\"},\"13-Line Item Status Format-C2\":{\"segmentType\":\"C2\",\"segmentDescription\":\"Line Item Status Format\","
 				+ "\"Indicator\":\"C2\",\"Segment Length\":\"0013\",\"Quantity\":\"0001\",\"Status Type Code\":\"6\",\"Line Item Effective Date\":\"2014-09-19\","
 				+ "\"Line Item Bin Location\":\" \"},\"14-Line Item Markdown Format-C3\":{\"segmentType\":\"C3\",\"segmentDescription\":\"Line Item Markdown Format\","
				+ "\"Indicator\":\"C3\",\"Segment Length\":\"0017\",\"Markdown Code\":\"4\",\"Markdown Amount\":\"0001025-\",\"Coupon Number\":\"        \","
				+ "\"Markdown Code Tax Status\":\"1\",\"Markdown Flags\":\"  \"},\"15-Line Item Markdown Format-C3\":{\"segmentType\":\"C3\",\"segmentDescription\""
				+ ":\"Line Item Markdown Format\",\"Indicator\":\"C3\",\"Segment Length\":\"0017\",\"Markdown Code\":\"4\",\"Markdown Amount\":\"0002044-\","
				+ "\"Coupon Number\":\"32      \",\"Markdown Code Tax Status\":\"1\",\"Markdown Flags\":\"  \"},\"16-Line Item Delivery Format-C4\":"
				+ "{\"segmentType\":\"C4\",\"segmentDescription\":\"Line Item Delivery Format\",\"Indicator\":\"C4\",\"Segment Length\":\"0048\","
				+ "\"Delivery Type\":\"A\",\"Delivery Route Number\":\"0000\",\"Delivery Stop Number\":\"00\",\"Delivery Handling Code\":\" \","
				+ "\"Delivery Customer ID\":\"00000000000\",\"Delivery Address ID\":\"00000000000\",\"Customer ID Status Code\":\"2\",\"Delivery "
				+ "Window Text\":\"     \",\"Delivery Confirmation Code\":\" \",\"Delivery Scheduled Date\":\"2014-09-24\",\"Delivery Zip Code\":"
				+ "\"00000\",\"Delivery Ship Store Number\":\"01264\",\"Delivery DOS Order Number\":\"00000000\",\"Delivery Warehouse Number\":"
				+ "\"0000\"},\"17-Line Item Format-C1\":{\"segmentType\":\"C1\",\"segmentDescription\":\"Line Item Format\",\"Indicator\":\"C1\","
				+ "\"Segment Length\":\"0054\",\"Selling Associate\":\"000075\",\"Division Number\":\"000\",\"Item Number\":\"00000\","
				+ "\"SKU Number\":\"000\",\"Miscellaneous Account Number\":\"116044\",\"Quantity\":\"0001\",\"Selling Amount\":\"0001908 "
				+ "\",\"Item Flags\":\"\u0000\u0000\u0000\u0000\u0000\u0000\",\"Line Item Type Code 1\":\"2\",\"Line Item Type Code 2\":\"1\","
				+ "\"Line Item Reason Code\":\"00\",\"Line Item Status Code\":\"1\",\"Line Item Status Reason Code\":\" \",\"Promised Date\":"
				+ "\"2014-09-19\",\"Regular Price\":\"0001908\",\"PLU Amount\":\"0001908\",\"PLU Amount Type Code\":\"1\",\"Tax Amount\":\"0000000 \","
				+ "\"Tax Code\":\"1\"},\"18-Payment Record-B5\":{\"segmentType\":\"B5\",\"segmentDescription\":\"Payment Record\",\"Indicator\":"
				+ "\"B5\",\"Segment Length\":\"0032\",\"Payment Method Type Code\":\"O\",\"Payment Method Amount\":\"0002044 \",\"Payment Method "
				+ "Account Number\":\"708101******8583\",\"Payment Method Expiration Date\":\"0914\",\"Payment Status Code\":\"2\",\"Payment Method"
				+ " Authorization Code\":\"D433XX\",\"Payment Method Date Code\":\"4\",\"Payment Method Date\":\"2014-09-19\"},"
				+ "\"19-Payment Record-B5\":{\"segmentType\":\"B5\",\"segmentDescription\":\"Payment Record\",\"Indicator\":\"B5\","
				+ "\"Segment Length\":\"0032\",\"Payment Method Type Code\":\"8\",\"Payment Method Amount\":\"0018476 \",\"Payment Method Account "
				+ "Number\":\"414720******3850\",\"Payment Method Expiration Date\":\"0516\",\"Payment Status Code\":\"2\",\"Payment Method "
				+ "Authorization Code\":\"00794C\",\"Payment Method Date Code\":\" \",\"Payment Method Date\":\"          \"},\"20-Out of Area,"
				+ " Out of State Tax segment - (Release 37)-E4\":{\"segmentType\":\"E4\",\"segmentDescription\":\"Out of Area, Out of State Tax segment -"
				+ " (Release 37)\",\"Indicator\":\"E4\",\"Segment length\":\"0021\",\"Delivery Tax Code\":\"1\",\"Tax Rate\":\"06000\",\"Mainframe Availability"
				+ " Indicator\":\"32\",\"AVP Taxing Jurisdiction Geo Code\":\"00\",\"AVP Taxing Jurisdiction County Number\":\"000\","
				+ "\"Transfer to State Code\":\"PA\",\"Ship-to Taxware GEO (9 digit State/ZIP/GEO)\":\"         \",\"Jurisdiction-County "
				+ "(origin or destination)\":\"O\",\"Jurisdiction-County-Special (origin or destination)\":\"X\",\"Jurisdiction-City/Local "
				+ "(origin or destination)\":\"O\",\"Jurisdiction-City/Local Special (origin or destination)\":\"X\",\"Jurisdiction-Transit "
				+ "(origin or destination)\":\"D\",\"Jurisdiction-Special/SPD (origin or destination)\":\" \",\"Jurisdiction-Other (origin or destination)\":"
				+ "\" \"},\"21-Customer Information Format-BF\":{\"segmentType\":\"BF\",\"segmentDescription\":\"Customer Information Format\",\"Indicator\":"
				+ "\"BF\",\"Segment length\":\"00E0\",\"Customer ID Number\":\"00000000000\",\"Address ID Number\":\"00000000000\",\"Customer ID "
				+ "Status Code\":\"2\",\"Customer Type Code\":\"2\",\"Customer Code\":\" \",\"Customer First Name\":\"TAMMY      \",\"Customer Middle"
				+ " Name\":\"           \",\"Customer Last Name\":\"                         \",\"Customer Phone\":\"***2692\",\"Customer Area Code\":\"407\","
				+ "\"Street Address 1\":\"2227 LOCUST RD                          \",\"Street Address 2\":\"                         \",\"City\":\"MORTON    "
				+ "               \",\"State\":\"PA\",\"Zip Code\":\"19070    \",\"Customer Inquiry Code\":\"1\",\"Customer Driver\u0027s License No.\":"
				+ "\"*************************\",\"Customer D.L.State Code\":\"  \",\"Customer Alternate Phone\":\"***    \",\"Customer Alternate Area Code\":"
				+ "\"   \"},\"22-Customer Information Format-BF\":{\"segmentType\":\"BF\",\"segmentDescription\":\"Customer Information Format\",\"Indicator\":"
				+ "\"BF\",\"Segment length\":\"00E0\",\"Customer ID Number\":\"00000000000\",\"Address ID Number\":\"00000000000\",\"Customer ID Status Code\":"
				+ "\"2\",\"Customer Type Code\":\"1\",\"Customer Code\":\" \",\"Customer First Name\":\"TAMMY      \",\"Customer Middle Name\":\"           \","
				+ "\"Customer Last Name\":\"ANDERSON                 \",\"Customer Phone\":\"***2692\",\"Customer Area Code\":\"407\",\"Street Address 1\":"
				+ "\"2227 LOCUST RD                          \",\"Street Address 2\":\"                         \",\"City\":\"MORTON                   \","
				+ "\"State\":\"PA\",\"Zip Code\":\"19070    \",\"Customer Inquiry Code\":\"1\",\"Customer Driver\u0027s License No.\":\"*************************\","
				+ "\"Customer D.L.State Code\":\"  \",\"Customer Alternate Phone\":\"***2692\",\"Customer Alternate Area Code\":\"407\"}}";
    	JMSMessage mockMessage = mock(JMSMessage.class, Mockito.withSettings().extraInterfaces(TextMessage.class));
    	when(((TextMessage) mockMessage).getText()).thenReturn(text);
    	testData = new ArrayList<Message>();
    	testData.add(mockMessage);
    	
    }

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		try{
			populateTestData();
		}
		catch (JMSException e){
			System.out.println("Fail....");
		}
	}

	@Override
	public void nextTuple() {
		if(!testData.isEmpty()){
			Message myMessage = testData.get(0);
			collector.emit(new Values(myMessage), myMessage);
			testData.remove(0);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("npos"));
		
	}

}
