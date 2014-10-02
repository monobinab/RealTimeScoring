package analytics.util;

import analytics.util.objects.ProcessTransaction;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by syermalk on 9/10/14.
 */
public class XMLParserTest {

    @Test
    public void testParsingWithOneLineItem()
    {
    	// String fileName = "./src/main/resources/POS.xml";
   	 	String fileName = oneLineItem;
        ProcessTransaction processTransaction = XMLParser.parseXMLProcessTransaction(fileName);

        assertTrue(processTransaction != null);
        assertEquals("7081253419145216",processTransaction.getMemberNumber());
        assertTrue(processTransaction.getLineItemList()!=null);
        assertEquals( 1,processTransaction.getLineItemList().size());
        assertEquals( "000084902",processTransaction.getLineItemList().get(0).getItemNumber());
    }
    

    static String oneLineItem=//"<request>\n" +
            "  <tns:ProcessTransaction>\n" +
            "    <tns:MessageVersion>09</tns:MessageVersion>\n" +
            "    <tns:MemberNumber>7081253419145216</tns:MemberNumber>\n" +
            "    <tns:RequestorID>NPOS</tns:RequestorID>\n" +
            "    <tns:OrderStoreNumber>01060</tns:OrderStoreNumber>\n" +
            "    <tns:PickUpStoreNumber>01060</tns:PickUpStoreNumber>\n" +
            "    <tns:TenderStoreNumber>01060</tns:TenderStoreNumber>\n" +
            "    <tns:RegisterNumber>582</tns:RegisterNumber>\n" +
            "    <tns:TransactionNumber>5214</tns:TransactionNumber>\n" +
            "    <tns:TransactionTotal>148.53</tns:TransactionTotal>\n" +
            "    <tns:TransactionTotalTax>8.91</tns:TransactionTotalTax>\n" +
            "    <tns:TransactionDate>2014-09-11</tns:TransactionDate>\n" +
            "    <tns:TransactionTime>11:13:17</tns:TransactionTime>\n" +
            "    <tns:AssociateID>001016</tns:AssociateID>\n" +
            "    <tns:EarnFlag>E</tns:EarnFlag>\n" +
            "    <tns:PointDebit>000000000</tns:PointDebit>\n" +
            "    <tns:CurrencyDebit>000000000</tns:CurrencyDebit>\n" +
            "    <tns:TenderList>\n" +
            "      <tns:Tender>\n" +
            "        <tns:TenderType>G</tns:TenderType>\n" +
            "        <tns:TenderAmount>1.64</tns:TenderAmount>\n" +
            "      </tns:Tender>\n" +
            "      <tns:Tender>\n" +
            "        <tns:TenderType>8</tns:TenderType>\n" +
            "        <tns:TenderAmount>155.8</tns:TenderAmount>\n" +
            "      </tns:Tender>\n" +
            "      <tns:Tender>\n" +
            "        <tns:TenderType>SV</tns:TenderType>\n" +
            "        <tns:TenderAmount>8.22</tns:TenderAmount>\n" +
            "        <tns:AuthorizationCode>6AQ2Y8S9</tns:AuthorizationCode>\n" +
            "      </tns:Tender>\n" +
            "    </tns:TenderList>\n" +
            "    <tns:LineItems>\n" +
            "      <tns:LineItem>\n" +
            "        <tns:LineNumber>0001</tns:LineNumber>\n" +
            "        <tns:ItemType>1</tns:ItemType>\n" +
            "        <tns:Division>067</tns:Division>\n" +
            "        <tns:ItemNumber>000084902</tns:ItemNumber>\n" +
            "        <tns:SKU>000</tns:SKU>\n" +
            "        <tns:UPC>0098775457846</tns:UPC>\n" +
            "        <tns:LineItemAmountTypeCode>2</tns:LineItemAmountTypeCode>\n" +
            "        <tns:DollarValuePreDisc>66</tns:DollarValuePreDisc>\n" +
            "        <tns:DollarValuePostDisc>59.41</tns:DollarValuePostDisc>\n" +
            "        <tns:PriceMatchAmount>0</tns:PriceMatchAmount>\n" +
            "        <tns:PriceMatchBonusAmount>0</tns:PriceMatchBonusAmount>\n" +
            "        <tns:PostSalesAdjustmentAmount>0</tns:PostSalesAdjustmentAmount>\n" +
            "        <tns:Quantity>0001</tns:Quantity>\n" +
            "        <tns:OriginalLineNumber>0000</tns:OriginalLineNumber>\n" +
            "        <tns:OriginalStoreNumber>00000</tns:OriginalStoreNumber>\n" +
            "        <tns:OriginalRegisterNumber>000</tns:OriginalRegisterNumber>\n" +
            "        <tns:OriginalTransactionNumber>0000</tns:OriginalTransactionNumber>\n" +
            "        <tns:OriginalTransactionTime>00000000</tns:OriginalTransactionTime>\n" +
            "        <tns:PointsRedeemed>000003290</tns:PointsRedeemed>\n" +
            "        <tns:DollarValueOfPointsRedeemed>3.29</tns:DollarValueOfPointsRedeemed>\n" +
            "        <tns:RedemptionExclusionFlag>N</tns:RedemptionExclusionFlag>\n" +
            "        <tns:TaxAmount>3.56</tns:TaxAmount>\n" +
            "        <tns:NonMemberPrice>110</tns:NonMemberPrice>\n" +
            "        <tns:Coupons>\n" +
            "          <tns:Coupon>\n" +
            "            <tns:CouponNumber>32</tns:CouponNumber>\n" +
            "            <tns:DiscountAmount>-3.29</tns:DiscountAmount>\n" +
            "            <tns:CouponType>2</tns:CouponType>\n" +
            "            <tns:AlternateCouponDecision>D</tns:AlternateCouponDecision>\n" +
            "          </tns:Coupon>\n" +
            "          <tns:Coupon>\n" +
            "            <tns:CouponNumber>29683</tns:CouponNumber>\n" +
            "            <tns:DiscountAmount>-3.3</tns:DiscountAmount>\n" +
            "            <tns:CouponType>2</tns:CouponType>\n" +
            "            <tns:AlternateCouponDecision>D</tns:AlternateCouponDecision>\n" +
            "          </tns:Coupon>\n" +
            "        </tns:Coupons>\n" +
            "      </tns:LineItem>\n" +

            "    </tns:LineItems>\n" +
            "  </tns:ProcessTransaction>\n" ;
           // "</request>";

    @Test
    public void testParsingWithTwoLineItem()
    {
    	// 
    	 String fileName = twoLineItem;
        ProcessTransaction processTransaction = XMLParser.parseXMLProcessTransaction(fileName);
        assertTrue(processTransaction != null);
        assertEquals("7081390000061954",processTransaction.getMemberNumber());
        assertTrue(processTransaction.getLineItemList()!=null);
        assertEquals( 2,processTransaction.getLineItemList().size());
        assertEquals( "082453511",processTransaction.getLineItemList().get(0).getItemNumber());
        assertEquals( "082453512",processTransaction.getLineItemList().get(1).getItemNumber());
       
    }

    static String twoLineItem="<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
            "    <soapenv:Body>\n" +
            "        <ProcessTransaction xmlns=\"http://www.epsilon.com/webservices/\">\n" +
            "            <MessageVersion>08</MessageVersion>\n" +
            "            <MemberNumber>7081390000061954</MemberNumber>\n" +
            "            <RequestorID>KCOM</RequestorID>\n" +
            "            <OrderStoreNumber>07840</OrderStoreNumber>\n" +
            "            <TenderStoreNumber>00000</TenderStoreNumber>\n" +
            "            <RegisterNumber>048</RegisterNumber>\n" +
            "            <TransactionNumber>0000</TransactionNumber>\n" +
            "            <TransactionTotal>3.67000</TransactionTotal>\n" +
            "            <TransactionTotalTax>0.00000</TransactionTotalTax>\n" +
            "            <TransactionDate>2014-08-07</TransactionDate>\n" +
            "            <TransactionTime>21:43:02</TransactionTime>\n" +
            "            <EarnFlag>T</EarnFlag>\n" +
            "            <TimeZone>CDT</TimeZone>\n" +
            "            <LineItems>\n" +
            "                <LineItem>\n" +
            "                    <LineNumber>1</LineNumber>\n" +
            "                    <ItemType>1</ItemType>\n" +
            "                    <ItemNumber>082453511</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "					 <LineItem>\n" +
            "                    <LineNumber>2</LineNumber>\n" +
            "                    <ItemType>2</ItemType>\n" +
            "                    <ItemNumber>082453512</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "            </LineItems>\n" +
            "        </ProcessTransaction>\n" +
            "    </soapenv:Body>\n" +
            "</soapenv:Envelope>\n";

    @Test
    public void testParsingWithNoMemberId()
    {
    	// 
    	 String fileName = noMemberId;
        ProcessTransaction processTransaction = XMLParser.parseXMLProcessTransaction(fileName);
        assertTrue(processTransaction != null);
        System.out.println(processTransaction.getMemberNumber());
        assertEquals(null,processTransaction.getMemberNumber());
        assertEquals("KCOM",processTransaction.getRequestorID());
        assertTrue(processTransaction.getLineItemList()!=null);
        assertEquals( 2,processTransaction.getLineItemList().size());
        assertEquals( "082453511",processTransaction.getLineItemList().get(0).getItemNumber());
        assertEquals( "082453512",processTransaction.getLineItemList().get(1).getItemNumber());
       
    }

    static String noMemberId="<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
            "    <soapenv:Body>\n" +
            "        <ProcessTransaction xmlns=\"http://www.epsilon.com/webservices/\">\n" +
            "            <MessageVersion>08</MessageVersion>\n" +
            "            <MemberNumber/>\n" +
            "            <RequestorID>KCOM</RequestorID>\n" +
            "            <OrderStoreNumber>07840</OrderStoreNumber>\n" +
            "            <TenderStoreNumber>00000</TenderStoreNumber>\n" +
            "            <RegisterNumber>048</RegisterNumber>\n" +
            "            <TransactionNumber>0000</TransactionNumber>\n" +
            "            <TransactionTotal>3.67000</TransactionTotal>\n" +
            "            <TransactionTotalTax>0.00000</TransactionTotalTax>\n" +
            "            <TransactionDate>2014-08-07</TransactionDate>\n" +
            "            <TransactionTime>21:43:02</TransactionTime>\n" +
            "            <EarnFlag>T</EarnFlag>\n" +
            "            <TimeZone>CDT</TimeZone>\n" +
            "            <LineItems>\n" +
            "                <LineItem>\n" +
            "                    <LineNumber>1</LineNumber>\n" +
            "                    <ItemType>1</ItemType>\n" +
            "                    <ItemNumber>082453511</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "					 <LineItem>\n" +
            "                    <LineNumber>2</LineNumber>\n" +
            "                    <ItemType>2</ItemType>\n" +
            "                    <ItemNumber>082453512</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "            </LineItems>\n" +
            "        </ProcessTransaction>\n" +
            "    </soapenv:Body>\n" +
            "</soapenv:Envelope>\n";
    
    @Test
    public void testParsingWithInvalidMemberId()
    {
    	// 
    	 String fileName = inValidMemberId;
        ProcessTransaction processTransaction = XMLParser.parseXMLProcessTransaction(fileName);
        assertTrue(processTransaction != null);
        assertEquals(null,processTransaction.getMemberNumber());
        assertEquals("KCOM",processTransaction.getRequestorID());
        assertTrue(processTransaction.getLineItemList()!=null);
        assertEquals( 2,processTransaction.getLineItemList().size());
        assertEquals( "082453511",processTransaction.getLineItemList().get(0).getItemNumber());
        assertEquals( "082453512",processTransaction.getLineItemList().get(1).getItemNumber());
       
    }

    static String inValidMemberId="<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
            "    <soapenv:Body>\n" +
            "        <ProcessTransaction xmlns=\"http://www.epsilon.com/webservices/\">\n" +
            "            <MessageVersion>08</MessageVersion>\n" +
            "            <MemberNumber>60909987676</MemberNumber>\n" +
            "            <RequestorID>KCOM</RequestorID>\n" +
            "            <OrderStoreNumber>07840</OrderStoreNumber>\n" +
            "            <TenderStoreNumber>00000</TenderStoreNumber>\n" +
            "            <RegisterNumber>048</RegisterNumber>\n" +
            "            <TransactionNumber>0000</TransactionNumber>\n" +
            "            <TransactionTotal>3.67000</TransactionTotal>\n" +
            "            <TransactionTotalTax>0.00000</TransactionTotalTax>\n" +
            "            <TransactionDate>2014-08-07</TransactionDate>\n" +
            "            <TransactionTime>21:43:02</TransactionTime>\n" +
            "            <EarnFlag>T</EarnFlag>\n" +
            "            <TimeZone>CDT</TimeZone>\n" +
            "            <LineItems>\n" +
            "                <LineItem>\n" +
            "                    <LineNumber>1</LineNumber>\n" +
            "                    <ItemType>1</ItemType>\n" +
            "                    <ItemNumber>082453511</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "					 <LineItem>\n" +
            "                    <LineNumber>2</LineNumber>\n" +
            "                    <ItemType>2</ItemType>\n" +
            "                    <ItemNumber>082453512</ItemNumber>\n" +
            "                    <SKU>000</SKU>\n" +
            "                    <UPC>07723100317</UPC>\n" +
            "                    <LineItemAmountTypeCode>1</LineItemAmountTypeCode>\n" +
            "                    <DollarValuePreDisc>3.67000</DollarValuePreDisc>\n" +
            "                    <DollarValuePostDisc>3.67000</DollarValuePostDisc>\n" +
            "                    <PriceMatchAmount>0</PriceMatchAmount>\n" +
            "                    <PriceMatchBonusAmount>0</PriceMatchBonusAmount>\n" +
            "                    <Quantity>1.0</Quantity>\n" +
            "                    <OriginalLineNumber>1</OriginalLineNumber>\n" +
            "                    <OriginalTransactionDate>21:43:02</OriginalTransactionDate>\n" +
            "                    <TaxAmount>0.00000</TaxAmount>\n" +
            "                    <PostSalesAdjustmentAmount>0</PostSalesAdjustmentAmount>\n" +
            "                    <Coupons/>\n" +
            "                </LineItem>\n" +
            "            </LineItems>\n" +
            "        </ProcessTransaction>\n" +
            "    </soapenv:Body>\n" +
            "</soapenv:Envelope>\n";

}
