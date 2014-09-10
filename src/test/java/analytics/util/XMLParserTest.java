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
        ProcessTransaction processTransaction = XMLParser.parseXMLProcessTransaction(oneLineItem);

        assertEquals(processTransaction.getMemberNumber(), "7081390000061954");
        assertTrue(processTransaction.getLineItemList()!=null);
        assertEquals(processTransaction.getLineItemList().size(), 1);
        assertEquals(processTransaction.getLineItemList().get(0).getItemNumber(), "082453511");
    }






    static String oneLineItem="<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
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
            "            </LineItems>\n" +
            "        </ProcessTransaction>\n" +
            "    </soapenv:Body>\n" +
            "</soapenv:Envelope>\n";



}
