/**
 * 
 */
package analytics.util;

import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ddas1
 * 
 */
public class XMLParser {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(XMLParser.class);

	public static ProcessTransaction parseXMLProcessTransaction(String fileName) {

		ProcessTransaction processTransaction = null;
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        xmlInputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
		List<LineItem> lineItemList = new ArrayList<LineItem>();
		LineItem lineItem = null;
		boolean bMemberNumber = false;
		boolean bLineItem = false;
		boolean bDivision = false;
		boolean bItemNumber = false;
		boolean bDollarValuePostDisc = false;
		boolean bRequestorID = false;
		boolean bItemType=false;
		boolean bEarnFlag=false;
		boolean btransactionFlag = false;
		boolean bOrderStoreFlag = false;
		boolean bpickUpStoreFlag = false;
		boolean btenderStoreFlag = false;
		boolean bRegisterFlag = false;
		boolean btransactionTimeFlag = false;
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new StringReader(fileName));
		
			// int event = xmlStreamReader.getEventType();
			processTransaction = new ProcessTransaction();
			while (xmlStreamReader.hasNext()) {
				
				int event = xmlStreamReader.getEventType();
				//event = 4;
                switch (event) {
									
				case XMLStreamConstants.START_ELEMENT:
                    String elementName = extractElementName(xmlStreamReader);


             //       QName qname = xmlStreamReader.getName();
					if (elementName.contains("MemberNumber")) {
						bMemberNumber = true;
					} else if ("RequestorID".equals(
							elementName)) {
						bRequestorID = true;
					} else if("EarnFlag".equals(elementName)){
						bEarnFlag = true;
					}
					else if("TransactionNumber".equals(elementName)){
						btransactionFlag = true;
					}
					else if("OrderStoreNumber".equals(elementName)){
						bOrderStoreFlag = true;
					}
					else if("PickUpStoreNumber".equals(elementName)){
						bpickUpStoreFlag = true;
					}
					else if("TenderStoreNumber".equals(elementName)){
						btenderStoreFlag = true;
					}
					else if("RegisterNumber".equals(elementName)){
						bRegisterFlag = true;
					}
					else if("TransactionTime".equals(elementName)){
						btransactionTimeFlag = true;
					}
					if ("LineItem".equals(elementName)) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if ("Division"
							.equals(elementName)) {
						bDivision = true;
					} else if ("ItemNumber".equals(
							elementName)) {
						bItemNumber = true;
					} else if("ItemType".equals(elementName)){
						bItemType = true;
					} else if ("DollarValuePostDisc".equals(
							elementName)) {
						bDollarValuePostDisc = true;
					} 
					break;
				case XMLStreamConstants.CHARACTERS:

					if (bMemberNumber) {
						String memberNumber = xmlStreamReader
								.getText();
						if(memberNumber.startsWith("7081") && memberNumber.length()==16)
							{
								processTransaction.setMemberNumber(xmlStreamReader
								.getText());
								}
						bMemberNumber = false;
					} else if (bRequestorID) {
						processTransaction.setRequestorID(xmlStreamReader
								.getText());
						LOGGER.debug("Requestor Id is..."
								+ processTransaction.getRequestorID());
						bRequestorID = false;
					}else if(btransactionFlag) {
						processTransaction.setTransactionNumber(xmlStreamReader
								.getText());
						btransactionFlag = false;
					}else if(btransactionTimeFlag) {
						processTransaction.setTransactionTime(xmlStreamReader
								.getText());
						btransactionTimeFlag = false;
					}
					else if(bOrderStoreFlag) {
						processTransaction.setOrderStoreNumber(xmlStreamReader
								.getText());
						bOrderStoreFlag = false;
					}
					else if(btenderStoreFlag) {
						processTransaction.setTenderStoreNumber(xmlStreamReader
								.getText());
						btenderStoreFlag = false;
					}
					else if(bpickUpStoreFlag) {
						processTransaction.setPickUpStoreNumber(xmlStreamReader
								.getText());
						bpickUpStoreFlag = false;
					}
					else if(bRegisterFlag) {
						processTransaction.setRegisterNumber(xmlStreamReader
								.getText());
						bRegisterFlag = false;
					}
					else if (bLineItem) {
						bLineItem = false;
					} else if (bDivision) {
						lineItem.setDivision(xmlStreamReader.getText());
						bDivision = false;
					} else if (bItemNumber) {
						lineItem.setItemNumber(xmlStreamReader.getText());
						bItemNumber = false;
					} else if (bItemType) {
						lineItem.setItemType(xmlStreamReader.getText());
						bItemType = false;
					} else if (bEarnFlag){
						processTransaction.setEarnFlag(xmlStreamReader.getText());
						bEarnFlag = false;
                    }
					else if (bDollarValuePostDisc) {
						lineItem.setDollarValuePostDisc(xmlStreamReader
								.getText());
						bDollarValuePostDisc = false;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:
                    elementName = extractElementName(xmlStreamReader);

                    if ("LineItem".equals(elementName)) {
                    	if(!("000000000".equals(lineItem.getItemNumber())) && "1".equals(lineItem.getItemType()) )
                    			lineItemList.add(lineItem);
					}
					processTransaction.setLineItemList(lineItemList);
					break;
				}
				if (!xmlStreamReader.hasNext())
					break;
				event = xmlStreamReader.next();

			}

		} catch (XMLStreamException e) {
			LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			LOGGER.info("exception in xml " + fileName);
		}
		return processTransaction;

	}

    private static String extractElementName(XMLStreamReader xmlStreamReader) {
        String elementName = xmlStreamReader.getLocalName();
        String[] parts = elementName.split(":");

        elementName = parts.length>1?parts[1]:elementName;
       // System.out.println(elementName);

        return elementName;
    }



}
