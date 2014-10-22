/**
 * 
 */
package analytics.util;

import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
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
		List<String> testMembers= new ArrayList<String>();
		testMembers.add("7081197526669586");
		testMembers.add("7081187606915702"); 
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
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new StringReader(fileName));
		
			// int event = xmlStreamReader.getEventType();
			processTransaction = new ProcessTransaction();
			while (xmlStreamReader.hasNext()) {
				
				int event = xmlStreamReader.getEventType();
                switch (event) {
									
				case XMLStreamConstants.START_ELEMENT:
                    String elementName = xmlStreamReader.getLocalName().replace("tns:","");


                    QName qname = xmlStreamReader.getName();
					if (elementName.contains("MemberNumber")) {
						bMemberNumber = true;
					} else if ("RequestorID".equals(
							elementName)) {
						bRequestorID = true;
					} else if("EarnFlag".equals(elementName)){
						bEarnFlag = true;
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
						if(memberNumber.startsWith("7081") && !testMembers.contains(memberNumber) && memberNumber.length()==16)
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
					} else if (bLineItem) {
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
						if("E".equals(xmlStreamReader.getText()))
								bEarnFlag = false;
						else 
							return null;
					}

					else if (bDollarValuePostDisc) {
						lineItem.setDollarValuePostDisc(xmlStreamReader
								.getText());
						bDollarValuePostDisc = false;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:
                    elementName = xmlStreamReader.getLocalName().replace("tns:","");

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
		}
		return processTransaction;

	}

}