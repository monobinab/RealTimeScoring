/**
 * 
 */
package analytics.util;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ddas1
 * 
 */
public class XMLParser {
	private static final Logger logger = LoggerFactory
			.getLogger(XMLParser.class);

	public static ProcessTransaction parseXMLProcessTransaction(String fileName) {
		
		ProcessTransaction processTransaction = null;
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		List<LineItem> lineItemList = new ArrayList<LineItem>();
		LineItem lineItem = null;
		boolean bMemberNumber = false;
		boolean bLineItem = false;
		boolean bDivision = false;
		boolean bItemNumber = false;
		boolean bDollarValuePostDisc = false;
		boolean bRequestorID = false;
		try {
			System.out.println("//////////XML STRING//////: " + fileName);
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new StringReader(fileName));
		
			// int event = xmlStreamReader.getEventType();
			processTransaction = new ProcessTransaction();
			while (xmlStreamReader.hasNext()) {
				
				int event = xmlStreamReader.getEventType();
				switch (event) {
									
				case XMLStreamConstants.START_ELEMENT:
				
					QName qname = xmlStreamReader.getName();
										
					if (xmlStreamReader.getLocalName().equals("MemberNumber")) {
						bMemberNumber = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"RequestorID")) {
						bRequestorID = true;
					}
					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Division")) {
						bDivision = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"ItemNumber")) {
						bItemNumber = true;
					}

					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Division")) {
						bDivision = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"ItemNumber")) {
						bItemNumber = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePostDisc")) {
						bDollarValuePostDisc = true;
					}
					break;
				case XMLStreamConstants.CHARACTERS:

					if (bMemberNumber) {
						processTransaction.setMemberNumber(xmlStreamReader
								.getText());
						System.out.println("member number: "
								+ processTransaction.getMemberNumber());
						bMemberNumber = false;
					} else if (bRequestorID) {
						processTransaction.setRequestorID(xmlStreamReader
								.getText());
						System.out.println("requestor id: "
								+ processTransaction.getRequestorID());
						logger.debug("Requestor Id is..."
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
					}

					else if (bDollarValuePostDisc) {
						lineItem.setDollarValuePostDisc(xmlStreamReader
								.getText());
						bDollarValuePostDisc = false;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("LineItem")) {
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
			e.printStackTrace();
		}
		return processTransaction;

	}

}