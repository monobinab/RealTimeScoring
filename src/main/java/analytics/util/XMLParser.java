/**
 * 
 */
package analytics.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import analytics.service.impl.LineItem;
import analytics.service.impl.OrderDetails;
import analytics.service.impl.ProcessTransaction;
import analytics.service.impl.Tender;

/**
 * @author ddas1
 * 
 */
public class XMLParser {
	private static final Logger logger = org.apache.log4j.Logger
			.getLogger(XMLParser.class);

	private static boolean bMessageVersion;
	private static boolean bMemberNumber;
	private static boolean bLineItem;
	private static boolean bLineNumber;
	private static boolean bItemType;
	private static boolean bDivision;
	private static boolean bItemNumber;
	private static boolean bLineItemAmountTypeCode;
	private static boolean bDollarValuePreDisc;
	private static boolean bDollarValuePostDisc;
	private static boolean bPriceMatchAmount;
	private static boolean bPriceMatchBonusAmount;
	private static boolean bQuantity;
	private static boolean bTaxAmount;
	private static boolean bPostSalesAdjustmentAmount;
	/*private static boolean bRequestorID;

	private static boolean bOrderStoreNumber;

	private static boolean bTenderStoreNumber;

	private static boolean bRegisterNumber;
*/
	private static boolean bTransactionNumber;

/*	private static boolean bTransactionTotal;

	private static boolean bTransactionTotalTax;

	private static boolean bTransactionDate;

	private static boolean bTransactionTime;

	private static boolean bAssociateID;

	private static boolean bEarnFlag;*/

	private static boolean bOrderDetails;

	private static boolean bOrderId;

	private static boolean bOrderSource;

	private static boolean bTotalTransactions;

	private static boolean bTransactionSequence;

	private static boolean bTender;

	private static boolean bTenderType;

	private static boolean bTenderAmount;

	public static void main(String[] args) throws FileNotFoundException,
			XMLStreamException, JAXBException {

		// String fileName = "./src/main/resources/soap_env.xml";
		String fileName = "./src/main/resources/POS.xml";
		//processPOS(fileName);
		//print(fileName);
		parseXMLProcessTransaction(fileName);
		//parseXML(fileName);
	}

	public static ProcessTransaction parseXMLProcessTransaction(String fileName) {
		ProcessTransaction processTransaction = null;
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		List<LineItem> lineItemList = new ArrayList<LineItem>();
		LineItem lineItem = null;
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new StringReader(fileName));
			int event = xmlStreamReader.getEventType();

			while (true) {				
				switch (event) {

				case XMLStreamConstants.START_ELEMENT:
					QName qname = xmlStreamReader.getName();
					String pref = qname.getPrefix();
					String name = qname.getLocalPart();
					
					if (xmlStreamReader.getLocalName().equals("MessageVersion")) {
						processTransaction = new ProcessTransaction();
						bMessageVersion = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"MemberNumber")) {
						bMemberNumber = true;
					}else if (xmlStreamReader.getLocalName().equals(
							"TransactionNumber")) {
						bTransactionNumber = true;
					} 
					
					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"LineNumber")) {
						bLineNumber = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("ItemType")) {
						bItemType = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Division")) {
						bDivision = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"ItemNumber")) {
						bItemNumber = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PostSalesAdjustmentAmount")) {
						bPostSalesAdjustmentAmount = true;
					}
					
					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"LineNumber")) {
						bLineNumber = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("ItemType")) {
						bItemType = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Division")) {
						bDivision = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"ItemNumber")) {
						bItemNumber = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"LineItemAmountTypeCode")) {
						bLineItemAmountTypeCode = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePreDisc")) {
						bDollarValuePreDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePostDisc")) {
						bDollarValuePostDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePostDisc")) {
						bDollarValuePostDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PriceMatchAmount")) {
						bPriceMatchAmount = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PriceMatchBonusAmount")) {
						bPriceMatchBonusAmount = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Quantity")) {
						bQuantity = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TaxAmount")) {
						bTaxAmount = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PostSalesAdjustmentAmount")) {
						bPostSalesAdjustmentAmount = true;
					}
					break;
				case XMLStreamConstants.CHARACTERS:
					if (bMessageVersion) {
						processTransaction.setMessageVersion(xmlStreamReader
								.getText());

						bMessageVersion = false;
					} else if (bMemberNumber) {
						processTransaction.setMemberNumber(xmlStreamReader
								.getText());

						bMemberNumber = false;
					} else if (bTransactionNumber) {
						processTransaction.setTransactionNumber(xmlStreamReader
								.getText());

						bTransactionNumber = false;
					}else if (bLineItem) {
						bLineItem = false;
					}else if (bLineNumber) {
						lineItem.setLineNumber(xmlStreamReader.getText());
						bLineNumber = false;
					} else if (bItemType) {
						lineItem.setItemType(xmlStreamReader.getText()
								.toString());
						bItemType = false;
					} else if (bDivision) {
						lineItem.setDivision(xmlStreamReader.getText());
						bDivision = false;
					} else if (bItemNumber) {
						lineItem.setItemNumber(xmlStreamReader.getText());
						bItemNumber = false;
					} else if (bPostSalesAdjustmentAmount) {
						lineItem.setPostSalesAdjustmentAmount(xmlStreamReader
								.getText());
						bPostSalesAdjustmentAmount = false;
					}
					
					if (bLineNumber) {
						lineItem.setLineNumber(xmlStreamReader.getText());
						bLineNumber = false;
					} else if (bItemType) {
						lineItem.setItemType(xmlStreamReader.getText()
								.toString());
						bItemType = false;
					} else if (bDivision) {
						lineItem.setDivision(xmlStreamReader.getText());
						bDivision = false;
					} else if (bItemNumber) {
						lineItem.setItemNumber(xmlStreamReader.getText());
						bItemNumber = false;
					} else if (bLineItemAmountTypeCode) {
						lineItem.setLineItemAmountTypeCode(xmlStreamReader
								.getText());
						bLineItemAmountTypeCode = false;
					} else if (bDollarValuePreDisc) {
						lineItem.setDollarValuePreDisc(xmlStreamReader
								.getText());
						bDollarValuePreDisc = false;
					} else if (bDollarValuePostDisc) {
						lineItem.setDollarValuePostDisc(xmlStreamReader
								.getText());
						bDollarValuePostDisc = false;
					} else if (bPriceMatchAmount) {
						lineItem.setPriceMatchAmount(xmlStreamReader.getText());
						bPriceMatchAmount = false;
					} else if (bPriceMatchBonusAmount) {
						lineItem.setPriceMatchBonusAmount(xmlStreamReader
								.getText());
						bPriceMatchBonusAmount = false;
					} else if (bQuantity) {
						lineItem.setQuantity(xmlStreamReader.getText());
						bQuantity = false;
					} else if (bTaxAmount) {
						lineItem.setTaxAmount(xmlStreamReader.getText());
						bTaxAmount = false;
					} else if (bPostSalesAdjustmentAmount) {
						lineItem.setPostSalesAdjustmentAmount(xmlStreamReader
								.getText());
						bPostSalesAdjustmentAmount = false;
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

	
	 	
	public static List<LineItem> parseXMLLineItems(String fileName) {
		List<LineItem> lineItemList = new ArrayList<LineItem>();
		LineItem lineItem = null;

		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new StringReader(fileName));
			int event = xmlStreamReader.getEventType();
			while (true) {
				switch (event) {

				case XMLStreamConstants.START_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItem = new LineItem();
						bLineItem = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"LineNumber")) {
						bLineNumber = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("ItemType")) {
						bItemType = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Division")) {
						bDivision = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"ItemNumber")) {
						bItemNumber = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"LineItemAmountTypeCode")) {
						bLineItemAmountTypeCode = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePreDisc")) {
						bDollarValuePreDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePostDisc")) {
						bDollarValuePostDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"DollarValuePostDisc")) {
						bDollarValuePostDisc = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PriceMatchAmount")) {
						bPriceMatchAmount = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PriceMatchBonusAmount")) {
						bPriceMatchBonusAmount = true;
					} else if (xmlStreamReader.getLocalName()
							.equals("Quantity")) {
						bQuantity = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TaxAmount")) {
						bTaxAmount = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"PostSalesAdjustmentAmount")) {
						bPostSalesAdjustmentAmount = true;
					}

					break;
				case XMLStreamConstants.CHARACTERS:
					if (bLineNumber) {
						lineItem.setLineNumber(xmlStreamReader.getText());
						bLineNumber = false;
					} else if (bItemType) {
						lineItem.setItemType(xmlStreamReader.getText()
								.toString());
						bItemType = false;
					} else if (bDivision) {
						lineItem.setDivision(xmlStreamReader.getText());
						bDivision = false;
					} else if (bItemNumber) {
						lineItem.setItemNumber(xmlStreamReader.getText());
						bItemNumber = false;
					} else if (bLineItemAmountTypeCode) {
						lineItem.setLineItemAmountTypeCode(xmlStreamReader
								.getText());
						bLineItemAmountTypeCode = false;
					} else if (bDollarValuePreDisc) {
						lineItem.setDollarValuePreDisc(xmlStreamReader
								.getText());
						bDollarValuePreDisc = false;
					} else if (bDollarValuePostDisc) {
						lineItem.setDollarValuePostDisc(xmlStreamReader
								.getText());
						bDollarValuePostDisc = false;
					} else if (bPriceMatchAmount) {
						lineItem.setPriceMatchAmount(xmlStreamReader.getText());
						bPriceMatchAmount = false;
					} else if (bPriceMatchBonusAmount) {
						lineItem.setPriceMatchBonusAmount(xmlStreamReader
								.getText());
						bPriceMatchBonusAmount = false;
					} else if (bQuantity) {
						lineItem.setQuantity(xmlStreamReader.getText());
						bQuantity = false;
					} else if (bTaxAmount) {
						lineItem.setTaxAmount(xmlStreamReader.getText());
						bTaxAmount = false;
					} else if (bPostSalesAdjustmentAmount) {
						lineItem.setPostSalesAdjustmentAmount(xmlStreamReader
								.getText());
						bPostSalesAdjustmentAmount = false;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("LineItem")) {
						lineItemList.add(lineItem);
					}

					break;
				}
				if (!xmlStreamReader.hasNext())
					break;

				event = xmlStreamReader.next();
			}

		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
		return lineItemList;
	}

	
	public static List<OrderDetails> parseOrderDetailsXML(String fileName) {
		List<OrderDetails> orderDetailsList = new ArrayList<OrderDetails>();
		OrderDetails orderDetails = null;

		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		try {

			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new FileInputStream(fileName));
			int event = xmlStreamReader.getEventType();
			while (true) {
				switch (event) {

				case XMLStreamConstants.START_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("OrderDetails")) {
						orderDetails = new OrderDetails();
						bOrderDetails = true;
					} else if (xmlStreamReader.getLocalName().equals("OrderId")) {
						bOrderId = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"OrderSource")) {
						bOrderSource = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TotalTransactions")) {
						bTotalTransactions = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TransactionSequence")) {
						bTransactionSequence = true;
					}
					break;
				case XMLStreamConstants.CHARACTERS:
					if (bOrderId) {
						orderDetails.setOrderId(xmlStreamReader.getText());
						bOrderId = false;
					} else if (bOrderSource) {
						orderDetails.setOrderSource(xmlStreamReader.getText()
								.toString());
						bOrderSource = false;
					} else if (bTotalTransactions) {
						orderDetails.setTotalTransactions(xmlStreamReader
								.getText());
						bTotalTransactions = false;
					} else if (bTransactionSequence) {
						orderDetails.setTransactionSequence(xmlStreamReader
								.getText());
						bTransactionSequence = false;
					}
					break;
				case XMLStreamConstants.END_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("OrderDetails")) {
						orderDetailsList.add(orderDetails);
					}

					break;
				}
				if (!xmlStreamReader.hasNext())
					break;

				event = xmlStreamReader.next();
			}

		} catch (FileNotFoundException | XMLStreamException e) {
			e.printStackTrace();
		}
		return orderDetailsList;
	}

	public static List<Tender> parseTenderXML(String fileName) {
		List<Tender> tenderList = new ArrayList<Tender>();
		Tender tender = null;

		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		try {
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new FileInputStream(fileName));
			int event = xmlStreamReader.getEventType();
			while (true) {
				switch (event) {

				case XMLStreamConstants.START_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("Tender")) {
						tender = new Tender();
						bTender = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TenderType")) {
						bTenderType = true;
					} else if (xmlStreamReader.getLocalName().equals(
							"TenderAmount")) {
						bTenderAmount = true;
					}
					break;
				case XMLStreamConstants.CHARACTERS:
					if (bTenderType) {
						tender.setTenderType(xmlStreamReader.getText());
						bTenderType = false;
					} else if (bTenderAmount) {
						tender.setTenderAmount(xmlStreamReader.getText()
								.toString());
						bTenderAmount = false;
					}
					break;
				case XMLStreamConstants.END_ELEMENT:

					if (xmlStreamReader.getLocalName().equals("Tender")) {
						tenderList.add(tender);
					}

					break;
				}
				if (!xmlStreamReader.hasNext())
					break;

				event = xmlStreamReader.next();
			}

		} catch (FileNotFoundException | XMLStreamException e) {
			e.printStackTrace();
		}
		return tenderList;
	}


	   
}