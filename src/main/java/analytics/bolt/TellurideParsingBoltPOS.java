package analytics.bolt;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.XMLParser;
import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;
import analytics.util.objects.TransactionLineItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ibm.jms.JMSMessage;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TellurideParsingBoltPOS extends BaseRichBolt {

	static final Logger logger = LoggerFactory
			.getLogger(TellurideParsingBoltPOS.class);
	/**
	 * Created by Devarshi Das 8/27/2014
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	private DB db;
	private DBCollection divLnItmCollection;
	private DBCollection ksndivcatCollection;
	private DBCollection divLnVariableCollection;
	private DBCollection divCatVariableCollection; 
	private Map<String, Collection<String>> divLnVariablesMap;
	private Map<String, Collection<String>> divCatVariablesMap;
	private String requestorID = "";

	public void setOutputCollector(OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
	}

	public void setDb(DB db) {
		this.db = db;
	}

	public void setDivLnItmCollection(DBCollection divLnItmCollection) {
		this.divLnItmCollection = divLnItmCollection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;

		logger.info("Preparing telluride parsing bolt");

		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			logger.error("Unable to obtain DB connection", e);
		}

		logger.debug("Getting mongo collections");
		divLnItmCollection = db.getCollection("divLnItm");
		divLnVariableCollection = db.getCollection("divLnVariable");
		ksndivcatCollection = db.getCollection("divCatKsn");
		divCatVariableCollection = db.getCollection("divCatVariable");

		logger.trace("Populate div line variables map");
		// populate divLnVariablesMap
		divLnVariablesMap = new HashMap<String, Collection<String>>();
		DBCursor divLnVarCursor = divLnVariableCollection.find();
		for (DBObject divLnDBObject : divLnVarCursor) {
			if (divLnVariablesMap.get(divLnDBObject.get("d")) == null) {
				Collection<String> varColl = new ArrayList<String>();
				varColl.add(divLnDBObject.get("v").toString());
				divLnVariablesMap.put(divLnDBObject.get("d").toString(),
						varColl);
			} else {
				Collection<String> varColl = divLnVariablesMap
						.get(divLnDBObject.get("d").toString());
				varColl.add(divLnDBObject.get("v").toString().toUpperCase());
				divLnVariablesMap.put(divLnDBObject.get("d").toString(),
						varColl);
			}
		}

		logger.trace("Populate div cat variables map");
		//populate divCatVariablesMap
		divCatVariablesMap = new HashMap<String, Collection<String>>();
		DBCursor divVarCursor = divCatVariableCollection.find();
		for (DBObject divDBObject : divVarCursor) {
			if (divCatVariablesMap.get(divDBObject.get("d").toString().trim()) == null) {
				Collection<String> varColl = new ArrayList<String>();
				varColl.add(divDBObject.get("v").toString());
				divCatVariablesMap.put(divDBObject.get("d").toString(),
						varColl);
			} else {
				Collection<String> varColl = divCatVariablesMap
						.get(divDBObject.get("d").toString().trim());
				varColl.add(divDBObject.get("v").toString().toUpperCase());
				divCatVariablesMap.put(divDBObject.get("d").toString(),
						varColl);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		if(logger.isDebugEnabled())
			logger.debug("The time it enters inside Telluride parsing bolt execute method"+System.currentTimeMillis()+" and the message ID is ..."+input.getMessageId());
		String lyl_id_no = "";
		ProcessTransaction processTransaction = null;

		String transactionXmlAsString = "";
		// KPOS and KCOM
		JMSMessage documentNPOS = (JMSMessage) input.getValueByField("npos");

		try {
			transactionXmlAsString = convertStreamToString(documentNPOS);
		} catch (JMSException e) {
			logger.error("Unable to read message from MQ",e);
			outputCollector.fail(input);
		} catch (Exception e) {
			logger.error("Unable to read message from MQ",e);
			outputCollector.fail(input);
		}
		if ( StringUtils.isEmpty(transactionXmlAsString)) {
			return;
		}

		logger.debug("Parsing MQ message XML");
		if (transactionXmlAsString.contains("xmlns:soapenv")) {

			//logger.info("Processing Soap Envelop xml String...");
			StringUtils.substringBetween(transactionXmlAsString.toString(),
					"<soapenv:Envelope xmlns:soapenv="
							+ "http://www.w3.org/2003/05/soap-envelope>"
							+ "<soapenv:Body>/",
					"</soapenv:Body></soapenv:Envelope>");
			processTransaction = XMLParser
					.parseXMLProcessTransaction(transactionXmlAsString);
			// XMLParser.parseXMLLineItems(nposTransaction);

		} else if (transactionXmlAsString.contains("tns:ProcessTransaction")) {
			StringUtils
					.substringBetween(
							transactionXmlAsString.toString(),
							"<tns:ProcessTransaction xmlns:tns=\"http://www.w3.org/2003/05/tns\">",
							"</tns:ProcessTransaction>");
			processTransaction = XMLParser
					.parseXMLProcessTransaction(transactionXmlAsString);
		}



		// 1) TEST IF TRANSACTION TYPE CODE IS = 1 (RETURN IF FALSE)
		// 2) TEST IF TRANSACTION IS A MEMBER TRANSACTION (IF NOT RETURN)
		// 3) HASH LOYALTY ID
		// 4) FIND DIVISION #, ITEM #, AMOUNT AND
		// FIND LINE FROM DIVISION # + ITEM #
		// AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID + ALL
		// TRANSACTION LEVEL DATA
		// 5) EMIT LINE ITEMS

		if (processTransaction != null) {
			requestorID = processTransaction.getRequestorID();
			logger.debug("Requestor ID is..." + requestorID);
		} else {
			logger.debug("Requestor ID is null....");
		}
		/*
		 * if (requestorID == null) { return; }
		 */
		if (processTransaction != null
				&& !"".equalsIgnoreCase(processTransaction
						.getTransactionNumber())) {

			lyl_id_no = processTransaction.getMemberNumber();

			if (StringUtils.isEmpty(lyl_id_no)) {
				return;
			}

			// 6) HASH LOYALTY ID
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);

			// 7)FIND DIVISION #, ITEM #, AMOUNT AND
			// FIND LINE FROM DIVISION # + ITEM #
			// AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID +
			// ALL
			// TRANSACTION LEVEL DATA

			Collection<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();
			//logger.info("nposTransaction XML is" + transactionXmlAsString.toString());

			List<LineItem> lineItems = processTransaction.getLineItemList();
			logger.trace("Line Items are .."+ lineItems.toString());

			if (lineItems != null && lineItems.size() != 0) {
				for (LineItem lineItem : lineItems) {

					String item = "";
					String amount = lineItem.getDollarValuePostDisc();
					/*logger.info("Item is...." + item + "...Amount is...."
							+ amount);*/
					if (amount.contains("-")) {
						logger.debug("amount_contains -");
						continue;
					} else {
						// KPOS and KCOM
						logger.debug("KPOS or KCOM transaction processing");
						if ("KPOS".equalsIgnoreCase(requestorID)
								|| "KCOM".equalsIgnoreCase(requestorID)) {
							item = lineItem.getItemNumber();
							//logger.info("Item is..."+item);
							String divCategory = getDivCategoryFromCollection(item);
							//logger.info("division and category are ...." + divCategory);
                            String div = StringUtils.substring(divCategory, 0, 3);//Picks up start, end-1
                            String cat = StringUtils.substring(divCategory, 3, 7);
                            TransactionLineItem transactionLineItem = new TransactionLineItem(
									l_id, div, item, cat,
									Double.valueOf(amount));
							/*logger.info("Transaction Line Item is ..."
									+ transactionLineItem);*/

							// find all variables affected by div-line
							List<String> foundVariablesList = null;
							foundVariablesList = new ArrayList<String>();
							if (divCatVariablesMap
									.containsKey(transactionLineItem.getDiv()
											+ transactionLineItem.getLineOrCategory())
									|| divCatVariablesMap
											.containsKey(transactionLineItem
													.getDiv())) {

								Collection<String> divVariableCollection = divCatVariablesMap
										.get(transactionLineItem.getDiv());
								Collection<String> divCatVariableCollection = divCatVariablesMap
										.get(transactionLineItem.getDiv()
												+ transactionLineItem
														.getLineOrCategory());
								if (divVariableCollection != null) {
									for (String var : divVariableCollection) {
										/*logger.info("Div is added.....  in variable List"
												+ transactionLineItem.getDiv());*/
										foundVariablesList.add(var);
									}
								}
								if (divCatVariableCollection != null) {
									for (String var : divCatVariableCollection) {
										/*logger.info("Div is added..... in lnvariable List"
												+ transactionLineItem.getDiv());*/
										foundVariablesList.add(var);
									}
								}
								transactionLineItem
										.setVariableList(foundVariablesList);
								lineItemList.add(transactionLineItem);
								/*logger.info("Line Items are added inside lineItemList......"
										+ lineItemList.size());*/
							}
						} else {
							logger.debug("Sears transaction processing");
							if (lineItem.getItemNumber().length() >= 6) {

								item = lineItem.getItemNumber().substring(
										lineItem.getItemNumber().length() - 5);

							} else {
								item = lineItem.getItemNumber();
							}
                            String div = lineItem.getDivision();

                            String line = getLineFromCollection(div, item);
							if (line == null) {
								/*logger.info("Line is null");*/
								continue;
							}
							//logger.info("Line is ...." + line);
							TransactionLineItem transactionLineItem = null; 
							transactionLineItem = new TransactionLineItem(
									l_id, div, item, line,
									Double.valueOf(amount));
							/*logger.info("Transaction Line Item is ..."
									+ transactionLineItem);*/
							// find all variables affected by div-line
							List<String> foundVariablesList = null;
									 foundVariablesList = new ArrayList<String>();
							if (divLnVariablesMap
									.containsKey(transactionLineItem.getDiv()
											+ transactionLineItem.getLineOrCategory())
									|| divLnVariablesMap
											.containsKey(transactionLineItem
													.getDiv())) {



								Collection<String> divVariableCollection = divLnVariablesMap
										.get(transactionLineItem.getDiv());
								Collection<String> divLnVariableCollection = divLnVariablesMap
										.get(transactionLineItem.getDiv()
												+ transactionLineItem.getLineOrCategory());
								if (divVariableCollection != null) {
									for (String var : divVariableCollection) {
										/*logger.info("Div is added.....  in variable List"
												+ transactionLineItem.getDiv());*/
										foundVariablesList.add(var);
									}
								}
								if (divLnVariableCollection != null) {
									for (String var : divLnVariableCollection) {
										/*logger.info("Div is added..... in lnvariable List"
												+ transactionLineItem.getDiv());*/
										foundVariablesList.add(var);
									}
								}
								transactionLineItem
										.setVariableList(foundVariablesList);
								lineItemList.add(transactionLineItem);
								/*logger.info("Line Items are added inside lineItemList......"
										+ lineItemList.size());*/
							}

						}
					}
				}
			}
			if (lineItemList != null && !lineItemList.isEmpty()) {

				// 8) FOR EACH LINE ITEM FIND ASSOCIATED VARIABLES BY DIVISION
				// AND LINE
				Map<String, String> varAmountMap = null;
				varAmountMap = new HashMap<String, String>();
				List<Object> listToEmit = new ArrayList<Object>();
				for (TransactionLineItem lnItm : lineItemList) {
					List<String> varList = lnItm.getVariableList();
					if (varList == null || varList.isEmpty()) {
						continue;
					}
					for (String v : varList) {
						if (!varAmountMap.containsKey(v.toUpperCase())) {
							varAmountMap.put(v.toUpperCase(),
									String.valueOf(lnItm.getAmount()));
						} else {
							Double a1 = Double.valueOf(varAmountMap.get(v));
							a1 = a1 + lnItm.getAmount();
							varAmountMap.remove(v.toUpperCase());
							varAmountMap.put(v.toUpperCase(),
									String.valueOf(a1));
						}
					}
				}

					listToEmit.add(l_id);
					listToEmit.add(JsonUtils.createJsonFromStringStringMap(varAmountMap));
					listToEmit.add(requestorID);
					listToEmit.add(input.getMessageId().toString());
					logger.debug(requestorID + " Point of SALE is touched...");
					logger.debug(" *** telluride parsing bolt emitting: "
						+ listToEmit.toString());

				// 9) EMIT VARIABLES TO VALUES MAP IN JSON DOCUMENT
				if (listToEmit != null && !listToEmit.isEmpty()) {
					this.outputCollector.emit(listToEmit);
				}
			}
		}
		outputCollector.ack(input);
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
	 * topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source","messageID"));
	}

	//TODO: Move this to util
	private final String getLineFromCollection(String div, String item) {
		logger.debug("searching for line");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", div);
		queryLine.put("i", item);

		/*logger.info("query: " + queryLine);*/
		DBObject divLnItm = divLnItmCollection.findOne(queryLine);
		logger.trace("line: " + divLnItm);

		if (divLnItm == null || divLnItm.keySet() == null
				|| divLnItm.keySet().isEmpty()) {
			logger.trace("Everything is null");
			return null;
		}
		String line = divLnItm.get("l").toString();
		logger.debug("  found line: " + line);
		return line;
	}

	private final String getDivCategoryFromCollection(String item) {
		logger.debug("searching for category");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("k", item);

		logger.trace("query: " + queryLine);
		DBObject ksndivcat = ksndivcatCollection.findOne(queryLine);
		//logger.info("category: " + ksndivcat);

		if (ksndivcat == null || ksndivcat.keySet() == null
				|| ksndivcat.keySet().isEmpty()) {
			//logger.info("Ksndivcat is null");
			return null;
		}
		String category = ksndivcat.get("c").toString();
        String div = ksndivcat.get("d").toString();

        /*logger.info("  found category: " + category);
        logger.info("  found division: " + div);*/

        return div+category;
	}

	private final static String convertStreamToString(final Message jmsMsg)
			throws Exception {
		String stringMessage = "";
		BytesMessage bMsg = (BytesMessage) jmsMsg;
		byte[] buffer = new byte[40620];
		int byteRead;
		ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
		while ((byteRead = bMsg.readBytes(buffer)) != -1) {
			bout.write(buffer, 0, byteRead);
		}
		bout.flush();
		stringMessage = new String(bout.toByteArray());

		bout.close();
		//logger.info(stringMessage.toString());
		return stringMessage;
	}

}
