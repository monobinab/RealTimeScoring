package analytics.bolt;

import analytics.service.impl.LineItem;
import analytics.service.impl.ProcessTransaction;
import analytics.service.impl.TransactionLineItem;
import analytics.util.DBConnection;
import analytics.util.XMLParser;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.ibm.jms.JMSMessage;
import com.mongodb.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class TellurideParsingBoltPOS extends BaseRichBolt {

	static final Logger logger = Logger
			.getLogger(TellurideParsingBoltPOS.class);
	/**
	 * Created by Devarshi Das 8/27/2014
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	private DB db;
	private MongoClient mongoClient;
	private DBCollection memberCollection;
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

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public void setMemberCollection(DBCollection memberCollection) {
		this.memberCollection = memberCollection;
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

		logger.info("PREPARING PARSING POS BOLT");

		try {
			db = new DBConnection().getDBConnectionWithoutCredentials();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		memberCollection = db.getCollection("memberVariables");
		divLnItmCollection = db.getCollection("divLnItm");
		divLnVariableCollection = db.getCollection("divLnVariable");
		ksndivcatCollection = db.getCollection("divCatKsn");
		divCatVariableCollection = db.getCollection("divCatVariable");

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
		/*DBCursor divCatVarCursor = divCatVariableCollection.find();
		for (DBObject divCatDBObject : divCatVarCursor) {
			logger.info("Division and Category are..."+divCatDBObject.get("d")+divCatDBObject.get("c").toString());
			if (divCatVariablesMap.get(divCatDBObject.get("d").toString().concat(divCatDBObject.get("c").toString())) == null) {
				Collection<String> varColl = new ArrayList<String>();
				varColl.add(divCatDBObject.get("v").toString());
				divCatVariablesMap.put(divCatDBObject.get("d").toString().trim().concat(divCatDBObject.get("c").toString().trim()),
						varColl);
			} else {
				Collection<String> varColl = divCatVariablesMap
						.get(divCatDBObject.get("d").toString().trim().concat(divCatDBObject.get("c").toString().trim()));
				varColl.add(divCatDBObject.get("v").toString().toUpperCase());
				divCatVariablesMap.put(divCatDBObject.get("d").toString().trim().concat(divCatDBObject.get("c").toString().trim()),
						varColl);
			}
		}*/
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
     
		logger.info("The time it enters inside execute method"+System.currentTimeMillis());
		String lyl_id_no = "";
		ProcessTransaction processTransaction = null;

		String transactionXmlAsString = "";
		String kcomTransaction = "";
		String kposTransaction = "";

		JMSMessage documentKPOS = null;
		JMSMessage documentKCOM = null;
		JMSMessage documentSCOM = null;
		// KPOS and KCOM
		JMSMessage documentNPOS = (JMSMessage) input.getValueByField("npos");

		try {

			transactionXmlAsString = convertStreamToString(documentNPOS);

		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if ( StringUtils.isEmpty(transactionXmlAsString)) {
			return;
		}

		if (transactionXmlAsString.contains("xmlns:soapenv")) {

			logger.info("Processing Soap Envelop xml String...");
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
			logger.info("Requestor ID is..." + requestorID);
		} else {
			logger.info("Requestor ID is null....");
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
			String l_id = hashLoyaltyId(lyl_id_no);

			// 7)FIND DIVISION #, ITEM #, AMOUNT AND
			// FIND LINE FROM DIVISION # + ITEM #
			// AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID +
			// ALL
			// TRANSACTION LEVEL DATA

			Collection<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();
			logger.info("nposTransaction XML is" + transactionXmlAsString.toString());

			List<LineItem> lineItems = processTransaction.getLineItemList();
			logger.info("Line Items are ...>>>>>>>>>>>>>>>>>>>>>>>>>>.."
					+ lineItems.toString());

			if (lineItems != null && lineItems.size() != 0) {
				for (LineItem lineItem : lineItems) {

					String item = "";
					String amount = lineItem.getDollarValuePostDisc();
					logger.info("Item is...." + item + "...Amount is...."
							+ amount);
										if (amount.contains("-")) {
						logger.info("amount_contains -");
						continue;
					} else {
						// KPOS and KCOM
						if ("KPOS".equalsIgnoreCase(requestorID)
								|| "KCOM".equalsIgnoreCase(requestorID)) {
							item = lineItem.getItemNumber();
							logger.info("Item is..."+item);
							String divCategory = getDivCategoryFromCollection(item);
							logger.info("division and category are ...." + divCategory);
                            String div = StringUtils.substring(divCategory, 0, 2);
                            String cat = StringUtils.substring(divCategory, 3, 6);
                            TransactionLineItem transactionLineItem = new TransactionLineItem(
									l_id, div, item, "", cat,
									Double.valueOf(amount));
							logger.info("Transaction Line Item is ..."
									+ transactionLineItem);

							// find all variables affected by div-line
							List<String> foundVariablesList = null;
							foundVariablesList = new ArrayList<String>();
							if (divCatVariablesMap
									.containsKey(transactionLineItem.getDiv()
											+ transactionLineItem.getCategory())
									|| divCatVariablesMap
											.containsKey(transactionLineItem
													.getDiv())) {

								Collection<String> divVariableCollection = divCatVariablesMap
										.get(transactionLineItem.getDiv());
								Collection<String> divCatVariableCollection = divCatVariablesMap
										.get(transactionLineItem.getDiv()
												+ transactionLineItem
														.getCategory());
								if (divVariableCollection != null) {
									for (String var : divVariableCollection) {
										logger.info("Div is added.....  in variable List"
												+ transactionLineItem.getDiv());
										foundVariablesList.add(var);
									}
								}
								if (divCatVariableCollection != null) {
									for (String var : divCatVariableCollection) {
										logger.info("Div is added..... in lnvariable List"
												+ transactionLineItem.getDiv());
										foundVariablesList.add(var);
									}
								}
								transactionLineItem
										.setVariableList(foundVariablesList);
								lineItemList.add(transactionLineItem);
								logger.info("Line Items are added inside lineItemList......"
										+ lineItemList.size());
							}
						} else {
							if (lineItem.getItemNumber().length() >= 6) {

								item = lineItem.getItemNumber().substring(
										lineItem.getItemNumber().length() - 5);

							} else {
								item = lineItem.getItemNumber();
							}
                            String div = lineItem.getDivision();

                            String line = getLineFromCollection(div, item);
							if (line == null) {
								logger.info("Line is null");
								continue;
							}
							logger.info("Line is ...." + line);
							TransactionLineItem transactionLineItem = null; 
							transactionLineItem = new TransactionLineItem(
									l_id, div, item, line,
									Double.valueOf(amount));
							logger.info("Transaction Line Item is ..."
									+ transactionLineItem);
							// find all variables affected by div-line
							List<String> foundVariablesList = null;
									 foundVariablesList = new ArrayList<String>();
							if (divLnVariablesMap
									.containsKey(transactionLineItem.getDiv()
											+ transactionLineItem.getLine())
									|| divLnVariablesMap
											.containsKey(transactionLineItem
													.getDiv())) {



								Collection<String> divVariableCollection = divLnVariablesMap
										.get(transactionLineItem.getDiv());
								Collection<String> divLnVariableCollection = divLnVariablesMap
										.get(transactionLineItem.getDiv()
												+ transactionLineItem.getLine());
								if (divVariableCollection != null) {
									for (String var : divVariableCollection) {
										logger.info("Div is added.....  in variable List"
												+ transactionLineItem.getDiv());
										foundVariablesList.add(var);
									}
								}
								if (divLnVariableCollection != null) {
									for (String var : divLnVariableCollection) {
										logger.info("Div is added..... in lnvariable List"
												+ transactionLineItem.getDiv());
										foundVariablesList.add(var);
									}
								}
								transactionLineItem
										.setVariableList(foundVariablesList);
								lineItemList.add(transactionLineItem);
								logger.info("Line Items are added inside lineItemList......"
										+ lineItemList.size());
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
					listToEmit.add(createJsonFromVarValueMap(varAmountMap));
					listToEmit.add(requestorID);
					logger.info(requestorID + " Point of SALE is touched...");
					logger.info(" *** parsing bolt emitting: "
						+ listToEmit.toString());

				// 9) EMIT VARIABLES TO VALUES MAP IN GSON DOCUMENT
				if (listToEmit != null && !listToEmit.isEmpty()) {
					this.outputCollector.emit(listToEmit);
				}
			}
		}
	}

	private final Object createJsonFromVarValueMap(Map<String, String> varAmountMap) {
		// Create string in JSON format to emit

		Gson gson = new Gson();
		Type transLineItemType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		String transLineItemListString = gson.toJson(varAmountMap,
				transLineItemType);
		return transLineItemListString;
	}

	private final Object createJsonFromLineItemList(
			Collection<TransactionLineItem> lineItemCollection) {
		// Create string in JSON format to emit
		Gson gson = new Gson();
		Map<String, String> varAmountMap = new HashMap<String, String>();
		Type transLineItemType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		if (lineItemCollection == null || lineItemCollection.isEmpty()) {
			return null;
		}

		for (TransactionLineItem lnItm : lineItemCollection) {
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
					varAmountMap.put(v.toUpperCase(), String.valueOf(a1));
				}
			}
		}

		String transLineItemListString = gson.toJson(varAmountMap,
				transLineItemType);
		logger.info(">>>>>>>>>>>>>>>>Trans LineItemList String"
				+ transLineItemListString
				+ "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		return transLineItemListString;
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
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source"));
	}

	public String hashLoyaltyId(String l_id) {
		String hashed = new String();
		try {
			SecretKeySpec signingKey = new SecretKeySpec("mykey".getBytes(),
					"HmacSHA1");
			Mac mac = Mac.getInstance("HmacSHA1");
			try {
				mac.init(signingKey);
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			byte[] rawHmac = mac.doFinal(l_id.getBytes());
			hashed = new String(Base64.encodeBase64(rawHmac));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashed;
	}

	private final String getLineFromCollection(String div, String item) {
		logger.info("searching for line");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", div);
		queryLine.put("i", item);

		logger.info("query: " + queryLine);
		DBObject divLnItm = divLnItmCollection.findOne(queryLine);
		logger.info("line: " + divLnItm);

		if (divLnItm == null || divLnItm.keySet() == null
				|| divLnItm.keySet().isEmpty()) {
			logger.info("Everything is null");
			return null;
		}
		String line = divLnItm.get("l").toString();
		logger.info("  found line: " + line);
		return line;
	}

	private final String getDivCategoryFromCollection(String item) {
		logger.info("searching for category");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("k", item);

		logger.info("query: " + queryLine);
		DBObject ksndivcat = ksndivcatCollection.findOne(queryLine);
		logger.info("category: " + ksndivcat);

		if (ksndivcat == null || ksndivcat.keySet() == null
				|| ksndivcat.keySet().isEmpty()) {
			logger.info("Ksndivcat is null");
			return null;
		}
		String category = ksndivcat.get("c").toString();
        String div = ksndivcat.get("d").toString();

        logger.info("  found category: " + category);
        logger.info("  found division: " + div);

        return div+category;
	}

	private final String getDivFromCollection(String item) {
		logger.info("searching for category");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("k", item);

		logger.info("query: " + queryLine);
		DBObject Ksndivcat = ksndivcatCollection.findOne(queryLine);
		logger.info("division: " + Ksndivcat);

		if (Ksndivcat == null || Ksndivcat.keySet() == null
				|| Ksndivcat.keySet().isEmpty()) {
			logger.info("Ksndivcat is null");
			return null;
		}
		String div = Ksndivcat.get("d").toString();
		logger.info("  found division: " + div);
		return div;
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
		logger.info(stringMessage.toString());
		return stringMessage;
	}

}
