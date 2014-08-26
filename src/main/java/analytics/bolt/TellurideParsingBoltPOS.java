package analytics.bolt;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import analytics.RealTimeScoringTopology;
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
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class TellurideParsingBoltPOS extends BaseRichBolt {

	static final Logger logger = Logger
			.getLogger(TellurideParsingBoltPOS.class);
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	DB db;
	MongoClient mongoClient;
	DBCollection memberCollection;
	DBCollection divLnItmCollection;
	DBCollection KsndivcatCollection;
	DBCollection divLnVariableCollection;

	private Map<String, Collection<String>> divLnVariablesMap;

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

		/*
		 * (non-Javadoc)
		 * 
		 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
		 * backtype.storm.task.TopologyContext,
		 * backtype.storm.task.OutputCollector)
		 */

		 logger.info("PREPARING PARSING POS BOLT");
		/*
		 * try { mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com",
		 * 20000); } catch (UnknownHostException e) { e.printStackTrace(); }
		 * 
		 * db = mongoClient.getDB("RealTimeScoring");
		 * //db.authenticate(configuration.getString("mongo.db.user"),
		 * configuration.getString("mongo.db.password").toCharArray());
		 * db.authenticate("rtsw", "5core123".toCharArray());
		 */
		/*try {
			db = new DBConnection().getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	    //System.out.println("PREPARING PARSING POS BOLT");
        try {
		// mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
            mongoClient = new MongoClient("trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

		//db = mongoClient.getDB("RealTimeScoring");
		//db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
		//db.authenticate("rtsw", "5core123".toCharArray());
        db = mongoClient.getDB("test");

		memberCollection = db.getCollection("memberVariables");
		divLnItmCollection = db.getCollection("divLnItm");
		divLnVariableCollection = db.getCollection("divLnVariable");
		KsndivcatCollection = db.getCollection("divCatKsn");
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
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

		String lyl_id_no = null;
		ProcessTransaction processTransaction =null;
		
		String nposTransaction = null;
		String requestorID = processTransaction.getRequestorID();
		JMSMessage document = null;
		//KPOS and KCOM
		if("KPOS".equalsIgnoreCase(requestorID)){
		document = (JMSMessage) input.getValueByField("kpos");
		}else if ("KCOM".equalsIgnoreCase(requestorID)){
		document = (JMSMessage) input.getValueByField("kcom");
		}else if ("NPOS".equalsIgnoreCase(requestorID)){
		document = (JMSMessage) input.getValueByField("npos");
		}
		try {

			nposTransaction = convertStreamToString(document);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (nposTransaction == null) {
			return;
		}

		
		if(nposTransaction.contains("xmlns:soapenv")){
			logger.info("Processing Soap Envelop xml String...");
			StringUtils.substringBetween(nposTransaction.toString(),
					"<soapenv:Envelope xmlns:soapenv="
							+ "http://www.w3.org/2003/05/soap-envelope>"
							+ "<soapenv:Body>/",
					"</soapenv:Body></soapenv:Envelope>");
			processTransaction = XMLParser
					.parseXMLProcessTransaction(nposTransaction);
			//XMLParser.parseXMLLineItems(nposTransaction);
	
		}else if(nposTransaction.contains("tns:ProcessTransaction")){
			StringUtils.substringBetween(nposTransaction.toString(),
					"<tns:ProcessTransaction xmlns:tns=\"http://www.w3.org/2003/05/tns\">",
					"</tns:ProcessTransaction>");
			processTransaction = XMLParser
					.parseXMLProcessTransaction(nposTransaction);
		}
	
		
		
		// 1) TEST IF TRANSACTION TYPE CODE IS = 1 (RETURN IF FALSE)
		// 2) TEST IF TRANSACTION IS A MEMBER TRANSACTION (IF NOT RETURN)
		// 3) HASH LOYALTY ID
		// 4) FIND DIVISION #, ITEM #, AMOUNT AND
		// FIND LINE FROM DIVISION # + ITEM #
		// AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID + ALL
		// TRANSACTION LEVEL DATA
		// 5) EMIT LINE ITEMS

			
		boolean isSale = false;
		if (processTransaction != null
				&& !"".equalsIgnoreCase(processTransaction
						.getTransactionNumber())) {
			String transactionNumber = processTransaction
					.getTransactionNumber();
			lyl_id_no = processTransaction.getMemberNumber();

			if (lyl_id_no == null) {
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
			logger.info("nposTransaction XML is" + nposTransaction.toString());
			List<LineItem> lineItems = processTransaction.getLineItemList();
			logger.info("Line Items are ...>>>>>>>>>>>>>>>>>>>>>>>>>>.."+lineItems.toString());
			String tempVal = "";
			Integer i = null;
			
			if (lineItems != null && lineItems.size() != 0) {
				for (LineItem lineItem : lineItems) {
					String div = lineItem.getDivision();
					String item="";
					if("KPOS".equalsIgnoreCase(requestorID)||"KCOM".equalsIgnoreCase(requestorID)){
						item = lineItem.getItemNumber();
					}else{
						if(lineItem.getItemNumber().length()>=6){
							
							item = lineItem.getItemNumber().substring(lineItem.getItemNumber().length() - 5);
							 
						}else{
							item = lineItem.getItemNumber();
						}	
					}
					String category = getCategoryFromCollection(div,item);;
					String amount = lineItem.getDollarValuePostDisc();
					logger.info("category is ...."+category+"...item is...."+item +"...amount is...."+amount);
					//String line = lineItem.getLineNumber();
					String line = getLineFromCollection(div,item);
					if (line == null) {
						logger.info("Line is null");
						continue;
					}
					if (amount.contains("-")) {
						logger.info("amount_contains -");
						continue;
					} else {
						//KPOS and KCOM
						if("KPOS".equalsIgnoreCase(requestorID)||"KCOM".equalsIgnoreCase(requestorID)){
						line ="";
						TransactionLineItem transactionLineItem = new TransactionLineItem(
								l_id, div, item, line,category,
								Double.valueOf(amount) / 100);
							logger.info("Transaction Line Item is ..."+transactionLineItem);
						// find all variables affected by div-line
						List<String> foundVariablesList = new ArrayList<String>();
						if (divLnVariablesMap.containsKey(transactionLineItem
								.getDiv() + transactionLineItem.getLine())
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
									logger.info("Div is added.....  in variable List"+transactionLineItem.getDiv());
									foundVariablesList.add(var);
								}
							}
							if (divLnVariableCollection != null) {
								for (String var : divLnVariableCollection) {
									logger.info("Div is added..... in lnvariable List"+transactionLineItem.getDiv());
									foundVariablesList.add(var);
								}
							}
							transactionLineItem
									.setVariableList(foundVariablesList);
							lineItemList.add(transactionLineItem);
							logger.info("Line Items are added inside lineItemList......"+lineItemList.size());
						}
					}else {
						TransactionLineItem transactionLineItem = new TransactionLineItem(
								l_id, div, item, line,
								Double.valueOf(amount) / 100);
							logger.info("Transaction Line Item is ..."+transactionLineItem);
						// find all variables affected by div-line
						List<String> foundVariablesList = new ArrayList<String>();
						if (divLnVariablesMap.containsKey(transactionLineItem
								.getDiv() + transactionLineItem.getLine())
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
									logger.info("Div is added.....  in variable List"+transactionLineItem.getDiv());
									foundVariablesList.add(var);
								}
							}
							if (divLnVariableCollection != null) {
								for (String var : divLnVariableCollection) {
									logger.info("Div is added..... in lnvariable List"+transactionLineItem.getDiv());
									foundVariablesList.add(var);
								}
							}
							transactionLineItem
									.setVariableList(foundVariablesList);
							lineItemList.add(transactionLineItem);
							logger.info("Line Items are added inside lineItemList......"+lineItemList.size());
						}
					
					}	
					}
				}
			}
			if (lineItemList != null && !lineItemList.isEmpty()) {/*
				List<Object> lineItemAsJsonString = new ArrayList<Object>();
				lineItemAsJsonString.add(l_id);
				lineItemAsJsonString
						.add(createJsonFromLineItemList(lineItemList));
				lineItemAsJsonString.add("NPOS");

				logger.info(" ************************** parsing bolt emitting: "
						+ lineItemAsJsonString.toString());
				// 8) EMIT LINE ITEMS
				if (lineItemAsJsonString != null
						&& !lineItemAsJsonString.isEmpty()) {
					this.outputCollector.emit(lineItemAsJsonString);
				}
			*/

	    		
	    		// 8) FOR EACH LINE ITEM FIND ASSOCIATED VARIABLES BY DIVISION AND LINE
	        	Map<String, String> varAmountMap = new HashMap<String, String>();
	        	List<Object> listToEmit = new ArrayList<Object>();
	        	for(TransactionLineItem lnItm : lineItemList) {
	        		List<String> varList = lnItm.getVariableList();
	        		if(varList == null || varList.isEmpty()) {
	        			continue;
	        		}
	        		for(String v : varList) {
	        			if(!varAmountMap.containsKey(v.toUpperCase())) {
	    	    			varAmountMap.put(v.toUpperCase(), String.valueOf(lnItm.getAmount()));
	        			}
	        			else {
	        				Double a1 = Double.valueOf(varAmountMap.get(v));
	        				a1 = a1 + lnItm.getAmount();
	        				varAmountMap.remove(v.toUpperCase());
	    	    			varAmountMap.put(v.toUpperCase(), String.valueOf(a1));
	        			}
	        		}
	    		}
	        	
				if("NPOS".equalsIgnoreCase(requestorID)){
			        listToEmit.add(l_id);
			        listToEmit.add(createJsonFromVarValueMap(varAmountMap));
			        listToEmit.add("NPOS");
			      //KPOS and KCOM
				}else if ("KCOM".equalsIgnoreCase(requestorID)){
					listToEmit.add(l_id);
			        listToEmit.add(createJsonFromVarValueMap(varAmountMap));
			        listToEmit.add("KCOM");
			    }else if ("KPOS".equalsIgnoreCase(requestorID)){
					listToEmit.add(l_id);
			        listToEmit.add(createJsonFromVarValueMap(varAmountMap));
			        listToEmit.add("KPOS");
			    }

		       logger.info(" *** parsing bolt emitting: " + listToEmit.toString());
		        
				// 9) EMIT VARIABLES TO VALUES MAP IN GSON DOCUMENT
		        if(listToEmit!=null && !listToEmit.isEmpty()) {
		        	this.outputCollector.emit(listToEmit);
		        }
	        }
		}
	}

	private Object createJsonFromVarValueMap(Map<String,String> varAmountMap) {
		// Create string in JSON format to emit

    	Gson gson = new Gson();
    	Type transLineItemType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;}.getType();
    	
    	
    	String transLineItemListString = gson.toJson(varAmountMap, transLineItemType);
		return transLineItemListString;
	}

	private Object createJsonFromLineItemList(
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

	public String getLineFromCollection(String div, String item) {
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

	public String getCategoryFromCollection(String div, String item) {
		logger.info("searching for category");

		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", div);
		queryLine.put("i", item);

		logger.info("query: " + queryLine);
		DBObject Ksndivcat = KsndivcatCollection.findOne(queryLine);
		logger.info("category: " + Ksndivcat);

		if (Ksndivcat == null || Ksndivcat.keySet() == null
				|| Ksndivcat.keySet().isEmpty()) {
			logger.info("Ksndivcat is null");
			return null;
		}
		String category = Ksndivcat.get("l").toString();
		 logger.info("  found category: " + category);
		return category;
	}

	private static String convertStreamToString(final Message jmsMsg)
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
