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

import analytics.util.Constants;
import analytics.util.HostPortUtility;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.XMLParser;
import analytics.util.dao.DivCatKsnDao;
import analytics.util.dao.DivCatVariableDao;
import analytics.util.dao.DivLnItmDao;
import analytics.util.dao.DivLnVariableDao;
import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;
import analytics.util.objects.TransactionLineItem;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ibm.jms.JMSBytesMessage;
import com.ibm.jms.JMSMessage;

public class TellurideParsingBoltPOS extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TellurideParsingBoltPOS.class);
	/**
	 * Created by Devarshi Das 8/27/2014
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
    private DivLnVariableDao divLnVariableDao;
    private DivLnItmDao divLnItmDao;
    private DivCatVariableDao divCatVariableDao;
    private DivCatKsnDao divCatKsnDao;
	private Map<String, List<String>> divLnVariablesMap;
	private Map<String, List<String>> divCatVariablesMap;
	private String requestorID = "";
	private MultiCountMetric countMetric;
	public void setOutputCollector(OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
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
	     initMetrics(context);
     //   System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
	     HostPortUtility.getInstance(stormConf.get("nimbus.host").toString());
		LOGGER.info("Preparing telluride parsing bolt");

		LOGGER.debug("Getting mongo collections");
		LOGGER.trace("Populate div line variables map");
		divLnItmDao = new DivLnItmDao();
		divCatKsnDao = new DivCatKsnDao();
		divLnVariableDao = new DivLnVariableDao();
		divCatVariableDao = new DivCatVariableDao();
		
		// populate divLnVariablesMap
		divLnVariablesMap = divLnVariableDao.getDivLnVariable();

		LOGGER.trace("Populate div cat variables map");
		//populate divCatVariablesMap
		divCatVariablesMap = divCatVariableDao.getDivCatVariable();
	}

	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, Constants.METRICS_INTERVAL);
	    }
	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		if(LOGGER.isDebugEnabled())
			LOGGER.debug("The time it enters inside Telluride parsing bolt execute method"+System.currentTimeMillis()+" and the message ID is ..."+input.getMessageId());
		countMetric.scope("incoming_tuples").incr();
		String lyl_id_no = "";
		ProcessTransaction processTransaction = null;
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering parsing bolt-" + System.currentTimeMillis());
		String transactionXmlAsString = "";
		// KPOS and KCOM
		JMSMessage documentNPOS = (JMSMessage) input.getValueByField("npos");

		try {
			if(documentNPOS instanceof JMSBytesMessage)
				transactionXmlAsString = convertStreamToString(documentNPOS);
		} catch (JMSException e) {
			LOGGER.error("Unable to read message from MQ",e);
		} catch (Exception e) {
			LOGGER.error("Unable to read message from MQ",e);
		}
		if ( StringUtils.isEmpty(transactionXmlAsString)) {
			countMetric.scope("empty_message").incr();
			outputCollector.ack(input);
			return;
		}

		LOGGER.debug("Parsing MQ message XML");
		if (transactionXmlAsString.contains("<ProcessTransaction")) {

			//logger.info("Processing Soap Envelop xml String...");
			processTransaction = XMLParser
					.parseXMLProcessTransaction(transactionXmlAsString);
		} else if (transactionXmlAsString.contains("tns:ProcessTransaction")) {
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
			LOGGER.debug("Requestor ID is..." + requestorID);
		} else {
			LOGGER.debug("Requestor ID is null....");
		}
		/*
		 * if (requestorID == null) { return; }
		 */
		if (processTransaction != null
				&& !"".equalsIgnoreCase(processTransaction
						.getTransactionNumber())) {

			lyl_id_no = processTransaction.getMemberNumber();


			if (lyl_id_no==null || StringUtils.isEmpty(lyl_id_no)) {
				countMetric.scope("empty_lid").incr();
				outputCollector.ack(input);
				return;
			}
			//TODO: uncomment this to debug
			/*if(lyl_id_no.equals("7081057588230760") || lyl_id_no.equals("7081400000032721")|| lyl_id_no.equals("7081187618793758") || lyl_id_no.equals("7081257366894445") 
					|| lyl_id_no.equals("7081133318057649") || lyl_id_no.equals("7081020830587635")){
                LOGGER.error("Received loyalty id" + lyl_id_no);
                LOGGER.error("XML for transaction = " + transactionXmlAsString);
          	}*/
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
			if(LOGGER.isTraceEnabled()){
				String lineItems_toString = null;
				if(lineItems != null)
					lineItems_toString = lineItems.toString();
				LOGGER.trace("Line Items are .."+ lineItems_toString);
			}
			if (lineItems != null && lineItems.size() != 0) {
				for (LineItem lineItem : lineItems) {

					String item = "";
					String amount = lineItem.getDollarValuePostDisc();
					/*logger.info("Item is...." + item + "...Amount is...."
							+ amount);*/
					if (amount.contains("-")) {
						LOGGER.debug("amount_contains -");
						continue;
					} else {
						// KPOS and KCOM
						LOGGER.debug("KPOS or KCOM transaction processing");
						if ("KPOS".equalsIgnoreCase(requestorID)
								|| "KCOM".equalsIgnoreCase(requestorID)) {
							item = lineItem.getItemNumber();
							//logger.info("Item is..."+item);
							String divCategory = getDivCategoryFromCollection(item);
							if(divCategory==null||divCategory.length()==0)
							{
								LOGGER.error("Unable to find div cat information for" + item);
								continue;
							}
							//logger.info("division and category are ...." + divCategory);
                            String div = StringUtils.substring(divCategory, 0, 3);//Picks up start, end-1
                            String cat = StringUtils.substring(divCategory, 3, 7);
                            TransactionLineItem transactionLineItem = new TransactionLineItem(
									l_id, div, item, cat,
									Double.valueOf(amount));
							/*logger.info("Transaction Line Item is ..."
									+ transactionLineItem);*/

							// find all variables affected by div-line
							List<String> foundVariablesList = new ArrayList<String>();
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
							LOGGER.debug("Sears transaction processing");
							if (lineItem.getItemNumber()!=null && lineItem.getItemNumber().length() >= 6) {

								item = lineItem.getItemNumber().substring(
										lineItem.getItemNumber().length() - 5);

							} else {
								item = lineItem.getItemNumber();
							}
                            String div = lineItem.getDivision();

                    		String line = divLnItmDao.getLnFromDivItem(div, item);
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
					listToEmit.add(messageID);
					LOGGER.debug(requestorID + " Point of SALE is touched...");
					LOGGER.debug(" *** telluride parsing bolt emitting: "
						+ listToEmit.toString());
					countMetric.scope("successful").incr();
					outputCollector.ack(input);
				// 9) EMIT VARIABLES TO VALUES MAP IN JSON DOCUMENT
				if (listToEmit != null && !listToEmit.isEmpty()) {
					this.outputCollector.emit(listToEmit);
					LOGGER.info("TIME:" + messageID + "-Emiting from parsing bolt-" + System.currentTimeMillis());
				}
			}
			else{
				countMetric.scope("empty_line_item").incr();
				outputCollector.ack(input);
				return;
			}
		}
		else{
			countMetric.scope("empty_xml").incr();
			outputCollector.ack(input);
			return;
		}
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

	private final String getDivCategoryFromCollection(String item) {
		LOGGER.debug("searching for category");
		DivCatKsnDao.DivCat divCat = divCatKsnDao.getVariableFromTopic(item);

		if(divCat==null)
			return null;
		String category = divCat.getCat();
        String div = divCat.getDiv();

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
