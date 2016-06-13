package analytics.bolt;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.XMLParser;
import analytics.util.dao.DivCatKsnDao;
import analytics.util.dao.DivCatVariableDao;
import analytics.util.dao.DivLnItmDao;
import analytics.util.dao.DivLnVariableDao;
import analytics.util.objects.DivCatLineItem;
import analytics.util.objects.LineItem;
import analytics.util.objects.ProcessTransaction;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import javax.jms.BytesMessage;
import javax.jms.Message;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class TellurideParsingBoltPOS extends EnvironmentBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(TellurideParsingBoltPOS.class);

    private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    private DivLnVariableDao divLnVariableDao;
    private DivLnItmDao divLnItmDao;
    private DivCatVariableDao divCatVariableDao;
    private DivCatKsnDao divCatKsnDao;
    private String host;
    private int port;

    public TellurideParsingBoltPOS(String systemProperty) {
        super(systemProperty);

    }

    public TellurideParsingBoltPOS(String systemProperty, String host, int port) {
        super(systemProperty);
        this.host = host;
        this.port = port;
    }

    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.outputCollector = collector;
        LOGGER.info("Preparing telluride parsing bolt");
        divLnItmDao = new DivLnItmDao();
        divCatKsnDao = new DivCatKsnDao();
        divLnVariableDao = new DivLnVariableDao();
        divCatVariableDao = new DivCatVariableDao();
    }

    @Override
    public void execute(Tuple input) {
    	long time = System.currentTimeMillis();
    	String lyl_id_no = "";
    	try{
	    	redisCountIncr("incoming_tuples");
     
	        ProcessTransaction processTransaction = null;
	        String messageID = "";
	        
	        if (input.contains("messageID")) {
	            messageID = input.getStringByField("messageID");
	        }
	        
	        //This stringBuilder is to populate div and lines and send purchase email to Responsys, and NOT involved in scoring
	        StringBuilder divLineBuff = new StringBuilder();
	        
	        //STEP 1: extract the required xml
	        String transactionXmlAsString = extractTransactionXml(input);
	    	
	        //STEP 2: Parse the xml and populate the ProcessTransaction DTO
	        processTransaction = parseXMLAndExtractProcessTransaction(processTransaction, transactionXmlAsString);
	     
	        //STEP 3: If NOT an empty xml, get the lineITems with all fields populated for valid transactions
	        if(processTransaction != null && processTransaction.getEarnFlag() != null ){
	       
	        	if ( processTransaction.getEarnFlag().equalsIgnoreCase("E")) {
	        		//Log the valid transactions
	        		logTransaction(processTransaction);
		         	redisCountIncr("valid_transactions");
		         	lyl_id_no = processTransaction.getMemberNumber();
		
		            if (lyl_id_no == null || StringUtils.isEmpty(lyl_id_no)) {
		                redisCountIncr("empty_lid");
		                outputCollector.ack(input);
		                return;
		            }
		           
		            String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
		            
		            List<LineItem> lineItemList = processTransaction.getLineItemList();
		            
		            if(lineItemList == null || lineItemList.size() == 0 || lineItemList.isEmpty()){
		            	 redisCountIncr("empty_line_item");
		            	 LOGGER.info("PERSIST: " + lyl_id_no + " not emitted to StrategyScoring bolt from TellurideParsing bolt and acked for empty lineItem " );
			             outputCollector.ack(input);
			             return;
		            }
		            //get list of lineItems with line/category and associated variables populated
		            getPopulatedLineItemsList(lyl_id_no, lineItemList, processTransaction.getRequestorID(), messageID, l_id, divLineBuff);
		            
		            //set the value for each variable with amount and poulate the varAmount map
		            Map<String, String> varValueMap = getVariablesValueMap(lineItemList);
		            if(varValueMap != null && !varValueMap.isEmpty() && varValueMap.size() > 0){
		            	emitToScoring(lyl_id_no, processTransaction.getRequestorID(), messageID, l_id, varValueMap );
		            	outputCollector.ack(input); 
		            }
		         	LOGGER.info("Time taken for Telluride Parsing Bolt: " + (System.currentTimeMillis() - time));
		        } 
	        	else {
		            redisCountIncr("invalid_transactions");
		            LOGGER.info("PERSIST: " + lyl_id_no + " not emitted to StrategyScoring bolt from TellurideParsing bolt and acked for invalid transaction " );
		            outputCollector.ack(input);
		            return;
		        }
	        } 
	        else {
	            redisCountIncr("empty_xml");
	            LOGGER.info("PERSIST: " + lyl_id_no + " not emitted to StrategyScoring bolt from TellurideParsing bolt and acked for empty xml " );
	            outputCollector.ack(input);
	            return;
	        }
	          
	        //Adding the Div Line information to Redis to send RTS_purchase e-mail
	        if (divLineBuff != null && divLineBuff.toString().length() > 0) {
	            //persisting the loyalty id and div lines to redis for sending RTS_Purchase e-mails to customers
	            try {
	                writeToRedis(lyl_id_no, divLineBuff);
	            } catch (Exception e) {
	                LOGGER.error("Exception Occurred Writing to Redis for Lid " + lyl_id_no + " with divLines " + divLineBuff);
	            }
	        }
    	}
    	catch(Exception e){
    		LOGGER.error("Exception in TellurideParsingbolt for loyalty id " + lyl_id_no + "exception: " + e.getStackTrace() + "cause: " + ExceptionUtils.getRootCauseMessage(e));
    	}
         outputCollector.ack(input);
         LOGGER.info("PERSIST: " + lyl_id_no + " acked successfully in TellurideParsing bolt " );
     }

    
	private void emitToScoring( String lyl_id_no, String requestorId, String messageID, String l_id, Map<String, String> varValueMap) {
		List<Object> listToEmit = new ArrayList<Object>();
	    listToEmit.add(l_id);
	    listToEmit.add(JsonUtils.createJsonFromStringStringMap(varValueMap));
	    listToEmit.add(requestorId);
	    listToEmit.add(messageID);
	    listToEmit.add(lyl_id_no);
	    redisCountIncr("successful");
	    if (listToEmit != null && !listToEmit.isEmpty()) {
	        this.outputCollector.emit(listToEmit);
	        LOGGER.info("PERSISI: " + lyl_id_no + " emitted to StrategyScoring bolt from TellurideParsing bolt " + requestorId);
        }
	}
    

	private Map<String, String> getVariablesValueMap(List<LineItem> lineItemList) {
		Map<String, String> varAmountMap = null;
		if (lineItemList != null && !lineItemList.isEmpty()) {
			varAmountMap = new HashMap<String, String>();
		    for( LineItem lnItm : lineItemList) {
		        List<String> varList = lnItm.getVariablesList();
		        if (varList == null || varList.isEmpty()) {
		        	LOGGER.info("PERSIST : varList is empty");
		            continue;
		        }
		        for (String v : varList) {
		            if (!varAmountMap.containsKey(v.toUpperCase())) {
		                varAmountMap.put(v.toUpperCase(),
		                        String.valueOf(lnItm.getDollarValuePostDisc()));
		            } else {
		                Double a1 = Double.valueOf(varAmountMap.get(v));
		                a1 = a1 + Double.valueOf(lnItm.getDollarValuePostDisc());
		                varAmountMap.remove(v.toUpperCase());
		                varAmountMap.put(v.toUpperCase(),
		                        String.valueOf(a1));
		            }
		        }
		    }
		}
		return varAmountMap;
	}

	protected String extractTransactionXml(Tuple input) {
		String transactionXmlAsString = null;
		if(input.contains("npos")){
			transactionXmlAsString = (String) input.getValueByField("npos");
		}
		else{
			transactionXmlAsString = (String) input.getValue(0);
			LOGGER.info("from kafka " + transactionXmlAsString );
		}
		return transactionXmlAsString;
	}

	private void logTransaction(ProcessTransaction processTransaction) {
		String memberNumber = (processTransaction.getMemberNumber() != null) ? processTransaction.getMemberNumber() : "NONE";
		String pickUpStoreNumber = (processTransaction.getOrderStoreNumber() != null) ? processTransaction.getOrderStoreNumber() : "NONE";
		String tenderStoreNumber = (processTransaction.getTenderStoreNumber() != null) ? processTransaction.getTenderStoreNumber() : "NONE";
		String orderStoreNumber = (processTransaction.getOrderStoreNumber() != null) ? processTransaction.getOrderStoreNumber() : "NONE";
		String registerNumber = (processTransaction.getRegisterNumber() != null) ? processTransaction.getRegisterNumber() : "NONE";
		String transactionNumber = (processTransaction.getTransactionNumber() != null) ? processTransaction.getTransactionNumber() : "NONE";
		String requestorId = (processTransaction.getRequestorID() != null) ? processTransaction.getRequestorID() : "NONE";
		String transactionTime = (processTransaction.getTransactionTime() != null) ? processTransaction.getTransactionTime() : "NONE";
		String earnFlag = (processTransaction.getEarnFlag() != null) ? processTransaction.getEarnFlag() : "NONE";
		List<LineItem> lineItemList = processTransaction.getLineItemList();
		for(LineItem lineItem : lineItemList){
			String division = lineItem.getDivision();
			String itemNumber = lineItem.getItemNumber();
			String value = lineItem.getDollarValuePostDisc();
			logPersist(memberNumber, pickUpStoreNumber, tenderStoreNumber,
					orderStoreNumber, registerNumber, transactionNumber, requestorId,
					transactionTime, division, itemNumber, value, earnFlag, "MQQueue");
		}
	}

	public void logPersist(String memberNumber, String pickUpStoreNumber,
			String tenderStoreNumber, String orderStoreNumber,
			String registerNumber, String transactionNumber,
			String requestorId, String transactionTime, String division, String itemNumber, String value, String earnFlag, String queueType) {
		LOGGER.info("PERSIST: " + memberNumber +", " + pickUpStoreNumber + ", " + tenderStoreNumber +", " + orderStoreNumber + ", " + registerNumber +", " + transactionNumber +", " + requestorId +", " + transactionTime + ", " +", " + division + ", " + itemNumber + ", " + value + ", " + earnFlag +", "+ queueType );
	}

    private void getPopulatedLineItemsList(String lyl_id_no,  List<LineItem> lineItemsList, String requestorId, String messageID, String l_id, StringBuilder divLineBuff) {
        Map<String, List<String>> divCatVariablesMap = divCatVariableDao.getDivCatVariable();
        Map<String, List<String>> divLnVariablesMap = divLnVariableDao.getDivLnVariable();
     
        if(divCatVariablesMap != null && divCatVariablesMap.size() > 0 && divLnVariablesMap != null && divLnVariablesMap.size() > 0){
	        //HANDLING KMART TRANSACTIONS       
	      	if ("KPOS".equalsIgnoreCase(requestorId)
	                    || "KCOM".equalsIgnoreCase(requestorId)) {
	      		
	      		getLineItemsListKmart(divCatVariablesMap, lineItemsList);
	        }
	      	
	      	//HANDLING SEARS TRANSACTIONS
	      	else{
	      		getLineItemsListSears(lineItemsList, divLnVariablesMap, divLineBuff);
	      	}
         }
      }

    
    
	private void getLineItemsListSears(List<LineItem> lineItemsList, Map<String, List<String>> divLnVariablesMap, StringBuilder divLineBuff) {
		LOGGER.debug("Sears transaction processing");
		/*
		 * fetch the list of items and divisions from all the incoming lineItems
		 */
		List<String> divItemsList = new ArrayList<String>();
		for(LineItem lineItem : lineItemsList){
			String item = "";
			if (lineItem.getItemNumber() != null ){
				if (lineItem.getItemNumber().length() >= 6) {
					item = lineItem.getItemNumber().substring(lineItem.getItemNumber().length() - 5);
					lineItem.setItemNumber(item);
				}
				else{
					item = lineItem.getItemNumber();
				}
				String div = lineItem.getDivision();
				divItemsList.add(item + "," + div);
			}
		}
		
		/**
		 * get the line for every lineItem by passing division and item to the corresponding mongo
		 * and populate every lineItem with line
		 */
		List<DivCatLineItem> divLineItemsList = divLnItmDao.getDivCatLineFromDivItem(divItemsList);
		if(divLineItemsList != null && !divLineItemsList.isEmpty() && divLineItemsList.size() > 0){
			for(DivCatLineItem divLineCatItem : divLineItemsList){
				for(LineItem lineItem : lineItemsList){
					if(lineItem.getItemNumber().equalsIgnoreCase(divLineCatItem.getItem()) &&
							lineItem.getDivision().equalsIgnoreCase(divLineCatItem.getDiv())){
						lineItem.setLineNumber(divLineCatItem.getLine());
						divLineBuff.append(divLineCatItem.getDiv() + divLineCatItem.getLine() + "~");
						break;
					}
				}
			}
			
		}
		
		/**
		 * fetching the list of variables from div/div+line
		 * and populate every lineItem 
		 */
		for(LineItem lineItem : lineItemsList){
			List<String> variablesList = new ArrayList<String>();
			if(divLnVariablesMap != null && divLnVariablesMap.size() > 0){
				if((divLnVariablesMap.containsKey(lineItem.getDivision()))){
					List<String> divVariableList = divLnVariablesMap.get(lineItem.getDivision());
					if(divVariableList != null && !divVariableList.isEmpty()){
						variablesList.addAll(divVariableList);
					}
				}
				if(lineItem.getLineNumber() != null  &&
						divLnVariablesMap.containsKey(lineItem.getDivision() + lineItem.getLineNumber()) ){
					List<String> divLineVariableList = divLnVariablesMap.get(lineItem.getDivision()+ lineItem.getLineNumber());
					if(divLineVariableList != null && !divLineVariableList.isEmpty()){
						variablesList.addAll(divLineVariableList);
					}
				}
				lineItem.setVariablesList(variablesList);
			}
		}
	}

	
	
	private List<LineItem> getLineItemsListKmart(Map<String, List<String>> divCatVariablesMap, List<LineItem> lineItemsList) {
		
		//fetch the list of items from all the incoming lineItems
		List<String> itemsList = new ArrayList<String>();
		for (LineItem lineItem : lineItemsList) {
		 	String amount = lineItem.getDollarValuePostDisc();
		 	
		 	/**
		 	 * if "dollarValuePostDisc" in the incoming xml contains a "-", skip the lineitem
		 	 */
		 	if (amount.contains("-")) {
		         LOGGER.debug("amount_contains -");
		         continue;
		     }
		 	itemsList.add(lineItem.getItemNumber());
		 }
		
		/**
		 * get the category for every lineItem by passing list of items
		 * and populate every lineItem with category
		 */
		List<DivCatLineItem> divCatItemsList = divCatKsnDao.getDivCatItemFromItems(itemsList);
		if(divCatItemsList != null && divCatItemsList.size() > 0 && !divCatItemsList.isEmpty()){
			for(DivCatLineItem divCatItem : divCatItemsList){
				for(LineItem lineItem : lineItemsList){
					if(lineItem.getDivision().equalsIgnoreCase(divCatItem.getDiv()) &&
							lineItem.getItemNumber().equalsIgnoreCase(divCatItem.getItem())){
						lineItem.setCategory(divCatItem.getCat());
						break;
					}
				}
			}
		}
		/**
		 * fetching the list of variables from div/div+cat
		 * and populate every lineItem 
		 */
		for(LineItem lineItem : lineItemsList){
			List<String> variablesList = new ArrayList<String>();
			List<String> divCatVariableList = null;
			List<String> divVariableList = null;
			if(divCatVariablesMap != null && divCatVariablesMap.size() > 0 ){
				if(divCatVariablesMap.containsKey(lineItem.getDivision())){
					divVariableList = divCatVariablesMap.get(lineItem.getDivision());
				}
					if(lineItem.getCategory() != null  && divCatVariablesMap.containsKey(lineItem.getDivision() + lineItem.getCategory())){
						divCatVariableList = divCatVariablesMap.get(lineItem.getDivision()+ lineItem.getCategory());
					}
					if(divVariableList != null && !divVariableList.isEmpty()){
						variablesList.addAll(divVariableList);
					}
					if(divCatVariableList != null && !divCatVariableList.isEmpty()){
						variablesList.addAll(divCatVariableList);
					}
					lineItem.setVariablesList(variablesList);
			}
		}
		return lineItemsList;
	}

	
    private void writeToRedis(String lyl_id_no, StringBuilder divLineBuff) {
        Jedis jedis = new Jedis(host, port, 1800);
        jedis.connect();
        jedis.set("Pos:" + lyl_id_no, divLineBuff.toString());
        jedis.disconnect();
    }

    
    private ProcessTransaction parseXMLAndExtractProcessTransaction(ProcessTransaction processTransaction, String transactionXmlAsString) {
        LOGGER.debug("Parsing MQ message XML");
        if ((transactionXmlAsString.contains("<ProcessTransaction") || transactionXmlAsString.contains(":ProcessTransaction")) ) {

            processTransaction = XMLParser
                    .parseXMLProcessTransaction(transactionXmlAsString);
        }
        return processTransaction;
    }

    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source", "messageID", "lyl_id_no"));
    }


    @SuppressWarnings("unused")
	private final static String convertStreamToString(final Message jmsMsg)throws Exception {
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
        return stringMessage;
    }

}
