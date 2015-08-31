package analytics.bolt;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

public class TellurideKafkaParsingBoltPOS extends TellurideParsingBoltPOS {
	
	public TellurideKafkaParsingBoltPOS(String systemProperty) {
		super(systemProperty);
	}
	
	public TellurideKafkaParsingBoltPOS(String systemProperty, String host, int port){
		super(systemProperty, host, port);
	}
	
	private static final Logger LOGGER = LoggerFactory
            .getLogger(TellurideKafkaParsingBoltPOS.class);
    
	 private static final long serialVersionUID = 1L;
	 
	
 	@Override
	protected String extractTransactionXml(Tuple input) {
		 String message = (String) input.getValueByField("str");
		// System.out.println(message);
      	
         //check the incoming string for <ProcessTransaction or :ProcessTransaction as it contains the member's transaction data
 			if (message.contains("<ProcessTransaction") || message.contains(":ProcessTransaction")) {
 				JSONObject obj;
				try {
					obj = new JSONObject(message);
					message = (String) obj.get("xmlReqData");
				//	System.out.println(message);
				} catch (JSONException e) {
					LOGGER.error("Exception in json parsing ", e);
					e.printStackTrace();
				}
								
 			}
			return message;
 	}


	@Override
	public void logPersist(String memberNumber, String pickUpStoreNumber,
			String tenderStoreNumber, String orderStoreNumber,
			String registerNumber, String transactionNumber,
			String transactionTime, String queueType) {
		
		super.logPersist(memberNumber, pickUpStoreNumber, tenderStoreNumber,
				orderStoreNumber, registerNumber, transactionNumber, transactionTime, "kafka");
	}

    

}
