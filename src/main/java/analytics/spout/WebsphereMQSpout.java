package analytics.spout;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ibm.jms.JMSBytesMessage;
import com.ibm.jms.JMSMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueReceiver;
import com.ibm.mq.jms.MQQueueSession;
public class WebsphereMQSpout extends BaseRichSpout {

	
	private SpoutOutputCollector collector;
	private MQQueueReceiver receiver;
	private MQQueueSession queueSession;
	private MQQueueConnection queueConnection;

	private String hostNanme;
	private int port;
	private String queueManager;
	private String queueChannel;
	private String queueName;

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(WebsphereMQSpout.class);

	/**
	 * Constructor
	 * 
	 * @param hostName
	 *           - host name
	 * @param port
	 *           - port to connect to
	 * @param queueManager
	 *           - queue mnanager name
	 * @param queueChannel
	 *           - queue channel name
	 * @param queueName
	 *           - queue name
	 */
	public WebsphereMQSpout(final String hostName, final int port, final String queueManager, final String queueChannel,
	      final String queueName) {
		this.hostNanme = hostName;
		this.port = port;
		this.queueManager = queueManager;
		this.queueChannel = queueChannel;
		this.queueName = queueName;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		LOGGER.info("Spout connecting to MQ queue");
		try {
			this.collector = collector;
			MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
			cf.setHostName(hostNanme);
			cf.setPort(port);
			cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
			cf.setQueueManager(queueManager);
			cf.setChannel(queueChannel);

			queueConnection = (MQQueueConnection) cf.createQueueConnection();
			queueSession = (MQQueueSession) queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			MQQueue queue = (MQQueue) queueSession.createQueue(queueName);
			receiver = (MQQueueReceiver) queueSession.createReceiver(queue);
			queueConnection.start();
		} catch (JMSException e) {
			LOGGER.error("Exception occurred while establishing queue connection", e);
		}
	}

	@Override
	public void nextTuple() {
		LOGGER.debug("Fetching a message from MQ");
		try {
			JMSMessage receivedMessage = (JMSMessage) receiver.receive();
			String messageID = receivedMessage.getJMSMessageID();
			LOGGER.info("TIME:" + messageID + "-Entering spout-" + System.currentTimeMillis());
			String transactionXmlString = getTransactionString(receivedMessage);
			collector.emit(new Values(transactionXmlString,messageID), (messageID + " xml: " + transactionXmlString));
			LOGGER.info("PERSIST: incoming tuples in spout from MQ TELLURIDE");
		} catch (JMSException e) {
			LOGGER.error("Exception occurred while receiving message from queue ", e);
		}
	}
	
	private String getTransactionString(JMSMessage receivedMessage){
		String transactionXmlAsString = "";
		  try {
		        if (receivedMessage instanceof JMSBytesMessage)
		            transactionXmlAsString = convertStreamToString(receivedMessage);
		    } catch (JMSException e) {
		        LOGGER.error("Unable to read message from MQ", e);
		    } catch (Exception e) {
		        LOGGER.error("Unable to read message from MQ", e);
		    }
		  return transactionXmlAsString;
	}

	/*private void logAllTransaction(String xmlString) {
		
		ProcessTransaction processTransaction = null;
 	    processTransaction = parseXMLAndExtractProcessTransaction(processTransaction, xmlString);
 	    if(processTransaction != null){
	    	String memberNumber = (processTransaction.getMemberNumber() != null) ? processTransaction.getMemberNumber() : "NONE";
	    	String pickUpStoreNumber = (processTransaction.getOrderStoreNumber() != null) ? processTransaction.getOrderStoreNumber() : "NONE";
	    	String tenderStoreNumber = (processTransaction.getTenderStoreNumber() != null) ? processTransaction.getTenderStoreNumber() : "NONE";
	    	String orderStoreNumber = (processTransaction.getOrderStoreNumber() != null) ? processTransaction.getOrderStoreNumber() : "NONE";
	        String registerNumber = (processTransaction.getRegisterNumber() != null) ? processTransaction.getRegisterNumber() : "NONE";
	        String transactionNumber = (processTransaction.getTransactionNumber() != null) ? processTransaction.getTransactionNumber() : "NONE";
	        String transactionTime = (processTransaction.getTransactionTime() != null) ? processTransaction.getTransactionTime() : "NONE";
	        String requestorId = (processTransaction.getRequestorID() != null) ? processTransaction.getRequestorID() : "NONE";
	        String earnFlag = (processTransaction.getEarnFlag() != null) ? processTransaction.getEarnFlag() : "NONE";
	        LOGGER.info("PERSIST: " + memberNumber +", " + pickUpStoreNumber + ", " + tenderStoreNumber +", " + orderStoreNumber + ", " + registerNumber +", " + transactionNumber +", " + transactionTime +", " + requestorId +", " + earnFlag + ", allTransactions in MQspout from MQQueue");
 	    }
	}*/

	@Override
	public void fail(Object msgId) {
	       LOGGER.info("PERSIST: Telluride Spout Failed : " + msgId);
    }
	
	@Override
	public void ack(Object msgId) {
	}


	@Override
	public void close() {
		closeConnections(queueSession, receiver, queueConnection);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("npos","messageID"));
	}

	
	 private final static String convertStreamToString( Message jmsMsg)
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
	        return stringMessage;
	    }
	 
	  /*private ProcessTransaction parseXMLAndExtractProcessTransaction(ProcessTransaction processTransaction, String transactionXmlAsString) {
	        LOGGER.debug("Parsing MQ message XML");
	        if ((transactionXmlAsString.contains("<ProcessTransaction") || transactionXmlAsString.contains(":ProcessTransaction")) && !transactionXmlAsString.contains("AnswerTxt")) {

	            processTransaction = XMLParser
	                    .parseXMLProcessTransaction(transactionXmlAsString);
	        }
	        return processTransaction;
	  }*/
	/**
	 * Close connections
	 * 
	 * @param session
	 * @param receiver
	 * @param connection
	 */
	private static void closeConnections(final MQQueueSession session, final MQQueueReceiver receiver, final MQQueueConnection connection) {
		try {
			session.close();
		} catch (JMSException e) {
			LOGGER.error("Exception occured while closing MQ session", e);
		}
		try {
			receiver.close();
		} catch (JMSException e) {
			LOGGER.error("Exception occured while closing MQ receiver", e);
		}
		try {
			connection.close();
		} catch (JMSException e) {
			LOGGER.error("Exception occured while closing MQ connection", e);
		}
	}
}
