package analytics.util;

import com.ibm.mq.jms.*;
import org.apache.log4j.Logger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Session;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;

public class MqSender {
	private static final Logger logger = org.apache.log4j.Logger
			.getLogger(MqSender.class);

	static int counter = 0;
	public static void initJMS() {
		try {
			MQQueueConnectionFactory cf1 = new MQQueueConnectionFactory();
			MQQueueConnectionFactory cf2 = new MQQueueConnectionFactory();
			
			MQConnectionConfig mqConnection = new MQConnectionConfig();
			WebsphereMQCredential mqCredential = mqConnection
					.getWebsphereMQCredential();
			cf1.setHostName(mqCredential.getHostOneName());
			cf1.setPort(mqCredential.getPort());
			cf1.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

			cf1.setQueueManager(mqCredential.getQueueOneManager());
			cf1.setChannel(mqCredential.getQueueChannel());

			cf2.setHostName(mqCredential.getHostTwoName());
			cf2.setPort(mqCredential.getPort());
			cf2.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

			cf2.setQueueManager(mqCredential
					.getQueueTwoManager());
			cf2.setChannel(mqCredential
					.getQueueChannel());
			
			
			MQQueueConnection connection = (MQQueueConnection) cf1
					.createQueueConnection();
			MQQueueSession session = (MQQueueSession) connection
					.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
			MQQueue queue = (MQQueue) session
					.createQueue(mqCredential.getQueueName());
			
            MQQueueConnection connection2 = (MQQueueConnection)
                    cf2.createQueueConnection();
            MQQueueSession session2 = (MQQueueSession)
                    connection2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        	MQQueue queue2 = (MQQueue) session2
					.createQueue(mqCredential.getQueueName());
            MQQueueSender sender = (MQQueueSender) session.createSender(queue);
            MQQueueSender sender2 = (MQQueueSender) session2.createSender(queue2);
            
			
			MQQueueReceiver receiver = (MQQueueReceiver)session2.createReceiver(queue);
            
			javax.jms.TemporaryQueue temporaryQueue = session2.createTemporaryQueue();
			// Listen to the temporary reply queue for messages returned by the scoring service
			javax.jms.MessageConsumer consumer = session2.createConsumer(temporaryQueue);

			String sCurrentLine;
			BufferedReader br = null; 
			Charset charset = Charset.forName("UTF-8");
			br = new BufferedReader(new FileReader("./src/main/resources/PROCTRAN3.txt"));
			while ((sCurrentLine = br.readLine()) != null && counter<=1) {
				BytesMessage message = (BytesMessage)session.createBytesMessage();
				message.writeBytes(sCurrentLine.getBytes("UTF-8"));
				logger.info(sCurrentLine);
				sender.send(message);
				sender2.send(message);
				counter++;
			}
			logger.info("SUCCESS");
			// Receive the reply message.
			// NOTE: This method blocks until a message is received.
			
			/*Message replyJMSMessage = consumer.receive();
			// The message format should be a bytes message.
			if (replyJMSMessage != null && replyJMSMessage instanceof javax.jms.BytesMessage)
			{
			    javax.jms.BytesMessage bytesMessage = (javax.jms.BytesMessage) replyJMSMessage;
			    byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
			    bytesMessage.readBytes(bytes);
			    logger.info("Reply Message");
			    // the reply message
			    String replyMessage = new String(bytes, "UTF-8");
			    logger.info("   " + replyMessage);
			    // the JMS correlation ID can be used to match a sent message with a response message 
			    String jmsCorrelationID = replyJMSMessage.getJMSCorrelationID();
			    logger.info("   reply message ID = " + jmsCorrelationID);
			}*/
			// After the message is sent, get the message ID.
			// You would keep the message ID around somewhere so you can match it to a reply later.
			//String messageID = message.getJMSMessageID();
	       
			sender.close();

			// Cleanup
			session.close();
			session2.close();
			connection.stop();
			
			
		} catch (JMSException jmsex) {
			logger.info(jmsex);
			logger.info("FAILURE");
		} catch (Exception ex) {
			logger.info(ex);
			logger.info("FAILURE");
		}

	}

}