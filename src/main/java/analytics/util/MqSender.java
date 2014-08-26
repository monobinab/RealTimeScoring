package analytics.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.log4j.Logger;

import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueReceiver;
import com.ibm.mq.jms.MQQueueSender;
import com.ibm.mq.jms.MQQueueSession;

public class MqSender {
	private static final Logger logger = org.apache.log4j.Logger
			.getLogger(MqSender.class);

	static int counter = 0;
	public static void initJMS() {
		try {
			MQQueueConnectionFactory cf1 = new MQQueueConnectionFactory();
			MQQueueConnectionFactory cf2 = new MQQueueConnectionFactory();
			
			cf1.setHostName("hofdvmq1.searshc.com");
			cf1.setPort(1415);
			cf1.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

			cf1.setQueueManager("SQAT0001");
			cf1.setChannel("PROCTRAN.SVRCONN");

			cf2.setHostName("hofdvmq2.searshc.com");
			cf2.setPort(1415);
			cf2.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

			cf2.setQueueManager("SQAT9999");
			cf2.setChannel("PROCTRAN.SVRCONN");
			
			
			MQQueueConnection connection = (MQQueueConnection) cf1
					.createQueueConnection();
			MQQueueSession session = (MQQueueSession) connection
					.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
			MQQueue queue = (MQQueue) session
					.createQueue("MDS0.PROCTRAN.TO.STORM.QC01");
			
            MQQueueConnection connection2 = (MQQueueConnection)
                    cf2.createQueueConnection();
            MQQueueSession session2 = (MQQueueSession)
                    connection2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        	MQQueue queue2 = (MQQueue) session2
					.createQueue("MDS0.PROCTRAN.TO.STORM.QC01");
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
			while ((sCurrentLine = br.readLine()) != null && counter<=10) {
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
	static String readFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return encoding.decode(ByteBuffer.wrap(encoded)).toString();
			}
}