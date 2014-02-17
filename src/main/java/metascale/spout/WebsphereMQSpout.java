package metascale.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.ibm.jms.JMSMessage;
import com.ibm.mq.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Map;

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
	private static final Logger LOG = LoggerFactory.getLogger(WebsphereMQSpout.class);

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
			LOG.error("Exception occurred while establishing queue connection", e);
		}
	}

	@Override
	public void nextTuple() {
		try {
			JMSMessage receivedMessage = (JMSMessage) receiver.receive();
			collector.emit(new Values(receivedMessage), receivedMessage);
		} catch (JMSException e) {
			LOG.error("Exception occurred while receiving message from queue ", e);
		}
	}

	@Override
	public void ack(Object msgId) {
		//System.out.println("spout acked : " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		// do nothing for now
        //System.out.println("spout failed : " + msgId);

    }

	@Override
	public void close() {
		closeConnections(queueSession, receiver, queueConnection);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("npos"));
	}

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
			LOG.error("Exception occured while closing MQ session", e);
		}
		try {
			receiver.close();
		} catch (JMSException e) {
			LOG.error("Exception occured while closing MQ receiver", e);
		}
		try {
			connection.close();
		} catch (JMSException e) {
			LOG.error("Exception occured while closing MQ connection", e);
		}
	}
}
