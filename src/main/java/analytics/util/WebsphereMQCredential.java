package analytics.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebsphereMQCredential {
	public static final Logger LOGGER = LoggerFactory.getLogger(WebsphereMQCredential.class);
	
	private String hostOneName;
	private String hostTwoName;
	private String hostRtsThreeName;
	private String hostRtsFourName;
	private int port;
	private String queueOneManager;
	private String queueTwoManager;
	private String queueRtsThreeManager;
	private String queueRtsFourManager;
	


	public String getQueueRts2Channel() {
		return queueRts2Channel;
	}
	public void setQueueRts2Channel(String queueRts2Channel) {
		this.queueRts2Channel = queueRts2Channel;
	}
	public String getQueueRts2Name() {
		return queueRts2Name;
	}
	public void setQueueRts2Name(String queueRts2Name) {
		this.queueRts2Name = queueRts2Name;
	}
	private String queueChannel;
	private String queueName;
	private String queueRts2Channel;
	private String queueRts2Name;

	public String getHostOneName() {
		return hostOneName;
	}
	public void setHostOneName(String hostOneName) {
		this.hostOneName = hostOneName;
	}

	public String getHostTwoName() {
		return hostTwoName;
	}
	public void setHostTwoName(String hostTwoName) {
		this.hostTwoName = hostTwoName;
	}
	
	public int getPort() {
		return port;
	}
	public String getHostRtsThreeName() {
		return hostRtsThreeName;
	}
	public void setHostRtsThreeName(String hostRtsThreeName) {
		this.hostRtsThreeName = hostRtsThreeName;
	}
	public String getHostRtsFourName() {
		return hostRtsFourName;
	}
	public void setHostRtsFourName(String hostRtsFourName) {
		this.hostRtsFourName = hostRtsFourName;
	}
	public String getQueueRtsThreeManager() {
		return queueRtsThreeManager;
	}
	public void setQueueRtsThreeManager(String queueRtsThreeManager) {
		this.queueRtsThreeManager = queueRtsThreeManager;
	}
	public String getQueueRtsFourManager() {
		return queueRtsFourManager;
	}
	public void setQueueRtsFourManager(String queueRtsFourManager) {
		this.queueRtsFourManager = queueRtsFourManager;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public String getQueueOneManager() {
		return queueOneManager;
	}
	public void setQueueOneManager(String queueOneManager) {
		this.queueOneManager = queueOneManager;
	}
	public String getQueueTwoManager() {
		return queueTwoManager;
	}
	public void setQueueTwoManager(String queueTwoManager) {
		this.queueTwoManager = queueTwoManager;
	}
	public String getQueueChannel() {
		return queueChannel;
	}
	public void setQueueChannel(String queueChannel) {
		this.queueChannel = queueChannel;
	}
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	

}
