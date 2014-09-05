package analytics.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebsphereMQCredential {
	static final Logger logger = LoggerFactory.getLogger(WebsphereMQCredential.class);
	
	private String hostOneName;
	private String hostTwoName;
	private int port;
	private String queueOneManager;
	private String queueTwoManager;
	private String queueChannel;
	private String queueName;

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
