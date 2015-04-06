package analytics.util;
/**
 * This code has been updated or modified to fit our needs...
 * The original credit goes to coder from "https://github.com/zxs/webhdfs-java-client"
 * @modified spannal
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author wesley
 *
 */
public class WebHDFSConnectionFactory {
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	/** The default host to connect to */
	public static final String DEFAULT_HOST = "localhost";

	/** The default port */
	public static final int DEFAULT_PORT = 14000;

	/** The default username */
	public static final String DEFAULT_USERNAME = "zen";

	/** The default username */
	public static final String DEFAULT_PASSWORD = "abc123";
	
	public static final String DEFAULT_PROTOCOL = "http://";

	public static enum AuthenticationType {
		KERBEROS, PSEUDO
	}

	private String 	host 	= 	DEFAULT_HOST;
	private int 	port	=	DEFAULT_PORT;
	private String 	username=	DEFAULT_USERNAME;
	private String 	password=	DEFAULT_PASSWORD;
	private String 	authenticationType	= AuthenticationType.KERBEROS.name();
	private WebHDFSConnection webHDFSConnection;
	
	public WebHDFSConnectionFactory() {
	}
	
	

	public WebHDFSConnectionFactory(String host, int port , String username, String password, String authType) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.authenticationType = authType;
	}

	
	public WebHDFSConnection getConnection() {
		//TODO: use pool ...
		
		if(host ==null || port < 0 || username == null || authenticationType == null){
			System.out.println("Either Host,Port,UserName or Authentication Type is set correctly... Please check and try again");
		}
			
		String httpfsUrl = DEFAULT_PROTOCOL + host + ":" + port;
		if (webHDFSConnection == null) {
			if (authenticationType.equalsIgnoreCase(AuthenticationType.KERBEROS.name())) {
				webHDFSConnection = new KerberosWebHDFSConnection(httpfsUrl, username, password);
			} else
			// if(authenticationType.equalsIgnoreCase(AuthenticationType.PSEUDO.name()))
			{
				webHDFSConnection = new PseudoWebHDFSConnection(httpfsUrl, username, password);
			}
		}
		return webHDFSConnection;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getAuthenticationType() {
		return authenticationType;
	}

	public void setAuthenticationType(String authenticationType) {
		this.authenticationType = authenticationType;
	}

}
