package analytics.util;

public class Constants {
	public static final int METRICS_INTERVAL = 60;
	//public static final String SCORING_API_PRE = "http://realtimescoring.intra.searshc.com/rtsapi/v1/top/categories/";
	public static final String SCORING_API_PRE = "http://rtsapi301p.qa.ch3.s.com:8180/rtsapi/v1/top/categories/";
	public static final String SCORING_API_POST = "?key=rtsTeam&tags=models"; 
	public static final String AUTH_PROPERTY_FILE = "authentication.properties";
	public static final String RESP_URL_USER_NAME = "responseWebserviceUsrname";
	public static final String RESP_URL_PASSWORD = "responseWebservicePassword";
	public static final String RESP_URL = "responseWebserviceURL";
	public static final String PURCHASE_OCCASSION = "OCC";
	public static final String SUB_BUSINESS_UNIT = "SUB";
	public static final String BUSINESS_UNIT= "BU_";
	public static final String SEG = "SEG";
	public static final String TAG_VAR = "v";
	public static final String TAG_MDTAG = "t";
	public static final String TAG_MODEL = "m";
	public static final String OCC_VAR = "v";
	public static final String OCC_BU = "b";
	public static final String OCC_SUB = "s";
	public static final String OCC_PO = "po";
	
	public static final String AAM_TRAITS_PATH="/smith/adobe/rts/out/traits";
	public static final String AAM_BROWSER_PATH="/smith/adobe/rts/out/products";
	public static final String AAM_INTERNAL_SEARCH_PATH="/smith/adobe/rts/out/is";
	public static final String HADOOP_WEBHDFS_URL="http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>?user.name=spannal&op=LISTSTATUS";
	public static final String CONTENT_SUMMARY_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=GETCONTENTSUMMARY";
	public static final String FILE_READ_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=OPEN";
	
	public static final String RESPONSE_REDIS_SERVER_HOST="respRedisServerHost";
	public static final String RESPONSE_REDIS_SERVER_PORT="respRedisServerPort";
	
	public static final String TELLURIDE_REDIS_SERVER_HOST="tellurideRedisServerHost";
	public static final String TELLURIDE_REDIS_SERVER_PORT="tellurideRedisServerPort";
}
