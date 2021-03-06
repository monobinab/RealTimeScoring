package analytics.util;


public interface TopicConstants {

	public static final String AAM_ATC_PRODUCTS = "ATC" ;
	public static final String AAM_BROWSE_PRODUCTS = "BROWSE" ;
	public static final String SIGNAL_BROWSE_FEED = "SB" ;
	public static final String AAM_CDF_INTERNALSEARCH = "InternalSearch" ;
	public static final String AAM_CDF_TRAITS = "WebTraits" ;
	public static final String PRODUCTS = "Products" ;
	public static final String FB = "FB" ;
	public static final String TW = "TW" ;
	public static final String SYW = "SYW_Interactions" ;
	public static final String OCCASSION = "Member_Tags" ;
	public static final int PORT = 6379 ;
	public static final String RESCORED_MEMBERIDS_KAFKA_TOPIC = "rts_rescored_memberIds";
	public static final String BROWSE_KAFKA_TOPIC = "rts_browse";
	public static final String SWEEPS_KAFKA_TOPIC = "rts_sweeps";
	public static final String RTS_CPS_SWEEPS_KAFKA_TOPIC = "rts_cps_sweeps_in";
	//public static final String RESCORED_MEMBERIDS_KAFKA_TOPIC = "test2";
	public static final String EMAIL_FEEDBACK_RESPONSES_KAFKA_TOPIC = "rts_emailFeedback_responses";
	
	public static final String PURCHASE_KAFKA_TOPIC = "rts_cp_purchase_scores";
}

