package analytics.util;

public class MongoNameConstants {
	
	public static final String IS_PROD = "rtseprod";
	
	public static final String ID="_id";
	public static final String L_ID= "l_id";
	public static final String TIMESTAMP = "t";
	
	//FB id , TW id collection
	public static final String SOCIAL_ID = "u";
	
	//FB keyword mapping
	public static final String SOCIAL_KEYWORD="k";
	public static final String SOCIAL_VARIABLE="v";
	
	//Model variables collection
	public static final String MODELV_VARIABLE="variable";
	public static final String MODELV_NAME="name";
	
	//Member UUID collection
	public static final String MUUID_UUID="u";
	
	//Member zip
	public static final String ZIP="zip";
	
	//div cat ksn collection
	public static final String DCK_K="k";
	public static final String DCK_C="c";
	public static final String DCK_D="d";
	
	//Pid div ln collection
	public static final String PID_DIV_LN_COLLECTION = "pidDivLn";
	public static final String PDL_PID="pid";
	public static final String PDL_D="d";
	public static final String PDL_L="l";
	
	//Div ln var
	public static final String DLV_DIV="d";
	public static final String DLV_VAR="v";
	
	//div ln boost
	public static final String DLB_DIV="d";
	public static final String DLB_BOOST="b";
	
	//Div ln item
	public static final String DLI_DIV="d";
	public static final String DLI_LN="l";
	public static final String DLI_ITEM="i";
	
	//Trait variables
	public static final String TV_TRAIT="t";
	public static final String TV_VARIABLES="v";
	
	//Member traits
	public static final String MT_TRAIT="t";
	public static final String MT_DATES_ARR="date";
	public static final String MT_DATE="d";
	
	//Model variables collection
	public static final String MODEL_ID = "modelId";
	public static final String MODEL_NAME="modelName";
	public static final String MONTH = "month";
	public static final String CONSTANT = "constant";
	public static final String VARIABLE = "variable";
	public static final String VAR_NAME = "name";
	public static final String COEFFICIENT = "coefficient";
	public static final String INTERCEPT = "intercept";
	
	//Variables collection
	public static final String V_ID = "VID";
	public static final String V_NAME = "name";
	public static final String V_STRATEGY = "strategy";
	
	//Member variables collection
	public static final String MV_EXPIRY_DATE = "e";
	public static final String MV_EFFECTIVE_DATE = "f";
	public static final String MV_VID = "v";

	//Changed member scores collection
	public static final String CMS_MIN_EXPIRY_DATE = "minEx";
	public static final String CMS_MAX_EXPIRY_DATE = "maxEx";
	public static final String CMS_EFFECTIVE_DATE = "f";
	public static final String CMS_SCORE = "s";
	public static final String CMS_SOURCE = "c";
	
	//Blackout variables
	public static final String BLACKOUT_VAR_PREFIX="BLACKOUT";

	//Member boosts
	public static final String BOOST_VAR_PREFIX="BOOST";
	public static final String MBR_BOOSTS_COLLECTION="memberBoosts";
	public static final String BOOSTS_ARRAY="boosts";
	
	//Feed boosts
	public static final String FEED_TO_BOOST_COLLECTION="feedBoosts";
	public static final String FB_BOOSTS="b";
	public static final String FB_FEED="f";
	public static final String BROWSE_BOOST_PREFIX="BOOST_BROWSE";

	//Sources
	public static final String SOURCES_S="s";
	public static final String SOURCES_N="n";
	
	//DC Collections
	public static final String DC_MODEL = "dcModel";
	public static final String DC_QA_STRENGTHS = "dcQAStrengths";

    //Client Campaign
    public static final String CC_client = "client";
    public static final String CC_channel = "channel";
    public static final  String CC_startDate = "startDate";
    public static final  String CC_endDate = "endDate";
    public static final  String CC_maxCount = "maxCount";
    public static final  String CC_currentCount = "currentCount";
    public static final  String CC_tagType = "type";
    public static final  String CC_tagId = "tagId";
    
    
    //modelPercentile collections
     public static final String MODEL_PERC = "percentile";
     public static final String MAX_SCORE = "maxScore";
     
     
     //tagVaraible collections
     public static final String TAG_VAR_VAR = "v";
     public static final String TAG_VAR_MDTAG = "t";
     public static final String TAG_VAR_MODEL = "m";
     
     //tagsMetadata collection
     public static final String PURCHASE_OCCASSION = "OCC";
     public static final String SUB_BUSINESS_UNIT = "SUB";
     public static final String BUSINESS_UNIT= "BU_";
     public static final String SEG = "SEG";

     //Member Info Collection
     public static final String E_ID = "eid";
     
     //Occasion Custome Event Collection
     public static final String OCCASION = "occasion";
     public static final String INTERACT_CUSTOME_EVENT = "intCustEvent";
     
     //Vibes Collection
     public static final String PROCESSED_FLAG = "processed";
     
     public static final String ACTIVE_BUSINESS_UNIT= "BU";

}
