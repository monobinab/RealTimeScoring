package analytics.util;

public class MongoNameConstants {
	
	public static final String ID="_id";
	public static final String L_ID= "l_id";
	//Model variables collection
	public static final String MODEL_ID = "modelId";
	public static final String MONTH = "month";
	public static final String CONSTANT = "constant";
	public static final String VARIABLE = "variable";
	public static final String VAR_NAME = "name";
	public static final String COEFFICIENT = "coefficient";
	
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
	
	public static final String BOOST_VAR_PREFIX="BOOST";
}
