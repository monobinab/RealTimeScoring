package analytics.util;

public class MongoNameConstants {
	
	public static final String ID="_id";
	public static final String L_ID= "l_id";
	
	//FB id collection
	public static final String FB_ID = "u";
	
	//FB keyword mapping
	public static final String FB_KEYWORD="k";
	public static final String FB_VARIABLE="v";
	
	//Model variables collection
	public static final String MODELV_VARIABLE="variable";
	public static final String MODELV_NAME="name";
	
	//Member UUID collection
	public static final String MUUID_UUID="u";
	
	//Member zip
	public static final String ZIP="z";
	
	//Pid div ln collection
	public static final String PDL_PID="pid";
	public static final String PDL_D="d";
	public static final String PDL_L="l";
	
	//Div ln var
	public static final String DLV_DIV="d";
	public static final String DLV_VAR="v";
	
	//Div ln item
	public static final String DLI_DIV="d";
	public static final String DLI_LN="l";
	public static final String DLI_ITEM="i";
	
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
