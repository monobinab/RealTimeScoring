package analytics.util;

public class SystemUtility {
 public static Boolean setEnvironment(String[] args){
	 if(args.length > 0){
			for(String argument:args){
				
				if(argument.equalsIgnoreCase("PROD")){
					System.setProperty(MongoNameConstants.IS_PROD, "PROD");
				}
				else if(argument.equalsIgnoreCase("QA")){
					System.setProperty(MongoNameConstants.IS_PROD, "QA");
				}
				else if(argument.equalsIgnoreCase("LOCAL")){
					System.setProperty(MongoNameConstants.IS_PROD, "LOCAL");
				}
			}
			return true;
		}
	 else{
		 return false;
	 }
 	}
}
