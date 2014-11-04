package analytics.util;

import analytics.util.dao.DivLnItmDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.DivLn;

public class PidMatchUtils {
	PidDivLnDao pidDivLn = new PidDivLnDao();
	DivLnItmDao divLnItm = new DivLnItmDao();
	public DivLn getDivInformation(String pid){
		String div;
		String divLn;
		String item;
		pid = pid.trim().toUpperCase();//maybe we can remove this
		if(pid.startsWith("S")||pid.length()>17){
			return null;
			//market place item -  we can not process it
		}
		//If pid does not end with P, we need to add it
		if(!(pid.endsWith("P")||pid.endsWith("B"))){
			pid = pid+'P';
		}
		if(pid.length()==12){
			//If PID length equals 12, then it is a Sears product and div and itm_no can be parsed out
			div = pid.substring(0, 2);
			item = pid.substring(3, 7);
			divLn = divLnItm.getLnFromDivItem(div, item);
			return new DivLn(div, divLn); 
		}
		if(pid.length()==14 && pid.contains("CO")){
			div = pid.substring(0, 2);
			return new DivLn(div, div);
		}
		if(pid.length()==17){
			div= pid.substring(0,2);
			if(pid.contains("V")||pid.contains("W")){
				item = pid.substring(4,12);//kmart item
				divLn = divLnItm.getLnFromDivItem(div, item);
				return new DivLn(div, divLn);
			}
		}
		//Default lookup. Also length = 14 and contains VA comes here
		return pidDivLn.getDivLnFromPid(pid);
	}
}
