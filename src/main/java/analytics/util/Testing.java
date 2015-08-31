package analytics.util;

public class Testing {

	public static void main(String[] args) {
		Integer result = null;
		try {
			result = getInt("a");
		} catch (Exception e) {
			System.out.println("calling exception");
			e.printStackTrace();
		}
		System.out.println(result);

	}
	
	
public static Integer getInt(String str) {
	System.out.println("");
	int a =10;
	Integer result = null;
	try{
	 result = Integer.parseInt(str);
	}
	catch(Exception e){
		System.out.println("method excepotion " );
		e.printStackTrace();
	}
	 return result;
	
}

}
