package analytics.exception;

public class RTSCacheException extends Exception{

	private static final long serialVersionUID = 1L;
	private String errorMessage;
	
	public RTSCacheException(String errorMessage){
		this.errorMessage = errorMessage;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
