package analytics.util.objects;

import java.io.Serializable;

public class Boost extends Variable implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private double intercept;
	 
	public Boost( String nm, double c, double i)
	{
		super (nm,c);
		this.intercept = i;
	}
	
	public double getIntercept() { return this.intercept; } 
}
