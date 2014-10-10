package analytics.util.objects;

import org.bson.types.ObjectId;

import java.io.Serializable;

public class Variable implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected ObjectId _id;
	protected String name;
	protected String vid;
	protected double coefficient;
	protected String strategy;
	 
	public Variable( String nm, double coefficnt)
	{
		this.name   = nm;
		this.coefficient = coefficnt;
		
	}
	
	public Variable( String nm, String id, String strategy)
	{
		this.name   = nm;
		this.vid   = id;
		this.strategy = strategy;
	}
	 
	public ObjectId getId() { return this._id; }
	public void generateId() { if( this._id == null ) this._id = new ObjectId(); } 
	public String getName() { return this.name; }
	public String getVid() { return this.vid; }
	public double getCoefficient() { return this.coefficient; } 
	public String getStrategy() { return this.strategy;}
}
