package analytics.util.objects;

import org.bson.types.ObjectId;

import java.io.Serializable;

public class Variable implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ObjectId _id;
	private String name;
	private String vid;
	private double coefficient;
	private String strategy;
	 
	public Variable( String nm, String vid, double coefficnt)
	{
		this.name   = nm;
		this.coefficient = coefficnt;
		this.vid = vid;
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
