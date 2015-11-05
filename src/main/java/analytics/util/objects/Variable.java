package analytics.util.objects;

import org.bson.types.ObjectId;

import java.io.Serializable;

public class Variable implements Serializable{
	
	private static final long serialVersionUID = 1L;
	protected ObjectId _id;
	protected String name;
	protected String vid;
	protected double coefficient;
	protected String strategy;
	protected double intercept;
	
	public Variable(){}
	
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

	/**
	 * @return the _id
	 */
	public ObjectId get_id() {
		return _id;
	}

	/**
	 * @param _id the _id to set
	 */
	public void set_id(ObjectId _id) {
		this._id = _id;
	}

	/**
	 * @return the intercept
	 */
	public double getIntercept() {
		return intercept;
	}

	/**
	 * @param intercept the intercept to set
	 */
	public void setIntercept(double intercept) {
		this.intercept = intercept;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param vid the vid to set
	 */
	public void setVid(String vid) {
		this.vid = vid;
	}

	/**
	 * @param coefficient the coefficient to set
	 */
	public void setCoefficient(double coefficient) {
		this.coefficient = coefficient;
	}

	/**
	 * @param strategy the strategy to set
	 */
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}
}
