package analytics.service.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Variable implements Serializable
{
	private static final Logger LOG = LoggerFactory.getLogger(Variable.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ObjectId _id;
	private String name;
	private String VID;
	private int realTimeFlag;
	private String type;
	private String strategy;
	private double coefficeint;
	 

	public Variable() {}
	 
	public Variable( String name, String VID, int realTimeFlag, String type, String strategy, double coefficeint)
	{
		this.name   = name;
		this.VID   = VID;
		this.realTimeFlag = realTimeFlag;
		this.type   = type;
		this.strategy  = strategy;
		this.coefficeint = coefficeint;
	}
	 
	public ObjectId getId() { return this._id; }
	public void setId( ObjectId _id ) { this._id = _id; }
	public void generateId() { if( this._id == null ) this._id = new ObjectId(); }
	 
	public String getName() { return this.name; }
	public void setName( String name ) { this.name = name; }

	public String getVid() { return this.VID; }
	public void setVid( String VID ) { this.VID = VID; }
	
	public int getRealTimeFlag() { return this.realTimeFlag; }
	public void setRealTimeFlag( int realTimeFlag ) { this.realTimeFlag = realTimeFlag; }
	 
	public String getType() { return this.type; }
	public void setType( String type ) { this.type = type; }
	 
	public String getStrategy() { return this.strategy; }
	public void setStrategy( String strategy ) { this.strategy = strategy; }
	 
	public double getCoefficeint() { return this.coefficeint; }
	public void setCoefficeint( double coefficeint ) { this.coefficeint = coefficeint; }
	 
	public DBObject bsonFromPojo()
	{
		BasicDBObject document = new BasicDBObject();
		 
		document.put( "_id",    this._id );
		document.put( "name",   this.name );
		document.put( "realTimeFlag", this.realTimeFlag );
		document.put( "type",   this.type );
		document.put( "strategy",  this.strategy );
		document.put( "coefficeint",  this.coefficeint );
		 
		return document;
	}
	 
	public void makePojoFromBson( DBObject bson )
	{
		BasicDBObject b = ( BasicDBObject ) bson;
		 
		this._id    	= ( ObjectId ) b.get( "_id" );
		this.name   	= ( String )   b.get( "name" );
		this.realTimeFlag = ( Integer )   b.get( "realTimeFlag" );
		this.type   	= ( String )   b.get( "type" );
		this.strategy  	= ( String )   b.get( "strategy" );
		this.coefficeint= ( Double )   b.get( "coefficeint" );
	}
}
