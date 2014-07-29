package analytics.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
	private int month;
	private String type;
	private double coefficeint;
	 

	public Variable() {}
	 
	public Variable( String nm, String id, int mnth, String typ, double coefficnt)
	{
		this.name   = nm;
		this.vid   = id;
		this.month = mnth;
		this.type   = typ;
		this.coefficeint = coefficnt;
	}
	 
	public ObjectId getId() { return this._id; }
	public void setId( ObjectId _id ) { this._id = _id; }
	public void generateId() { if( this._id == null ) this._id = new ObjectId(); }
	 
	public String getName() { return this.name; }
	public void setName( String name ) { this.name = name; }

	public String getVid() { return this.vid; }
	public void setVid( String VID ) { this.vid = VID; }
	
	public int getMonth() { return this.month; }
	public void setMonth( int mnth ) { this.month = mnth; }
	 
	public String getType() { return this.type; }
	public void setType( String type ) { this.type = type; }
	 
	 
	public double getCoefficeint() { return this.coefficeint; }
	public void setCoefficeint( double coefficeint ) { this.coefficeint = coefficeint; }
	 
	public DBObject bsonFromPojo()
	{
		BasicDBObject document = new BasicDBObject();
		 
		document.put( "_id",    this._id );
		document.put( "name",   this.name );
		document.put( "month", this.month );
		document.put( "type",   this.type );
		document.put( "coefficeint",  this.coefficeint );
		 
		return document;
	}
	 
	public void makePojoFromBson( DBObject bson )
	{
		BasicDBObject b = ( BasicDBObject ) bson;
		 
		this._id    	= ( ObjectId ) b.get( "_id" );
		this.name   	= ( String )   b.get( "name" );
		this.month = ( Integer )   b.get( "realTimeFlag" );
		this.type   	= ( String )   b.get( "type" );
		this.coefficeint= ( Double )   b.get( "coefficeint" );
	}
}
