package analytics.util.objects;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SYWEntity {
private int Id;
private String EntityType;
private String OwnerId;
private boolean InStore;
public int getId() {
	return Id;
}
public void setId(int id) {
	this.Id = id;
}
public String getType() {
	return EntityType;
}
public void setType(String type) {
	this.EntityType = type;
}
public String getOwner() {
	return OwnerId;
}
public void setOwner(String owner) {
	this.OwnerId = owner;
}
public boolean getInStore() {
	return InStore;
}
public void setInStore(boolean inStore) {
	InStore = inStore;
}

@Override
public String toString() {
	   return "Entities [Id=" + Id + ", EntityType=" + EntityType + ", OwnerId="+ OwnerId + ", InStore=" + InStore + "]";

	}
}
