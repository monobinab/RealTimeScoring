package metascale.bolt;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import metascale.bolt.ScoringBolt;

import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;



public class ScoringBoltTest {

	@Test
	public void test() {
		ScoringBolt scoringBolt = new ScoringBolt();
		DBCollection mockedMemberVariables = mock(DBCollection.class);
		DBCollection mockedModelVariables = mock(DBCollection.class);
		MongoClient mockedMongoClient = mock(MongoClient.class);
		
		
		scoringBolt.setMemberCollection(mockedMemberVariables);
		scoringBolt.setModelCollection(mockedModelVariables);
		scoringBolt.setMongoClient(mockedMongoClient);
		assertTrue(true);
	}

}
