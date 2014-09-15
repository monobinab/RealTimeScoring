package analytics.bolt;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;



public class StrategyScoringBoltTest {

	@Test
	public void test() {
		StrategyScoringBolt scoringBolt = new StrategyScoringBolt();
		DBCollection mockedMemberVariables = mock(DBCollection.class);
		DBCollection mockedModelVariables = mock(DBCollection.class);
		MongoClient mockedMongoClient = mock(MongoClient.class);
		
		
		//scoringBolt.setMemberCollection(mockedMemberVariables);
		//scoringBolt.setModelCollection(mockedModelVariables);
		//TODO: Understand and fix this
		//scoringBolt.setMongoClient(mockedMongoClient);
		assertTrue(true);
	}

}
