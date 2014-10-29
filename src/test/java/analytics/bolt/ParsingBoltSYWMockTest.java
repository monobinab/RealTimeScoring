package analytics.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.FakeMongo;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.github.fakemongo.Fongo;

public class ParsingBoltSYWMockTest {
	/**
	 * WE ARE NOT STUBBING OUT SYWAPICALL FOR USERID
	 */
	static Map<String,String> conf;
	@BeforeClass
	public static void initializeFakeMongo(){
		System.setProperty("rtseprod", "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));			
	}
	@Test
	public void invalidInteractionTypeIsIgnored(){		
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        ParsingBoltSYW boltUnderTest = new ParsingBoltSYW();
        TopologyContext context = new MockTopologyContext();
        boltUnderTest.prepare(conf, context, outputCollector);
        String input = "[{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":1221863,\"EntityType\":\"Topic\"},{\"IsTaggedItem\":true,\"Id\":373541114,\"EntityType\":\"Product\"}],\"InteractionType\":\"TagItem\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}]";
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockTuple(input,"syw");
        
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple();
        Assert.assertNull(outputTuple);
	}
	
	@Test
	public void catalogInteractionsAreProcessed(){
		String lId = "dxo0b7SN1eER9shCSj0DX+eSGag=";
		String interactionType = "AddToCatalog";
		String expected = "{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":184008680,\"EntityType\":\"Product\"},{\"Id\":8353466,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}";
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        ParsingBoltSYW boltUnderTest = new ParsingBoltSYW();
        TopologyContext context = new MockTopologyContext();
        boltUnderTest.prepare(conf, context, outputCollector);
        String input = "[{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":184008680,\"EntityType\":\"Product\"},{\"Id\":8353466,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}]";
        
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockTuple(input,"syw");
        
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple();
        Assert.assertEquals(lId, outputTuple.get(0));
        Assert.assertEquals(interactionType, outputTuple.get(2));
        Assert.assertEquals(expected, outputTuple.get(1));
	}

}
