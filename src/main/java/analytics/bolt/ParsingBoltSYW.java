package analytics.bolt;

import analytics.util.SecurityUtils;
import analytics.util.SywApiCalls;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParsingBoltSYW extends EnvironmentBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltSYW.class);
	private OutputCollector outputCollector;
	private List<String> listOfInteractionsForRTS;
	SywApiCalls sywApiCalls;

	public ParsingBoltSYW(String systemProperty){
		 super(systemProperty);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		sywApiCalls = new SywApiCalls();
		this.outputCollector = collector;
		listOfInteractionsForRTS = new ArrayList<String>();
		
		//TODO: This is where more interactions are added. 	
		//Master list of interactions we process
		//listOfInteractionsForRTS.add("Like");
		//listOfInteractionsForRTS.add("Want");
		//listOfInteractionsForRTS.add("Own");
		listOfInteractionsForRTS.add("AddToCatalog"); 
	}

	@Override
	public void execute(Tuple input) {
		
		String lyl_id_no = null;
		try{
			redisCountIncr("incoming_tuples");
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = parser.parse(input.getString(0));
			JsonArray interactionArray = jsonElement.getAsJsonArray();
			
			    JsonElement interaction = interactionArray.get(0);
				JsonObject interactionObject = interaction.getAsJsonObject();		
				lyl_id_no = sywApiCalls.getLoyaltyId(interactionObject.get("UserId").getAsString());
				if (lyl_id_no == null) {
					redisCountIncr("null_lid");
					outputCollector.ack(input);
					return;
			 	} else {		
			 		String l_id  = SecurityUtils.hashLoyaltyId(lyl_id_no);
					JsonElement interactionType = interactionObject.get("InteractionType");
					String interactionTypeString = interactionType.getAsString();
					if (listOfInteractionsForRTS.contains(interactionTypeString)) {
						List<Object> listToEmit = new ArrayList<Object>();
						listToEmit.add(l_id);
						listToEmit.add(interactionObject.toString());
						listToEmit.add(interactionTypeString);
						listToEmit.add(lyl_id_no);
						outputCollector.emit(listToEmit);
						LOGGER.info("PERSIST: " + lyl_id_no + " gets emitted from ParsingBoltSYW");
						redisCountIncr("sent_to_process");
					} else {
						LOGGER.info("Ignore interaction type" + interactionType.getAsString());
						redisCountIncr("unwanted_interaction_type");
					}
			 	}
			}
			catch(Exception e){
				LOGGER.error("Exception in ParsingBoltSYW: " + ExceptionUtils.getMessage(e));
				e.printStackTrace();
			}
		this.outputCollector.ack(input);
		LOGGER.info(input.getMessageId() +  ", " + lyl_id_no + " gets acked in ParsingBoltSYW");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "message", "InteractionType","lyl_id_no"));
		/*Possible interactions - may not be an exhaustive list*/
		/*
		 * AddProductMedia AddToCatalog AddToPendingPoll Answer Ask
		 * CatalogCreated Comment CommentOnStory CreatePoll EarnBadge
		 * FilteredNewsfeed Follow FollowTag Import ImportedExternalProduct
		 * InviteFriends Like LikedCatalog LikeStory MessageWrittenOnWall
		 * MobileFirstUse Own Post PostedPhotoOnTagWall
		 * PostedTextMessageOnTagWall PostedVideoOnTagWall PostShopin
		 * ProductQuickview ProductWasAddedToCart ProductWasRecommended
		 * ProfileUpdated QuestionAsked Rate Recommend Review SearchFacetChosen
		 * Share SharedProduct SharedStory Shopin StatusUpdate Tagged TagItem
		 * UnfollowTag UploadedImage UserJoined UserNotInterestedIn View
		 * VisitTag VotedOnTopicalPoll VoteOnPoll Want
		 */
	}

}
