package analytics.bolt;

import analytics.util.SecurityUtils;
import analytics.util.SywApiCalls;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

public class ParsingBoltSYW extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ParsingBoltSYW.class);
	private OutputCollector outputCollector;
	private List<String> listOfInteractionsForRTS;
	SywApiCalls sywApiCalls;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		sywApiCalls = new SywApiCalls();
		this.outputCollector = collector;
		listOfInteractionsForRTS = new ArrayList<String>();
		
		//TODO: This is where more interactions are added. 	
		//Master list of interactions we process
		//listOfInteractionsForRTS.add("Like");
		listOfInteractionsForRTS.add("AddToCatalog"); 
	}

	@Override
	public void execute(Tuple input) {
		//Read the JSON message from the spout
		JsonParser parser = new JsonParser();
		JsonArray interactionArray = parser.parse(input.getStringByField("message")).getAsJsonArray();

		// Each record can have multiple elements in it, though it is generally only one
		for (JsonElement interaction : interactionArray) {
			JsonObject interactionObject = interaction.getAsJsonObject();		
			String l_id = sywApiCalls.getLoyaltyId(interactionObject.get("UserId").getAsString());
			/*Ignore if we can not get member information
			 * Possible causes are
			1. user is not subscribed to our app
			2. Invalid user id - but how would this come in here??
			*/

			if (l_id == null) {
				//LOGGER.warn("Unable to get member information" + input);
				outputCollector.fail(input);
				//could not process record
				return;
			} else {		
				// RTS only wants encrypted loyalty ids
				l_id = SecurityUtils.hashLoyaltyId(l_id);
			}
			JsonElement interactionType = interactionObject.get("InteractionType");
			/*Ignore interactions that we dont want. We can do further refinements if needed*/
			String interactionTypeString = interactionType.getAsString();
			if (listOfInteractionsForRTS.contains(interactionTypeString)) {
				// Create a SYW Interaction object
					outputCollector.emit(tuple(l_id, interactionObject.toString(),interactionTypeString));
			} else {
				//We should look into either processing this request type or not subscribing to it
				LOGGER.info("Ignore interaction type" + interactionType.getAsString());
			}
		}
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "message", "InteractionType"));
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
