package com.redislabs.ingest.sortedsets;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * This is a custom TimeSeriesFilter class that will enable filtering 
 * tweets that are marked lang=en. (English tweets)
 * 
 */
public class EnglishTweetsFilter extends SortedSetFilter
{
	/*
	 * @param name: name of the TimeSeriesFilter object
	 * @param subscriberChannel: name of the channel to listen to the availability of new messages
	 * @param publisherChannel: name of the channel to publish the availability of new messages
	 */
	public EnglishTweetsFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		super(name, subscriberChannel, publisherChannel);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.redislabs.ingest.sortedsets.TimeSeriesFilter#processMessage(java.lang.String)
	 */
	@Override
	protected void processMessage(String message) throws Exception{
		
		//Filter; add them to a new time series database and publish
		JsonParser jsonParser = new JsonParser();

		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();

		if(jsonObject.get("lang") != null && jsonObject.get("lang").getAsString().equals("en")){
			System.out.println(jsonObject.get("text").getAsString());
			if(sortedSetPublisher != null){
				sortedSetPublisher.publish(jsonObject.toString());					
			}
		}
	}
	
	/*
	 * Main method to start EnglishTweetsFilter
	 */
	public static void main(String[] args) throws Exception{
		EnglishTweetsFilter englishFilter = new EnglishTweetsFilter("EnglishFilter", "alldata", "englishtweets");
		englishFilter.start();
	}

}