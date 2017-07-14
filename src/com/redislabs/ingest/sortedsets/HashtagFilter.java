package com.redislabs.ingest.sortedsets;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * This is a custom TimeSeriesFilter class that will listen to English tweets, 
 * and counts the number of hashtags in English tweets.
 * 
 */
public class HashtagFilter extends SortedSetFilter
{
	// Regular expression for hashtags
	Pattern HASHPATTERN = Pattern.compile("#(\\w+)");
	
	/*
	 * @param name: name of the TimeSeriesFilter object
	 * @param subscriberChannel: name of the channel to listen to the availability of new messages
	 * @param publisherChannel: name of the channel to publish the availability of new messages
	 */
	public HashtagFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		super(name, subscriberChannel, publisherChannel);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.redislabs.ingest.sortedsets.TimeSeriesFilter#processMessage(java.lang.String)
	 */
	@Override
	protected void processMessage(String message) throws Exception{
		JsonParser jsonParser = new JsonParser();
				
		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		
		
		if(jsonObject.get("lang") != null && jsonObject.get("lang").getAsString().equals("en")){
			Matcher mat = HASHPATTERN.matcher(jsonObject.get("text").getAsString());
			while(mat.find()){
				if(jedis != null){
					// Increase the hashtag count in the Sorted Set					
					jedis.zincrby(name, 1, mat.group(1));
				}
			}
			
		}
	}

	/*
	 * Main method to start HashtagFilter
	 */
	public static void main(String[] args) throws Exception{
		HashtagFilter hashtagFilter = new HashtagFilter("hashtagset", "englishtweets", null);
		hashtagFilter.start();
	}
	

}