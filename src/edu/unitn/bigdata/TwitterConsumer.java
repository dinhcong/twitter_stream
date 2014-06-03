package edu.unitn.bigdata;

import twitter4j.RawStreamListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

/**
 * Created by congdinh on 6/3/14.
 */
public class TwitterConsumer {
    public static void main(String args[]) throws Exception{
        AccessToken accessToken = new AccessToken("867221126-vsmwC9Gf5DNTh7zQkxxnWojhzAdrEQ0kqKSEZhI7", "FCEgFIWVIwdpKYqkQ7YRxHa1sxlT0MFDJN6hfCWQc");

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.setOAuthConsumer("OOZKe0dvCzgS6Isxcpg98g", "HkLM90v39VhgptMCPMENUd2Sgq6rS2YpX45xS6Nro");
        twitterStream.setOAuthAccessToken(accessToken);
        RawStreamListener listener = new RawStreamListener() {
            @Override
            public void onMessage(String rawJSON) {
                System.out.println(rawJSON);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
