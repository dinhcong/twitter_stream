package edu.unitn.bigdata;

import twitter4j.*;
import twitter4j.auth.AccessToken;

/**
 * Created by congdinh on 6/3/14.
 */
public class TwitterConsumer {
    public static void main(String args[]) throws Exception{
        String token ="867221126-vsmwC9Gf5DNTh7zQkxxnWojhzAdrEQ0kqKSEZhI7"; // load from a persistent store
        String tokenSecret ="FCEgFIWVIwdpKYqkQ7YRxHa1sxlT0MFDJN6hfCWQc"; // load from a persistent store
        AccessToken accessToken = new AccessToken(token, tokenSecret);

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
