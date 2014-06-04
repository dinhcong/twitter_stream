package edu.unitn.bigdata;

import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by congdinh on 6/3/14.
 */
public class TwitterConsumer {
    static String dataPath = "tweets.txt";

    public static void main(String args[]) throws Exception{

        if (args.length > 0) dataPath = args[0];

        AccessToken accessToken = new AccessToken("867221126-vsmwC9Gf5DNTh7zQkxxnWojhzAdrEQ0kqKSEZhI7", "FCEgFIWVIwdpKYqkQ7YRxHa1sxlT0MFDJN6hfCWQc");

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.setOAuthConsumer("OOZKe0dvCzgS6Isxcpg98g", "HkLM90v39VhgptMCPMENUd2Sgq6rS2YpX45xS6Nro");
        twitterStream.setOAuthAccessToken(accessToken);
        StatusListener simpleStatusListener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getLang().equals("en"))
                {
                    writeTweets(status.getId() + "\t" + status.getUser().getScreenName() + "\t" + status.getCreatedAt().getTime() + "\t" + status.getText().replace("\n", " ").replace("  ", " "));
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l2) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {
                System.out.println("Exception!!!");
                e.printStackTrace();
            }
        };

        twitterStream.addListener(simpleStatusListener);
        twitterStream.sample();
    }

    public static void writeTweets(String tweet) {
        Path path = Paths.get(dataPath);
        try
        {
            OutputStream outputStream = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            tweet = tweet + '\n';
            Files.write(path, tweet.getBytes(), StandardOpenOption.APPEND);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
