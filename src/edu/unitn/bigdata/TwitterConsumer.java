package edu.unitn.bigdata;

import sun.org.mozilla.javascript.internal.json.JsonParser;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by congdinh on 6/3/14.
 */
public class TwitterConsumer {
    static String dataDir = "./";
    static String dataPath = "";
    static DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    static int countTweets = -1;
    public static void main(String args[]) throws Exception{

        if (args.length > 0) dataDir = args[0];

        AccessToken accessToken = new AccessToken("1065129572-a2xiO3kOCJkXOmX7ttUR3Nq34Aivmunlk4UKq5t", "0JEeHnQUzshIEwkqa0w58VcKOkHeoAUI9gVMZhmpObmss");
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.setOAuthConsumer("3dvYuPSa5esLHFuSUzJ4tgFMk", "Km0D2t0qkQWq5IjP3po9QiXjcyEjnSIt8WXck2zGW2MvTHruKO");
        twitterStream.setOAuthAccessToken(accessToken);


        StatusListener simpleStatusListener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
               // if (status.getLang().equals("en"))
              //  {
                    String rawJSON = TwitterObjectFactory.getRawJSON(status);
                    writeTweets(cleanText(rawJSON));
                    /*writeTweets(status.getId() + "\t"
                            // User profile
                            + status.getUser().getScreenName() + "\t"
                            + cleanText(status.getUser().getLocation()) + "\t"
                            + cleanText(status.getUser().getDescription()) + "\t"
                            + status.getUser().getLang() + "\t" // Lang user prefers
                            + status.getUser().getTimeZone() + "\t"
                            + status.getUser().getFollowersCount() + "\t"
                            + status.getUser().getFriendsCount() + "\t"
                            + status.getUser().getStatusesCount() + "\t"
                            // Posted time
                            + status.getCreatedAt().getTime() + "\t"
                            // Geo location
                            + status.getGeoLocation().getLatitude() + "\t"
                            + status.getGeoLocation().getLongitude() + "\t"
                            // Place detail
                            + status.getPlace().getName() + "\t"
                            + status.getPlace().getCountry() + "\t"
                            + status.getPlace().getStreetAddress() + "\t"
                            + status.getLang() + "\t" // Lang of the tweet
                            + cleanText(status.getText()));*/
              //  }
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
        FilterQuery filterQuery = new FilterQuery();
        double[][] locations = { {10.439443587087794,45.679983268451274},
                {11.987546900385569, 46.532036602887004} };
        filterQuery.locations(locations);
        twitterStream.filter(filterQuery);
    }

    public static void writeTweets(String tweet) {
        if(++countTweets%1000000==0) {
            Date date = new Date();
            dataPath = dataDir + "tweets_" + dateFormat.format(date) + ".txt";
            System.out.println("Start a new file " + dataPath);
            countTweets = 0;
        }
        File file = new File(dataPath);
        try
        {
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getPath(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(tweet);
            bw.newLine();
            bw.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    public static String cleanText (String orgText) {
        String temp = orgText.replace("\n"," ").replace("\t"," ");
        while(temp.contains("  ")){
            temp = temp.replace("  "," ");
        }
        return temp.trim();
    }
}
