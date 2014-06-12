package edu.unitn.bigdata;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.log4j.*;
import org.apache.log4j.Logger;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;

import static java.lang.System.exit;

/**
 * Created by congdinh on 6/6/14.
 */
public class TwitterPusher {
    static DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    static int countTweets = -1;
    static String dataDir = "./"; // remember to have a slash / at the end
    static String dataPath = "";
    static String bucketName = "trentinotwitterdata"; //bucket on S3
    static String dirToCredentials = "./credentials.txt"; // dir on EC2
    static String accessKey = "";
    static String secretKey = "";
    static String preFile = "";
    static AmazonS3 client;

    public static void main(String args[]) throws Exception {
        dateFormat.setTimeZone(TimeZone.getTimeZone("Europe/Rome"));
        String[] cre = readCredentials(dirToCredentials);

        if (cre != null) {
            accessKey = cre[0];
            secretKey = cre[1];
        }

        if (args.length > 0) dataDir = args[0] + "/"; // avoid forgetting a slash at the end of dir
        dataDir = dataDir.replace("//", "/");

        AccessToken accessToken = new AccessToken("1735631401-v2EsRbkgLZ7HWs9Al3JLWC6tIbjmVN24J9PgRpy", "eImxQGATXbrcUbACbVFyaowveFAX1xB271RqNmmJ9x4");
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.setOAuthConsumer("g7io17JPwsztoevvp73ew", "ZCg0QdpG7pC6ZtpgtTpx9ytr3uWtHQGa7xZtwBWHk");
        twitterStream.setOAuthAccessToken(accessToken);

        final Logger logger = Logger.getLogger("MyLog");
        Appender fh = new FileAppender(new SimpleLayout(), "MyLogFile.log");
        logger.addAppender(fh);
        fh.setLayout(new SimpleLayout());

        StatusListener simpleStatusListener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                String rawJSON = TwitterObjectFactory.getRawJSON(status);

                if (writeTweets(cleanText(rawJSON),logger)) {
                    client = new AmazonS3Client(
                            new BasicAWSCredentials(accessKey, secretKey));
                    client.setEndpoint("s3.amazonaws.com");
                    File file = new File(dataDir + preFile);
                    client.putObject(new PutObjectRequest(
                            bucketName, preFile, file));
                    System.out.println("Uploaded " + dataDir + preFile);
                   logger.info("Uploaded " + dataDir + preFile);
                    if (file.delete()) logger.info("Deleted file " + file.getPath());
                    else {
                        System.out.println("Error while deleting file " + file.getPath());
                        logger.info("Error while deleting file " + file.getPath());
                        exit(1);
                    }
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
                logger.log(Level.ERROR,"Exception!!!",e);
                e.printStackTrace();
            }
        };
        twitterStream.addListener(simpleStatusListener);
        FilterQuery filterQuery = new FilterQuery();
        double[][] locations = {{-73.985527, -33.750702},
                {-28.839041, 5.264860}};
        filterQuery.locations(locations);
        twitterStream.filter(filterQuery);
    }

    public static boolean writeTweets(String tweet, Logger logger) {
        boolean needUpload = false;
        if (++countTweets % 1000000 == 0) {
            Date date = new Date();
            if (!preFile.equals("")) {
                needUpload = true;
                preFile = dataPath.replace(dataDir, "");
            } else preFile = "tweets_" + dateFormat.format(date) + ".txt";
            dataPath = dataDir + "tweets_" + dateFormat.format(date) + ".txt";
            System.out.println("Start a new file " + dataPath);
            logger.info("Start a new file " + dataPath);
            countTweets = 0;
        }

        try {
            File file = new File(dataPath);
            if (!file.exists()) {
                if (file.createNewFile()) {
                    System.out.println("Successfully create file " + file);
                    logger.info("Successfully create file " + file);
                } else {
                    System.out.println("Failed to create file " + file);
                    logger.info("Failed to create file " + file);
                    exit(1);
                }
            }
            FileWriter fw = new FileWriter(file.getPath(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(tweet);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
            logger.log(Level.ERROR,"Exception in write file",e);
            e.printStackTrace();
        }
        return needUpload;
    }

    public static String[] readCredentials(String dirToCredentials) {
        String[] cre = null;
        try {
            FileInputStream fis = new FileInputStream(new File(dirToCredentials));
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                cre = line.split(",");
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cre;
    }

    public static String cleanText(String orgText) {
        String temp = orgText.replace("\n", " ").replace("\t", " ");
        while (temp.contains("  ")) {
            temp = temp.replace("  ", " ");
        }
        return temp.trim();
    }
}
