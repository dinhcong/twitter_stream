package edu.unitn.bigdata;

import java.util.HashMap;

/**
 * Created by quynh on 6/5/14.
 */
public class TopKeyword {
    private String tweets;
    private HashMap<String, Integer> hashMap;

    public TopKeyword(String tweets)
    {
        tweets = tweets;
        hashMap = new HashMap<String, Integer>();

        // tokenize

        findTopKeyword();
    }

    public


}
