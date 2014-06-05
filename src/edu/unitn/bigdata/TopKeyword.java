package edu.unitn.bigdata;

import javax.swing.text.html.HTMLDocument;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by quynh on 6/5/14.
 */
public class TopKeyword {
    private HashMap<String, Integer> hashMap;
    String stopWords = "|a|about|above|after|am|an|and|any|are|aren't|as|at|be|because|been|before|being|below|between|both|but|by|can't|cannot|could|couldn't|did|didn't|do|does|doesn't|doing|don't|down|during|each|few|for|from|further|had|hadn't|has|hasn't|have|haven't|having|he|he'd|he'll|he's|her|here|here's|hers|herself|him|himself|his|how|how's|i|i'd|i'll|i'm|i've|if|in|into|is|isn't|it|it's|its|itself|let's|me|more|most|mustn't|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|same|shan't|she|she'd|she'll|she's|should|shouldn't|so|some|such|than|that|that's|the|their|theirs|them|themselves|then|there|there's|these|they|they'd|they'll|they're|they've|this|those|through|to|too|under|until|up|very|was|wasn't|we|we'd|we'll|we're|we've|were|weren't|what|what's|when|when's|where|where's|which|while|who|who's|whom|why|why's|with|won't|would|wouldn't|you|you'd|you'll|you're|you've|your|yours|yourself|yourselves|";

    public TopKeyword(String tweets) {
        hashMap = new HashMap<String, Integer>();

        StringTokenizer st = new StringTokenizer(tweets);

        while (st.hasMoreTokens()) {
            String key = st.nextToken();
            if (!stopWords.contains(key)) {
                if (!hashMap.containsKey(key)) {
                    hashMap.put(key, Integer.valueOf(1));
                } else {
                    int value = hashMap.get(key);
                    hashMap.put(key, Integer.valueOf(value + 1));
                }
            }
        }
        Iterator<String> keySetIterator = hashMap.keySet().iterator();

        while(keySetIterator.hasNext())
        {
            String key = keySetIterator.next();
            System.out.println(key + "/" + hashMap.get(key));
        }
    }

    public static void main(String args[]) {
        TopKeyword tw = new TopKeyword("nguyen pham xuan quynh quynh quynh quynh");
    }

    public void findTopKeywords() {
        List list = new LinkedList(hashMap.entrySet());

        // sort list based on comparator
        Collections.sort(list, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                    .compareTo(((Map.Entry) (o2)).getValue());

            }
        });

        // put sorted list into map again
        Map sortedMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();)
        {
            Map.Entry entry = (Map.Entry)
        }
    }
    public void writeTopKeywords(String path)
    {
        File file = new File(path);
        try
        {
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getPath(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            Iterator<String> keySetIterator = hashMap.keySet().iterator();
            Collection<Integer> values = hashMap.values();

            while(keySetIterator.hasNext())
            {
                String key = keySetIterator.next();
                bw.write(key + "\t" + );
            }
            bw.write(tweet);
            bw.newLine();
            bw.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
