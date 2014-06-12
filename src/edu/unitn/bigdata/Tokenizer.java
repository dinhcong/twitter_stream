package edu.unitn.bigdata;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * Created by congdinh on 6/6/14.
 */
public class Tokenizer extends EvalFunc<Tuple> {
    static String stopWords = "|a|about|above|after|am|an|and|any|are|aren't|as|at|be|because|been|before|being|below|between|both|but|by|can't|cannot|could|couldn't|did|didn't|do|does|doesn't|doing|don't|down|during|each|few|for|from|further|had|hadn't|has|hasn't|have|haven't|having|he|he'd|he'll|he's|her|here|here's|hers|herself|him|himself|his|how|how's|i|i'd|i'll|i'm|i've|if|in|into|is|isn't|it|it's|its|itself|let's|me|more|most|mustn't|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|same|shan't|she|she'd|she'll|she's|should|shouldn't|so|some|such|than|that|that's|the|their|theirs|them|themselves|then|there|there's|these|they|they'd|they'll|they're|they've|this|those|through|to|too|under|until|up|very|was|wasn't|we|we'd|we'll|we're|we've|were|weren't|what|what's|when|when's|where|where's|which|while|who|who's|whom|why|why's|with|won't|would|wouldn't|you|you'd|you'll|you're|you've|your|yours|yourself|yourselves|";
    @Override
    public Tuple exec(Tuple objects) throws IOException {
        if(objects == null || objects.size() != 1) {
            return null;
        }
        String line = (String) objects.get(0);
        String[] tokens = line.trim().toLowerCase().split("[^A-Za-z0-9_£$%<>]");
        TupleFactory tf = TupleFactory.getInstance();
        Tuple t = tf.newTuple();
        for (String token: tokens) {
            token = token.trim();
            if(!stopWords.contains("|" + token + "|") && !token.equals("") && token.length() > 1) {
                t.append(token);
            }
        }
        return t;
    }
}
