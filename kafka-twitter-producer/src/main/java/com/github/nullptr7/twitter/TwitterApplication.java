package com.github.nullptr7.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.stream.Collectors;

public class TwitterApplication {

    private static final Logger logger = LoggerFactory.getLogger(TwitterApplication.class);

    public static void main(String[] args) {
        try {
//            System.out.println(new TwitterApplication().createTweet("Hello World!"));
//            System.out.println(new TwitterApplication().createTweet("Second Tweet"));
            final List<String> banTikTok = new TwitterApplication().searchTweets("#BanTiktok");
            System.out.println(banTikTok.size());
/*
            banTikTok.forEach(s -> {
                System.out.println("=".repeat(10));
                System.out.println(s);
                System.out.println("=".repeat(10));
            });
*/
        } catch (TwitterException e) {
            logger.error("Error creating tweet - {}", e.getErrorMessage());
        }
    }

    private String createTweet(String tweet) throws TwitterException {
        Twitter twitter = getTwitterInstance();
        Status status = twitter.updateStatus(tweet);
        return status.getText();
    }

    private Twitter getTwitterInstance() {
        final String CONSUMER_KEY = "";
        final String CONSUMER_SECRET = "";
        final String TOKEN = "";
        final String SECRET = "";

        final Configuration config = new ConfigurationBuilder().setDebugEnabled(true)
                                                               .setOAuthConsumerKey(CONSUMER_KEY)
                                                               .setOAuthConsumerSecret(CONSUMER_SECRET)
                                                               .setOAuthAccessToken(TOKEN)
                                                               .setOAuthAccessTokenSecret(SECRET)
                                                               .build();
        TwitterFactory tf = new TwitterFactory(config);
        return tf.getInstance();
    }

    public List<String> searchTweets(String searchQuery) throws TwitterException {

        Twitter twitter = getTwitterInstance();
        Query query = new Query(searchQuery);
        query.setCount(100);
        QueryResult result = twitter.search(query);

        return result.getTweets()
                     .stream()
                     .map(Status::getText)
                     .collect(Collectors.toList());
    }
}
