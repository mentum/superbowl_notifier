var Lambdaws    = require('lambdaws'),
    Configs     = require('./configs'),
    Stream      = require('user-stream'),
    Lawgs       = require('lawgs'),
    Rx          = require('rx');

Lambdaws.config(Configs.LAMBDAWS);

var logger = Lawgs('SuperbowlTweets');
logger.settings.aws = Configs.AWS;
logger.configure({});

function tweetCompute(tweets, callback) {
  var FOOTBALL_LEXICAL_FIELD = ['touchdown', 'fumble', 'interception', 'turnover', 'field goal', 'superbowl'],
      plainTextTweets = JSON.stringify(tweets),
      AWS = require('aws-sdk');

  var keywords = FOOTBALL_LEXICAL_FIELD.filter(function(term) {
    var termRegEx = new RegExp(term, 'gi'),
      termCount = (plainTextTweets.match(termRegEx) || []).length,
      threshold = 0.55 * tweets.length;
    return termCount >= threshold;
  });

  callback(keywords);
}

function notify(notification, callback){
  var sns = require('aws-sdk').SNS();
  var snsCallback = function() {callback('alerts sent')};
  var snsData = {
    Message: 'Super Bowl XLIX Alert: ' + notification,
    TopicArn: 'arn:aws:sns:us-east-1:246175073899:Superbowl'
  }
  sns.publish(snsData, snsCallback);
}

var cloudedTweetCompute = Lambdaws.create(tweetCompute, [], { name: 'SUPERBOWL' });
var cloudedNotify     = Lambdaws.create(notify, [], {name : 'NOTIFY'});

function mapTweet(tweet){
  return {
    id : tweet.id,
    username : tweet.user ? tweet.user.screen_name : '',
    text : tweet.text,
    retweetId : tweet.retweeted_status ? tweet.retweeted_status.id : null,
    timestamp : tweet.timestamp_ms
  }
}

var stream = new Stream(Configs.TWITTER);
stream.stream({track:'superbowl'});

Rx.Node.fromEvent(stream, 'data')
  .map(mapTweet)
  .do(function(tweet) { logger.log('tweet', tweet) })
  .bufferWithTimeOrCount(3000, 1000)
  .filter(function(a) { return a.length > 0 })
  .subscribe(function(tweets) { cloudedTweetCompute(tweets, function(err, keywords) { console.log(keywords) }) } );

setTimeout(function() {}, 100000 * 100000); // Keep Alive