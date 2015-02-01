var Lambdaws    = require('lambdaws'),
    Configs     = require('./configs'),
    Stream      = require('user-stream'),
    Lawgs       = require('lawgs'),
    AWS 		= require('aws-sdk'),
    Rx          = require('rx');

Lambdaws.config(Configs.LAMBDAWS);

var notifQueue = new Rx.Subject();

AWS.config.update(Configs.AWS);
var s3 = new AWS.S3();

var logger = Lawgs('SuperbowlTweets');
logger.settings.aws = Configs.AWS;
logger.configure({});

function tweetCompute(s3key, tweetsCount, callback) {
	var AWS = require('aws-sdk');
	var s3 = new AWS.S3();
	s3.getObject({ Key: s3key, Bucket: 'superbowl-lambdaws'}, function(err, data) {
		var FOOTBALL_LEXICAL_FIELD = ['touchdown', 'fumble', 'interception', 'turnover', 'field goal'],
		plainTextTweets = data.Body.toString();
		var keywords = FOOTBALL_LEXICAL_FIELD.filter(function(term) {
			var termRegEx = new RegExp(term, 'gi'),
			termCount = (plainTextTweets.match(termRegEx) || []).length,
			threshold = 0.55 * tweetsCount;
			return termCount >= threshold;
		});

		callback(keywords);
	});
}

function notify(notification, callback){
	var aws = require('aws-sdk');
  var sns = new aws.SNS();
  var snsCallback = function() {callback('alerts sent')};
  var snsData = {
    Message: 'Super Bowl XLIX Alert: ' + notification,
    TopicArn: 'arn:aws:sns:us-east-1:246175073899:Superbowl'
  }
  sns.publish(snsData, snsCallback);
}

var cloudedTweetCompute = Lambdaws.create(tweetCompute, [], { name: 'SUPERBOWLPROD' });
var cloudedNotify     = Lambdaws.create(notify, [], {name : 'NOTIFYPROD'});

function mapTweet(tweet){
  return {
    id : tweet.id,
    username : tweet.user ? tweet.user.screen_name : '',
    text : tweet.text,
    retweetId : tweet.retweeted_status ? tweet.retweeted_status.id : null,
    timestamp : tweet.timestamp_ms,
    hashtags: tweet.entities ? tweet.entities.hashtags.map(function(h) {return h.text;}) : ''
  }
}

var stream = new Stream(Configs.TWITTER);
stream.stream({track:'superbowl,seahawks,patriots'});
var count = 0;

notifQueue
	.filter(function(data) {return data.length > 0})
	.throttleFirst(60 * 1000)
	.subscribe(function(data) {
		var msg = data.reduce(function(a, c) { return a + ' ' + c }, '');
		cloudedNotify(msg, function(err, data) {
			console.log(err, data);
		});
	});

Rx.Node.fromEvent(stream, 'data')
  .map(mapTweet)
  .do(function(tweet) { logger.log('tweet', tweet) })
  .bufferWithTimeOrCount(5000, 5000)
  .filter(function(a) { return a.length > 0 })
  .subscribe(function(tweets) { 
  	var key = count++ + '.data';
  	s3.putObject({
  		Bucket: 'superbowl-lambdaws',
  		Key: key,
  		Body: JSON.stringify(tweets)
  	}, function(err, data) {
  		if(err) console.log('ERROR', err);
  		else console.log(data);
  	});
  	cloudedTweetCompute(key, tweets.length, function(err, keywords) {
  		notifQueue.onNext(keywords);
  	}) 
  } );

setTimeout(function() {}, 100000 * 100000); // Keep Alive