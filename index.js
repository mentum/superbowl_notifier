var Lambdaws    = require('lambdaws'),
    Configs     = require('./configs'),
    Stream      = require('user-stream'),
    Lawgs       = require('lawgs'),
    Rx          = require('rx');

Lambdaws.config(Configs.LAMBDAWS);

function tweetCompute(tweets, callback) {
    var FOOTBALL_LEXICAL_FIELD = ['touchdown', 'fumble', 'interception', 'turnover', 'field goal'],
        plainTextTweets = JSON.stringify(tweets),
        AWS = require('aws-sdk');

	var sms = FOOTBALL_LEXICAL_FIELD.filter(function(term) {
		var termRegEx = new RegExp(term, 'gi'),
			termCount = (plainTextTweets.match(termRegEx) || []).length,
			threshold = 0.5 * tweets.length;
		return termCount >= threshold;
	}).reduce(function(sms, word) { return sms += word + ' ' }, '');

	if(sms.length > 0) {
		new AWS.SNS().publish({
			Message: 'Superbowl Alert: ' + sms,
			TopicArn: 'arn:aws:sns:us-east-1:246175073899:Superbowl'
		}, function() { callback('alerts sent') });
	} else callback('nothing special');
}

var cloudedTweetCompute = Lambdaws.create(tweetCompute, [], { name: 'SUPERBOWL' });

var logger = Lawgs('SuperbowlTweets');
logger.settings.aws = Configs.AWS;
logger.configure({});

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
stream.stream({track:'#superbowl'});

Rx.Node.fromEvent(stream, 'data')
	.map(mapTweet)
	.do(function(tweet) { logger.log('tweet', tweet) })
	.bufferWithTimeOrCount(3000, 1000)
	.filter(function(a) { return a.length > 0 })
	.subscribe(function(tweets) { cloudedTweetCompute(tweets, function() { /* Discard response */ }) } );

setTimeout(function() {}, 100000 * 100000); // Keep Alive
