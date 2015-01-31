var	Lambdaws 	= require('lambdaws'),
	Configs		= require('./configs'),
	Stream 		= require('user-stream'),
	Rx			= require('rx');

Lambdaws.config(Configs.LAMBDAWS);

function tweetCompute(tweets){
	var FOOTBALL_LEXICAL_FIELD = [
		'touchdoun',
		'fumble',
		'interception',
		'turnover',
		'field goal'
	];

	var plainTextTweets = JSON.strignyfy(tweets);

	


	FOOTBALL_LEXICAL_FIELD.map(function(term){
		var termRegEx = new RegExp(term, 'g'),
			termCount = (termRegEx.match(plainTextTweets) || []).length,
			threshold = 0.5 * tweets.length
		
		return termCount >= threshold;
	});


	console.log('data has arrived:', JSON.stringify(data).length);
}

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
	.bufferWithTimeOrCount(1000, 1000)
	.filter(function(a){return a.length >0})
	.subscribe(cloudedTweetCompute);

setTimeout(function() {}, 1000 * 1000); // Keep Alive
