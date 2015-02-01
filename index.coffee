Lambdaws = require 'lambdaws'
Configs  = require './configs'
Stream   = require 'user-stream'
Lawgs    = require 'lawgs'
Rx       = require 'rx'

Lambdaws.config(Configs.LAMBDAWS)

logger = Lawgs('SuperbowlTweets')
logger.settings.aws = Configs.AWS
logger.configure({})

tweetCompute = (tweets, callback) =>
    AWS = require('aws-sdk')
    FOOTBALL_LEXICAL_FIELD = ['touchdown', 'fumble', 'interception', 'turnover', 'field goal']
    plainTextTweets = JSON.stringify(tweets)

    keywords = FOOTBALL_LEXICAL_FIELD.filter () =>
    	termRegEx = new RegExp(term, 'gi')
    	termCount = (plainTextTweets.match termRegEx || []).length
    	threshold = 0.55 * tweets.length;
    	return termCount >= threshold
    
    callback(keywords)

notify = (notification, callback) =>
	sns = require('aws-sdk').SNS()
	snsCallback = () -> callback 'alerts sent'
	snsData =
		Message: 'Super Bowl XLIX Alert: ' + notification
		TopicArn: 'arn:aws:sns:us-east-1:246175073899:Superbowl'

	sns.publish snsData, snsCallback

cloudedTweetCompute = Lambdaws.create(tweetCompute, [], { name: 'SUPERBOWL' })
cloudedNotify 		= Lambdaws.create(notify, [], { name : 'NOTIFY' })

mapTweet = (tweet) =>
	id : tweet.id
	username : if tweet.user then tweet.user.screen_name else ''
	text : tweet.text
	retweetId : if tweet.retweeted_status then tweet.retweeted_status.id else null
	timestamp : tweet.timestamp_ms

stream = new Stream Configs.TWITTER
stream.stream {track:'superbowl'} 

Rx.Node.fromEvent(stream, 'data')
	.map(mapTweet)
	.do((tweet) -> logger.log 'tweet', tweet )
	.bufferWithTimeOrCount(3000, 1000)
	.filter((tweets) -> tweets.length > 0)
	.subscribe((tweets) -> cloudedTweetCompute tweets, (keywords) ->
		console.log keywords	
	)

setTimeout(() -> '', 100000 * 100000) # Keep Alive
