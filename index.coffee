Lambdaws 	= require 'lambdaws'
Configs 	= require './configs'
Stream 		= require 'user-stream'
Lawgs 		= require 'lawgs'
AWS 		= require 'aws-sdk'
Rx 			= require 'rx'

logger 		= Lawgs 'SuperbowlTweets'
s3 			= new (AWS.S3)

AWS.config.update Configs.AWS
Lambdaws.config Configs.LAMBDAWS
logger.settings.aws = Configs.AWS
logger.configure {}

tweetCompute = (s3key, tweetsCount, callback) ->
	aws = require 'aws-sdk'
	s3 = new (aws.S3)
	s3.getObject {Key: s3key,Bucket: 'superbowl-lambdaws'}, (err, data) ->
		FOOTBALL_LEXICAL_FIELD = ['touchdown','fumble','interception','turnover','field goal']
		plainTextTweets = data.Body.toString()

		keywords = FOOTBALL_LEXICAL_FIELD.filter((term) ->
			termRegEx = new RegExp(term, 'gi')
			termCount = (plainTextTweets.match(termRegEx) or []).length
			threshold = 0.55 * tweetsCount
			termCount >= threshold
		)

		callback keywords
		return
	return

notify = (notification, callback) ->
	aws = require 'aws-sdk'
	sns = new (aws.SNS)

	snsCallback = ->
	   	callback 'alerts sent'
	   	return

	snsData = 
    	Message: 'Super Bowl XLIX Alert: ' + notification
    	TopicArn: 'arn:aws:sns:us-east-1:246175073899:Superbowl'
  	sns.publish snsData, snsCallback
  	return

mapTweet = (tweet) ->
  	id: tweet.id
    username: if tweet.user then tweet.user.screen_name else ''
    text: tweet.text
    retweetId: if tweet.retweeted_status then tweet.retweeted_status.id else null
    timestamp: tweet.timestamp_ms
    hashtags: if tweet.entities then tweet.entities.hashtags.map(((h) -> h.text)) else null

cloudedTweetCompute = Lambdaws.create(tweetCompute, [], name: 'SUPERBOWLPROD')
cloudedNotify = Lambdaws.create(notify, [], name: 'NOTIFYPROD')

notifQueue 	= new (Rx.Subject)
stream 		= new Stream(Configs.TWITTER)
count 		= 0

stream.stream track: 'superbowl,seahawks,patriots'

notifQueue
	.filter((data) -> data.length > 0)
	.throttleFirst(60 * 1000)
	.subscribe (data) ->
	  msg = data.reduce(((a, c) ->
	    a + ' ' + c
	  ), '')
	  cloudedNotify msg, (err, data) ->
	    console.log err, data
	    return
	  return

Rx.Node.fromEvent(stream, 'data')
	.map(mapTweet)
	.tap((tweet) ->
		logger.log 'tweet', tweet
		return
	).bufferWithTimeOrCount(5000, 5000)
	.filter((tweets) ->
		tweets.length > 0
	).subscribe (tweets) ->
		key = "#{count++}.data"
		s3.putObject {Bucket: 'superbowl-lambdaws', Key: key, Body: JSON.stringify(tweets)}, (err, data) ->
			if err then console.log 'ERROR', err else console.log data
			return
		cloudedTweetCompute key, tweets.length, (err, keywords) ->
			notifQueue.onNext keywords
			return
		return

setTimeout (->
), 100000 * 100000
# Keep Alive