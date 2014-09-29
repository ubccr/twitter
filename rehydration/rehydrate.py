#!/usr/bin/env python
# ====================================================================================================
# Rehydrate tweet ids using the twitter api. Tweets are exported in json format as an array of tweet
# objects.  A twitter application key and application secret are required to use the API. These are
# stored in the configuration file and can be found on your twitter application page
# (apps.twitter.com) under "Keys and Access Tokens".
#
# The configuration file has the format:
#
# [twitter]
# # Twitter access tokens from Twitter app tools (https://apps.twitter.com/)
# app_key = XXXXXXXXXX
# app_secret = XXXXXXXXXX
# # If access_token is not present the API will be used to retrieve a token for you.
# access_token = XXXXXXXXXX

# Due to Twitter terms of service limitations you cannot re-distribute tweets, but only tweet
# identifiers. Users must use the Twitter API to re-hydrate the tweets using the ids. This allows
# Twitter to delete or otherwise restrict tweets if necessary. Also, twitter does not allow
# applications to support programmatic access to twitter data but only a "bulk download" capability
# that is limited to 100k tweets per day.
#
# Twitter Rules (See https://dev.twitter.com/overview/terms/rules-of-the-road)
# Section I.4.1 (limit on downloadable datasets, redistribution)
#
# Twitter Terms of Service (See https://twitter.com/tos)
# Section 8
# "...you have to use the Twitter API if you want to reproduce, modify, create derivative works,
# distribute, sell, transfer, publicly display, publicly perform, transmit, or otherwise use the
# Content or Services."
#
# Twitter API documentation (See https://dev.twitter.com/rest/public)
# ====================================================================================================

import json
import csv
import argparse
import os
import sys
import time
import string
import ConfigParser

# http://twython.readthedocs.org
from twython import Twython, TwythonError, TwythonAuthError, TwythonRateLimitError

# ====================================================================================================
# Rehydrate tweet ids. Rehydrated tweets are written to the output file descriptor and missing tweet
# ids (tweets that were requested but not available) are written to the missing file descriptor.
# This uses the rate-limited Twitter API. For a description of Twitter rate-limiting see:
# https://dev.twitter.com/rest/public/rate-limiting
#
# @param twitterObj Twython object
# @param tweetIdList List of twitter tweet ids
# @param sleepSeconds Optional number of seconds to sleep between requests
#
# Global variables used:
# @global firstLine True if the first line of the output file has not been written yet
# @global outFd The output file descriptor
# @global missingFd The missing tweet file descriptor
#
# If we have hit the rate limit, catch the exception, sleep the required number of seconds, and
# re-submit the query
#
# @return A list of rehydrated tweets
# ====================================================================================================

def rehydrateTweets(twitterObj, tweetIdList, firstLine, sleepSeconds=None):

    if ( 0 == len(tweetIdList) ):
        return []

    sys.stderr.write("Processing chunk of " + str(len(tweetIdList)) + " tweets\n")

    apiUrl = 'https://api.twitter.com/1.1/statuses/lookup.json'
    idString = ','.join(tweetIdList)
    constructedUrl = twitterObj.construct_api_url(apiUrl, id=idString)

    # Twitter returns the number of requests available in the current time window in the header of
    # the last request. If we hit the limit we will need to sleep until the start of next window and
    # re-submit the request.
    
    requestProcessed = False

    while not requestProcessed:
        try:
            rehydratedTweets = twitterObj.get(constructedUrl)
            sys.stderr.write("Rehydrated " + str(len(rehydratedTweets)) + "/" + str(len(tweetIdList)) + " tweets\n")
            requestProcessed = True
            remaining = twitterObj.get_lastfunction_header("x-rate-limit-remaining")
            if ( None != remaining ):
                sys.stderr.write("Requests remaining in current window: " + remaining + "\n")
            
        except TwythonRateLimitError as e:
            # We've hit our rate limit for this window, sleep until the start of the next window and
            # resubmit the request
            
            resetTime = float(twitterObj.get_lastfunction_header("x-rate-limit-reset"))
            delta = int(resetTime) - time.time()
            sys.stderr.write("Hit rate limit for current window (ending " +
                             time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(resetTime)) +
                             "), sleep " + str(round(delta/60, 2)) + " minutes\n")
            time.sleep(delta + 1)
        except:
            raise
    # while not requestProcessed:

    # Check to see if we didn't get all of the requested tweets back, keep track of those too

    retrievedTweets = []
    if ( len(tweetIdList) != len(rehydratedTweets) ):
        for tweet in rehydratedTweets:
            retrievedTweets.append(tweet['id_str'])
        for missingTweetId in set(tweetIdList).difference(set(retrievedTweets)):
            missingFd.write("Missing tweet for id: " + missingTweetId + "\n");
        missingFd.flush()
    # if ( len(tweetIdList) != len(rehydratedTweets) ):

    # Save the rehydrated tweets
            
    for tweet in rehydratedTweets:
        if ( firstLine ):
            firstLine = True
        else:
            outFd.write(",\n")
        outFd.write(json.dumps(tweet, separators=(',',':')))
        outFd.flush()
    # for tweet in rehydratedTweets:

    # Be a good citizen and sleep before making another request
    
    if ( None != sleepSeconds ):
        sys.stderr.write("Sleeping " + str(sleepSeconds) + "s\n")
        time.sleep(sleepSeconds)

    return rehydratedTweets
# rehydrateTweets()

# ====================================================================================================

# Twitter access tokens from Twitter app tools (https://apps.twitter.com/)
twitterAppKey = None
twitterAppSecret = None
twitterAccessToken = None

appendExistingFile = False
inFd = None
outFd = None
missingFd = None

# --------------------------------------------------------------------------------
# Set up arguments

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", dest="configFile", metavar="FILE", required=True,
                    help="Configuration file")
parser.add_argument("-i", "--id-file", dest="idFile", metavar="FILE", required=True,
                    help="File containing tweet ids to rehydrate")
parser.add_argument("-m", "--missing-file", dest="missingFile", metavar="FILE",
                    required=False, help="File for tracking tweets that could not be retrieved")
parser.add_argument("-o", "--output-file", dest="outFile", metavar="FILE",
                    required=False, help="Output file")
parser.add_argument("-r", "--restart", dest="restartAtLine", metavar="NUM", type=int,
                    required=False, help="Restart the rehydration at this line in the id file")
parser.add_argument("-s", "--chunk-size", dest="chunkSize", metavar="NUM", type=int,
                    required=False, default=100, help="Number of tweet ids to process in one API call (max 100)")
parser.add_argument("-w", "--wait", dest="sleepSeconds", metavar="NUM", type=int,
                    required=False, default=1, help="Number of seconds to sleep between API requests")


args = parser.parse_args()

if not os.path.isfile(args.idFile):
    sys.stderr.write("Tweet ID file '" + args.idFile + "' not found\n")
    raise SystemExit

if ( not os.path.isfile(args.configFile) ):
    sys.stderr.write("Cannot read config file '" + args.configFile + "'\n")
    raise SystemExit

if ( args.chunkSize > 100 or args.chunkSize < 1 ):
    sys.stderr.write("Chunk size " + str(args.chunkSize) + " out of bounds, using 100\n")
    args.chunkSize = 100
    
# --------------------------------------------------------------------------------
# Read options from the config file. The twitter app key and secret are required to connect to the
# twitter API

try:
    config = ConfigParser.ConfigParser()
    config.read(args.configFile)
    twitterAppKey=config.get("twitter", "app_key")
    twitterAppSecret=config.get("twitter", "app_secret")
except ConfigParser.Error as e:
    sys.stderr.write("Error parsing configuration file: " + str(e) + "\n")
    raise SystemExit

# --------------------------------------------------------------------------------
# Open up input and output files

try:
    inFd = open(args.idFile, 'r')
except IOError:
    sys.stderr.write("Error opening input file: '", args.idFile, "' ", e.strerror, "\n")
    raise SystemExit
    
# Open up the output stream, default to stdout

if ( None == args.outFile ):
    outFd = sys.stdout
else:
    try:
        # Track if we are appending to an existing file so we can output the appropriate
        # strings at the beginning
        appendExistingFile = os.path.isfile(args.outFile)
        outFd = open(args.outFile, 'a')
    except IOError:
        sys.stderr.write("Error opening output file: '" + args.outFile + "' " + e.strerror + "\n")
        raise SystemExit

# Open up the missing tweet file, default to stderr

if ( None == args.missingFile ):
    missingFd = sys.stderr
else:
    try:
        missingFd = open(args.missingFile, 'a')
    except IOError:
        sys.stderr.write("Error opening missing file: '" + args.missingFile + "' " + e.strerror + "\n")
        raise SystemExit

# --------------------------------------------------------------------------------
# Connect to twitter

# We only need to get an access token if one has not already been provided.

if ( config.has_option("twitter", "access_token") ):
    twitterAccessToken = config.get("twitter", "access_token")
else:
    sys.stderr.write("Requesting Twitter access token\n")
    try:
        twitter = Twython(twitterAppKey, twitterAppSecret, oauth_version=2)
        twitterAccessToken = twitter.obtain_access_token()
        sys.stderr.write("Saving Twitter access token to config file' " + args.configFile + "'\n")
        config.set("twitter", "access_token", twitterAccessToken)
        with open(args.configFile, 'wb') as configfile:
            config.write(configfile)
    except (TwythonError, TwythonAuthError, TwythonRateLimitError) as e:
        sys.stderr.write("Error:" + str(e) + "\n")
        raise SystemExit
    except:
        sys.stderr.write("Unexpected error:" + sys.exc_info()[0] + e.strerror + "\n")
        raise
    
# Set up our object using the access token.
try:
    twitter = Twython(twitterAppKey, access_token=twitterAccessToken)
except (TwythonError, TwythonAuthError, TwythonRateLimitError) as e:
    sys.stderr.write("Error:" + str(e) + "\n")
    raise SystemExit
except:
    sys.stderr.write("Unexpected error:" + sys.exc_info()[0] + e.strerror + "\n")
    raise

# --------------------------------------------------------------------------------
# Rehydrate the tweets and dump them in compactly-formatted json objects separated by a comma.

# List of tweet ids to rehydrate
tweetIdList = []

# Number of lines processed from input file
numLinesProcessed = 0

# Number of ids in the current chunk
linesInCurrentChunk = 0

# Number of chunks processed
numChunksProcessed = 0

# Keep track of whether or not we have printed the first line so we can add a comma to the end of
# each object
firstLine = True

# Skip lines if restarting

if ( None != args.restartAtLine ):
    sys.stderr.write("Restarting processing at line " + str(args.restartAtLine) + "\n")
    if ( 1 != args.restartAtLine ):
        firstLine = False
    while ( numLinesProcessed != ( args.restartAtLine - 1) and inFd.readline() ):
        numLinesProcessed += 1

# If we are appending an existing file via a restart, assume that a closing array brace was not
# written and add a coma to the end of the file rather than an opening array brace

if ( appendExistingFile and None != args.restartAtLine):
    outFd.write(",\n")
else:
    outFd.write("[\n")

# Rehydrate the tweets

for line in inFd:
    numLinesProcessed += 1
    linesInCurrentChunk += 1
    tweetIdList.append(line.strip())
    if ( linesInCurrentChunk == args.chunkSize ):
        try:
            rehydratedTweets = rehydrateTweets(twitter, tweetIdList, firstLine, args.sleepSeconds)
            if ( 0 != len(rehydratedTweets) ):
                firstLine = False
                
            # Reset counters
            sys.stderr.write("Processed " + str(numLinesProcessed) + " lines\n")
            tweetIdList = []
            linesInCurrentChunk = 0
            numChunksProcessed += 1
            
        except TwythonError as e:
            sys.stderr.write("Error:" + str(e) + "\n")
        except TwythonAuthError as e:
            sys.stderr.write("Auth Error:" + str(e) + "\n")
        except:
            raise
    # if ( 0 == linesInCurrentChunk % args.chunkSize ):
# for line in inFd:

# Process any remaining lines that don't make up a full chunk.

if ( 0 != len(tweetIdList) ):
     numChunksProcessed += 1
     try:
         rehydratedTweets = rehydrateTweets(twitter, tweetIdList, firstLine, args.sleepSeconds)
         if ( 0 != len(rehydratedTweets) ):
             firstLine = False

     except TwythonError as e:
         sys.stderr.write("Error:" + str(e) + "\n")
     except TwythonAuthError as e:
         sys.stderr.write("Auth Error:" + str(e) + "\n")
     except:
         raise
     
# if ( 0 != len(tweetIdList) ):

# Write the closing array brace

outFd.write("\n]\n")

sys.stderr.write("Processed " + str(numLinesProcessed) + " lines in " + str(numChunksProcessed) + " chunks\n")

# Cleanup

inFd.close()

if ( None != args.outFile ):
    outFd.close()

if ( None != args.missingFile ):
    missingFd.close()

raise SystemExit
