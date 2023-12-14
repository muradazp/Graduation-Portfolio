"""
*******************************************************
* This code will handle the interfacing between 
* the Reddit & AWS APIs and our bot processor. 
* 
* It will listen to the Reddit API for user 
* activity and will call on the bot processor 
* when it hears something actionable (should do 
* some screening).
* 
* It will also be called on by the bot processor
* when the user input has been processed and a
* response is ready.
* 
* Python Reddit API Wrapper (PRAW): 
* https://praw.readthedocs.io/en/stable/
*******************************************************
"""

import praw, boto3, requests, time, traceback, json

ecs_client = boto3.client("ecs")
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
KEYWORDS = [  # TODO: Make this comprehensive
    "deepfake",
    "deep-fake",
    "depfake",
    "deepfke" "depfke",
    "deefake",
]
REDDIT_API = praw.Reddit(
    client_id="fake_id",
    client_secret="fake_secret",
    password="fake_password",
    user_agent="script:Deepfake Exposing Bot:v0.0.1 (by u/ExposeDeepFakes)",
    username="ExposeDeepFakes",
)
CLUSTER_NAME = "DetectorCluster"
TASK_DEF = "DetectorTask:1"
BUCKET_NAME = "deepfake-detection-store"
S3_BUCKET = s3_resource.Bucket(BUCKET_NAME)


""" ============ Helper Functions ============ """
# Restore all values to defaults for the next run.
# Then quit the program and log results.
def restoreS3Defaults():
    for file in S3_BUCKET.objects.all():
        if ".mp4" in file.key or ".json" in file.key:
            s3_client.delete_object(BUCKET_NAME, file.key)

# Create the metadata file that the system will refer 
# to when running the respond process. The detector also
# uses this file to determine which bot kicked it off.
def createMetadataFile(posts):
    json_str = str({"bot_function": "reddit-bot", "post_data": posts})
    json_obj = json_str.replace("'", '"')
    s3_client.put_object(
        Body=json_obj, Bucket=BUCKET_NAME, Key="current_run_metadata.json"
    )

# Take the comment and post metadata and bundle them
# into a usable object for the processor to work with.
def bundle(comment, post):
    return {
        "calloutMessage": comment.body,
        "postTitle": post.title,
        "postURL": post.url,
        "commentID": comment.id,
        "postID": post.id,
    }

# Goes through the comment body and returns True if
# a keyword is found, false otherwise.
def flagInComment(comment):
    words = comment.lower().split()
    i = 0
    for word in words:
        i += 1
        wlen = len(words)
        if word == "deep" and i > wlen and words[i] == "fake":
            return True
        for flag in KEYWORDS:
            if word == flag:
                return True
    return False


""" ========== Get Flagged Posts Functions ========== """
# Go through the bot's mentions to find instances where
# the bot was called on to do its job.
def screenMentions():
    mentions = []
    print("Screening bot mentions...")
    for message in REDDIT_API.inbox.unread(limit=None):
        subject = message.subject.lower()
        mention = "username mention"
        is_comment = isinstance(message, praw.models.Comment)
        if subject == mention and is_comment:
            post = REDDIT_API.submission(message.parent().id)
            if post.url: mentions.append(bundle(message, post))
            message.mark_read()
    print("Bot mentions screened. Found", len(mentions), "flags.")
    return mentions

# Go though the days top posts on r/all to find any
# comments that talk about deep fakes. Currently,
# getting the comments for each post takes ~3 seconds.
# Also, only works with reddit hosted videos right now.
def screenReddit():
    otherFlags = []
    subName = "all"
    p = 0
    postDepth = 1
    subTimeFilter = "day"
    subreddit = REDDIT_API.subreddit(subName)
    print("Screening posts...")
    print(
        "Subreddit: r/%s\nPost Depth: %s\nMethod: %s\nTimeframe: %s"
        % (subName, postDepth, "top", subTimeFilter)
    )
    for post in subreddit.top(time_filter=subTimeFilter):
        p += 1
        if len(post.url) > 0 and "v.redd.it" in post.url:
            post.comments.replace_more(limit=0)
            for comment in post.comments:
                if flagInComment(comment.body):
                    otherFlags.append(bundle(comment, post))
        if p % 10 == 0: print("%s posts screened." % (p))
        if p == postDepth: break
    print("r/%s screened. Found %s flags." % (subName, len(otherFlags)))
    return otherFlags

# Package the flagged posts that are found in a
# standard way for the processor code to use. Searches
# through flaggedPosts and, if duplicate posts are
# found, combines their comments under one post.
def packagePosts(flaggedPosts):
    print("Packaging posts...")
    packagedPosts = []
    for fPost in flaggedPosts:
        for pPost in packagedPosts:
            if fPost["postID"] == pPost["postID"]:
                pPost["commentIDs"].append(fPost["commentID"])
            else:
                packagedPosts.append(fPost)
        if len(packagedPosts) == 0:
            fPost["commentIDs"] = [fPost["commentID"]]
            packagedPosts.append(fPost)
    print("Posts packaged.")
    return packagedPosts

# Download each posts video, using the post media link,
# and save those videos to an S3 bucket. Return the
# number of media files downloaded.
def downloadPostMediaToS3(posts):
    print("Downloading media from Reddit to S3...")
    createMetadataFile(posts)
    for post in posts:
        try:
            session = requests.Session()
            video_url = post["postURL"] + "/DASH_1080.mp4?source=fallback"
            file_name = post["postID"] + ".mp4"
            object_name = "videos_to_process/" + file_name
            video = session.get(video_url, stream=True)
            with video as part:
                part.raw.decode_content = True
                conf = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=10000, max_concurrency=4
                )
                s3_client.upload_fileobj(
                    part.raw, BUCKET_NAME, object_name, Config=conf
                )
            print("Post %s media download SUCCEEDED" % (post["postTitle"]))
        except:
            print("Post %s media download FAILED" % (post["postTitle"]))
            traceback.print_exc()
    print("All post media downloaded.")
    return len(posts)

# Kick off the detector process and point the system
# towards the media in S3. Return True if the detector
# ackgnowledges the kick-off, false if otherwise.
def kickOffDetector():
    print("Starting deepfake detector...")
    network_config = {
        "awsvpcConfiguration": {
            "subnets": [
                "subnet-fake-1",
                "subnet-fake-2",
                "subnet-fake-3",
                "subnet-fake-4",
                "subnet-fake-5",
                "subnet-fake-6",
            ],
            "securityGroups": ["sg-fake"],
            "assignPublicIp": "ENABLED",
        }
    }
    try:
        response = ecs_client.run_task(
            cluster=CLUSTER_NAME,
            launchType="FARGATE",
            taskDefinition=TASK_DEF,
            count=1,
            platformVersion="LATEST",
            networkConfiguration=network_config,
        )
        print("Detector startup SUCCEEDED.")
    except:
        print("Detector startup FAILED.")
        traceback.print_exc()
    task_started = response["tasks"][0]["lastStatus"] == "PROVISIONING"
    return task_started


""" ======== Respond With Results Functions ========= """
# Gets the metadata for the current run from S3.
# This metadata file contains information about the
# posts that were found during the "search" process.
def getRunMetadata():
    print("Getting current run metadata...")
    metadata_obj = s3_resource.Object(BUCKET_NAME, "current_run_metadata.json")
    metadata_stream = metadata_obj.get()["Body"]
    metadata_string = ""
    for byte in metadata_stream:
        metadata_string += byte.decode()
    metadata_json = json.loads(metadata_string)
    print("Metadata retrieved and variables set.")
    return metadata_json["post_data"]

# Take the packaged result response messages and run
# any final processing necessary prior to posting.
def prepareResponses(results, posts):
    print("Preparing responses for posting...")
    responses = []
    for result in results:
        prediction = float(result["prediction"])
        percentage = str(round(prediction * 100)) + "%"
        response = "This video is %s likely to be fake." % (percentage)
        responses.append({"postID": result["postID"], "body": response})
        for post in posts:
            if post["postID"] == result["postID"]:
                for comment_id in post["commentIDs"]:
                    responses.append({"commentID": comment_id, "body": response})
    print("Responses are ready for posting.")
    return responses

# Interface with the Reddit API to post our array of
# responses. Each response should be posted:
# 1. As a reply to the flagging comment
# 2. As a direct comment on the main post
def postResponses(responses):
    i = 1
    rlen = len(responses)
    print("Posting %s responses to Reddit..." % (rlen))
    for response in responses:
        try:
            post_id = response["postID"]
            post = REDDIT_API.submission(post_id)
            post.reply(body=response["body"])
        except:
            comment_id = response["commentID"]
            comment = REDDIT_API.comment(comment_id)
            comment.reply(body=response["body"])
        print("Posted: %s/%s" % (i, rlen))
        # Need to sleep for 6 seconds between posts
        # so that we don't trigger the ratelimit
        if i != rlen: time.sleep(6)
        i += 1
    print("All responses posted.")
    return True


""" ============ Orchestration Functions ============ """
# Run the processes necessary to query the Reddit API
# to find our flagged posts and package them for the
# processor to use. Format of posts sent to Processor:
# {
#     "calloutMessage": "Comment Body",
#     "postTitle": "Post Title",
#     "postURL": "Post URL",
#     "commentID": "Comment ID",
#     "postID": "Post ID"
# }
def getFlaggedPosts():
    mentions = screenMentions()
    print("------------------------------------------")
    otherFlags = screenReddit()
    print("------------------------------------------")
    packagedPosts = packagePosts(mentions + otherFlags)
    return packagedPosts

# Run the processes necessary to prepare our packaged
# responses for posting and to then actually post those
# responses via the Reddit API.
def respondWithResults(results, posts):
    responses = prepareResponses(results, posts)
    print("------------------------------------------")
    responsesPosted = postResponses(responses)
    return responsesPosted
