"""
*******************************************************
* This code will handle the processing of user 
* input and detector output. 
* 
* It will accept input from the Reddit interface
* and will process that input to be sent to the
* Detector interface. 
* 
* It will then wait to hear back from the Detector 
* interface before processing the detector output
* to send back to the Reddit interface, then on
* to the user.
*******************************************************
"""

import traceback, calendar, interface
from datetime import datetime, timezone

dtstart = datetime.now()
dt = dtstart.replace(tzinfo=timezone.utc)
START = "%s, %s %s %s %s %s" % (
    dt.strftime("%A"),
    dt.day,
    calendar.month_abbr[dt.month],
    dt.year,
    dt.time(),
    dt.tzname(),
)
EVENT = ""


""" ================ Entrance/Exit Functions ================= """
# Logs metadata about this sprecific instance of file
# access by the bot processor.
def logAccessMetadata():
    # TODO: add info on the event that started the bot
    print("------------------------------------------")
    print("Something started the Reddit bot processor.")
    print('Process: "%s"\nStarted at: %s' % (EVENT, START))
    print("Redit Bot Name:", interface.REDDIT_API.user.me())
    print("Bot Auth Scope:", interface.REDDIT_API.auth.scopes())
    print("------------------------------------------")

# Restore all values to defaults for the next run.
# Then quit the program and log results.
def teardownAndExit(error=""):
    if EVENT == "respond":
        interface.restoreS3Defaults()
    executionTime = datetime.now() - dtstart
    print(
        "Reddit %s process completed in %s seconds."
        % (EVENT, round(executionTime.total_seconds(), 2))
    )
    print("Defaults restored. Exited gracefully.")
    print("------------------------------------------")
    if len(error) > 0: return {"statusCode": 500, "body": error}        
    else: return {"statusCode": 200, "body": "Process complete: SUCCESS"}
        
# Handle any error that arrises by logging it.
# Then call teardownAndExit to quit the program.
def handleError(error):
    # Should send an email notifying us there was an error
    return teardownAndExit(error)


""" ============= Process Orchestration Functions ============= """
# Run all of the processes necessary to find flagged
# posts, download post media, and start the detector.
def runSearchProcess():
    posts = interface.getFlaggedPosts()
    print("------------------------------------------")
    filesDownoaded = interface.downloadPostMediaToS3(posts)
    print("------------------------------------------")
    if filesDownoaded > 0:
        detectorStarted = interface.kickOffDetector()
        print("------------------------------------------")
        if not detectorStarted: return handleError("ERROR: detector not started")
        else: return teardownAndExit()
    else:
        print("No media found. Exiting without starting detector.")
        return teardownAndExit()

# Run all of the processes necessary to getthe detector
# output, package bot responses, and post those
# responses to Reddit.
def runRespondProcess(results):
    post_data = interface.getRunMetadata()
    print("------------------------------------------")
    responsesSent = interface.respondWithResults(results, post_data)
    print("------------------------------------------")
    if not responsesSent: return handleError("ERROR: responses not sent")
    else: return teardownAndExit()


""" ================= Main Function ================= """
# Main method. Runs either the search process or the
# respond process based on whether this is a scheduled
# run or a run kicked off by the detector.
def main(event_data):
    global EVENT
    EVENT = event_data["event"]
    logAccessMetadata()
    if EVENT == "search": return runSearchProcess()
    if EVENT == "respond": return runRespondProcess(event_data["results"])

# Used for running the bot in AWS Lambda
def lambda_handler(event, context):
    try: return main(event)
    except: return {"statusCode": 500, "body": "Unhandled Error:\n" + traceback.format_exc()}

# Used for running the bot in a local machine
if __name__ == "__main__":
    test_search = {"event": "search"}
    test_respond = {"event": "respond", "results": [{"postID": "yhh0vx", "prediction": "0.99"}]}
    main(test_respond)
