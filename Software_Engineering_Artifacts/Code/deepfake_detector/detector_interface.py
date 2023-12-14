"""
*******************************************************
* This code handles the interfacing between the 
* DetectorCluster and the outside AWS ecosystem. 
* 
* It pulls media and metadata from S3 when kicked off 
* and passes this data along to the Detector.
* 
* It also passes detection output back to the lambda
* function that kicked off the DetectorTask.
*******************************************************
"""

import calendar, boto3, json, os, traceback
from datetime import datetime, timezone
from detector_processing.detector_processor import runDetector

dtstart = datetime.now()
dt = dtstart.replace(tzinfo=timezone.utc)
s3_resource = boto3.resource('s3')
lambda_client = boto3.client('lambda')
FUNCTION_NAME = ''
BUCKET_NAME = 'deepfake-detection-store'
S3_BUCKET = s3_resource.Bucket(BUCKET_NAME)
START = '%s, %s %s %s %s %s' % (dt.strftime("%A"), dt.day, 
    calendar.month_abbr[dt.month], dt.year, dt.time(), dt.tzname())
ALL_MODELS = [
    'models/final_111_DeepFakeClassifier_tf_efficientnet_b7_ns_0_36', 
    'models/final_555_DeepFakeClassifier_tf_efficientnet_b7_ns_0_19',
    'models/final_777_DeepFakeClassifier_tf_efficientnet_b7_ns_0_29',
    'models/final_777_DeepFakeClassifier_tf_efficientnet_b7_ns_0_31',
    'models/final_888_DeepFakeClassifier_tf_efficientnet_b7_ns_0_37',
    'models/final_888_DeepFakeClassifier_tf_efficientnet_b7_ns_0_40',
    'models/final_999_DeepFakeClassifier_tf_efficientnet_b7_ns_0_23'
]

""" ============ Entrance/Exit Functions ============ """
# Logs metadata about this sprecific instance of file
# access by the bot processor.
def logAccessMetadata():
    global FUNCTION_NAME
    s3_resource.Object(BUCKET_NAME, 'current_run_metadata.json').download_file('current_run_metadata.json')
    with open('current_run_metadata.json', 'r') as file:
        json_str = file.read()
        metadata = json.loads(json_str)
        FUNCTION_NAME = metadata['bot_function']
    print('------------------------------------------')
    print(FUNCTION_NAME, 'started the Deepfake Detector.')
    print('Process Started at: %s' % (START))
    print('------------------------------------------')

# Restore all values to defaults for the next run.
# Then quit the program and log results.
def teardownAndExit():
    # Remove the files that are no longer needed
    os.remove('current_run_metadata.json')
    for filename in os.listdir('videos_to_process'):
        os.remove('videos_to_process/'+filename)
    for filename in os.listdir('detector_processing/models'):
        os.remove('detector_processing/models/'+filename)
    # Exit the container process
    executionTime = datetime.now() - dtstart
    print('------------------------------------------')
    print('Detector Interface completed in %s seconds.' % 
    (round(executionTime.total_seconds(), 2)))
    print('------------------------------------------')


""" ============ Interface Helper Functions ============ """
# Downloads videos and video metadata from S3. Also,
# reads the metadata file to get the lambda funtion that
# started the detector container task.
def downloadMediaFromS3(media_location):
    print('Downloading media from S3...')
    for file in S3_BUCKET.objects.all():
        if media_location in file.key: 
            s3_resource.Object(BUCKET_NAME, file.key).download_file(file.key)
            print('DONE:', file.key)
    print('All media downloaded.')

# Downloads each of the specified models, that the system will 
# be using to make its predictions, from S3 into local memory.
def downloadModelsFromS3(models):
    print('Downloading models from S3...')
    for file in S3_BUCKET.objects.all():
        if file.key in models: 
            s3_resource.Object(BUCKET_NAME, file.key).download_file('detector_processing/'+file.key)
            print('DONE:', file.key)
    print('All models downloaded.')

# Transforms the raw detector output into a form that 
# the bots can recognize and work with. 
# Formmatted output takes the form:
# {
#   "event": "respond",
#   "results": [
#       {
#           "postID": "<Post ID>",
#           "prediction": "<Fake likelyhood decimal>"
#       }, ...
#   ]
# }
def formatOutput(output):
    formatted_output = {"event": "respond", "results": []}
    file_names = output['filenames']
    predictions = output['predictions']
    file_num = 0

    # NOTE: Assumes each video file is named after its post ID
    while file_num < len(file_names):
        file_name = file_names[file_num]
        prediction = predictions[file_num]
        post_id = file_name.replace('.mp4', '')
        result_json = {"postID": post_id, "prediction": round(prediction, 2)}
        formatted_output['results'].append(result_json)
        file_num += 1

    return json.loads(str(formatted_output).replace("'", '"'))

# Kicks off the respond process in the lambda function that
# kicked off this detection process. Passes formatted detection
# output to the lambda function for its responses.
def invokeBotLambdaFunction(detector_output):
    print('Handing output off to lambda...')
    invoke_async = 'Event'
    payload = json.dumps(detector_output)
    res = lambda_client.invoke(FunctionName=FUNCTION_NAME, InvocationType=invoke_async, Payload=payload)
    print('Output handed off. Response:', res)


""" ================= Main Function ================= """
# Main method. Runs the detection process when kicked 
# off by one of the bot processors.
def main(media_location='videos_to_process'): # TODO: this should be set by lambda
    try:
        models = ALL_MODELS[0:1]
        logAccessMetadata()
        downloadMediaFromS3(media_location)
        print('------------------------------------------')
        downloadModelsFromS3(models)
        print('------------------------------------------')
        output = runDetector(media_location, models)
        print('------------------------------------------')
        invokeBotLambdaFunction(formatOutput(output))
    except:
        traceback.print_exc()
    teardownAndExit()

if __name__ == "__main__":
    main()