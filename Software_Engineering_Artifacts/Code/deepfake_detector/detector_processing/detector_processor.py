"""
*******************************************************
* This code handles the processing of media files to 
* determine their probability of authenticity. 
* 
* It gets kicked off by the detector_interface and 
* uses one or more pre-trained models to process each
* media file provided by the interface.
* 
* It then passes the results back up to the interface 
* to be distributed out into the AWS ecosystem.
*******************************************************
"""

import os, re, torch, traceback
from detector_processing.kernel_utils import VideoReader, FaceExtractor, confident_strategy, predict_on_video_set
from detector_processing.classifiers import DeepFakeClassifier
from detector_processing.kernel_utils import VideoReader, FaceExtractor
from detector_processing.kernel_utils import confident_strategy, predict_on_video_set


""" ================= Helper Functions ================= """
# Load each of the specified models, that the system
# will be using to make its predictions, into pytorch.
def loadModels(models):
    loaded_models=[]
    models_dir='/detector_processing/'
    model_paths = [os.path.join(models_dir, model) for model in models]

    for path in model_paths:
        print("Loading model: {}".format(path))
        try:
            model = DeepFakeClassifier(encoder="tf_efficientnet_b7_ns")
            checkpoint = torch.load(os.getcwd()+path, map_location="cpu")
            state_dict = checkpoint.get("state_dict", checkpoint)
            model.load_state_dict({re.sub("^module.", "", k): v for k, v in state_dict.items()}, strict=True)
            model.eval()
            del checkpoint
            loaded_models.append(model)
            print('Load SUCCEEDED')
        except:
            print('Load FAILED')
            traceback.print_exc()
    return loaded_models

# Process each video file to determine its authenticity.
# Return the array of predictions to the orchestration method.
def makePredictions(media_dir, media_file_names, models):
    print("Predicting {} videos... (this can take a while)".format(len(media_file_names)))
    frames_per_video = 32
    video_reader = VideoReader()
    video_read_fn = lambda x: video_reader.read_frames(x, num_frames=frames_per_video)
    face_extractor = FaceExtractor(video_read_fn)
    input_size = 380
    strategy = confident_strategy
    return predict_on_video_set(face_extractor=face_extractor, input_size=input_size, models=models, strategy=strategy, 
                                frames_per_video=frames_per_video, videos=media_file_names, num_workers=6, test_dir=media_dir)


""" ================= Orchestaration Function ================= """
# Orchestrates the running of the necessary detection 
# process functions. Returns raw output to interface file.
def runDetector(media_dir, models):
    print('Running Detector...')
    media_file_names = sorted([x for x in os.listdir(media_dir) if x[-4:] == ".mp4"])
    loaded_models = loadModels(models)
    predictions = makePredictions(media_dir, media_file_names, loaded_models)
    print("Detector finished.")
    return {"filenames": media_file_names, "predictions": predictions}
