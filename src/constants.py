STREAM_INTERVAL = 3 # seconds between frames per feed
MAX_FRAMES = 5 # how many frames to send per feed
RANDOM_DELAY = True # add small random jitter
SHUFFLE_FRAMES = True # randomize order of images
USE_MULTIPROCESS = True # run feeds in parallel True=parallel, False=one by one

OUTPUT_DIR = "stream_input"
LOG_FILE = "feed_log.csv"

# camera feed paths considered
FEEDS = {
    "feed_1": "inSitu/1/video", 
    "feed_2": "inSitu/14/video",
    "feed_3": "inSitu/25/video",
    "feed_4": "inSitu/90/video",
    "feed_5": "inSitu/120/video",
}
