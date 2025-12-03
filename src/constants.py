STREAM_INTERVAL = 4 # seconds between frames per feed
MAX_FRAMES = 8   # how many frames to send per feed
RANDOM_DELAY = True # add small random jitter
SHUFFLE_FRAMES = True # randomize order of images
USE_MULTIPROCESS = True # run feeds in parallel True=parallel, False=one by one

# NUM_FEEDS = 7 # Sending it directly when running the file

OUTPUT_DIR = "stream_input"
LOG_FILE = "feed_log.csv"

ALL_FEEDS = [
    "inSitu/1/video", 
    "inSitu/2/video", 
    "inSitu/3/video", 
    "inSitu/4/video", 
    "inSitu/5/video", 
    "inSitu/6/video", 
    "inSitu/7/video", 
    "inSitu/8/video", 
    "inSitu/9/video", 
    "inSitu/10/video", 
    "inSitu/11/video", 
    "inSitu/12/video",
    "inSitu/13/video",
    "inSitu/14/video",
    "inSitu/15/video",
    "inSitu/25/video",
    "inSitu/90/video",
    "inSitu/120/video",
]
