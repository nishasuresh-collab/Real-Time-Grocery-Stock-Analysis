# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument(
#     "--num_feeds",
#     type=int,
#     default=1,
#     help="Number of simulated camera feeds (1, 3, 5, etc.)"
# )

# args, unknown = parser.parse_known_args()

# NUM_FEEDS = args.num_feeds

# NUM_FEEDS = 7

STREAM_INTERVAL = 3 # seconds between frames per feed
MAX_FRAMES = 5    # how many frames to send per feed
RANDOM_DELAY = True # add small random jitter
SHUFFLE_FRAMES = True # randomize order of images
USE_MULTIPROCESS = True # run feeds in parallel True=parallel, False=one by one

OUTPUT_DIR = "stream_input"
LOG_FILE = "feed_log.csv"

# camera feed paths considered
# ALL_FEEDS = {
#     "feed_1": "inSitu/1/video", 
#     "feed_2": "inSitu/14/video",
#     "feed_3": "inSitu/25/video",
#     "feed_4": "inSitu/90/video",
#     "feed_5": "inSitu/120/video",
# }
ALL_FEEDS = [
    "inSitu/1/video", 
    "inSitu/2/video", 
    "inSitu/3/video", 
    "inSitu/4/video", 
    "inSitu/5/video", 
    "inSitu/14/video",
    "inSitu/25/video",
    "inSitu/90/video",
    "inSitu/120/video",
]

# FEEDS = {
#     f"feed_{i+1}": ALL_FEEDS[i]
#     for i in range(min(NUM_FEEDS, len(ALL_FEEDS)))
# }
