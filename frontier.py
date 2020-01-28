import logging
import os
from collections import deque
import pickle

logger = logging.getLogger(__name__)

class Frontier:
    """
    This class acts as a representation of a frontier. It has method to add a url to the frontier, get the next url and
    check if the frontier has any more urls. Additionally, it has methods to save the current state of the frontier and
    load existing state

    Attributes:
        urls_queue: A queue of urls to be download by crawlers
        urls_set: A set of urls to avoid duplicated urls
        fetched: the number of fetched urls so far
    """

    # File names to be used when loading and saving the frontier state
    FRONTIER_DIR_NAME = "frontier_state"
    URL_QUEUE_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "url_queue.pkl")
    URL_SET_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "url_set.pkl")
    FETCHED_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "fetched.pkl")


    def __init__(self):
        self.urls_queue = deque()
        self.urls_set = set()
        self.fetched = 0

    def add_url(self, url):
        """
        Adds a url to the urls queue
        :param url: the url to be added
        """
        if not self.is_duplicate(url):
            self.urls_queue.append(url)
            self.urls_set.add(url)

    def is_duplicate(self, url):
        return url in self.urls_set

    def get_next_url(self):
        """
        Returns the next url to be fetched
        """
        if self.has_next_url():
            self.fetched += 1
            return self.urls_queue.popleft()

    def has_next_url(self):
        """
        Returns true if there are more urls in the queue, otherwise false
        """
        return len(self.urls_queue) != 0

    def save_frontier(self):
        """
        saves the current state of the frontier in two files using pickle
        """
        if not os.path.exists(self.FRONTIER_DIR_NAME):
            os.makedirs(self.FRONTIER_DIR_NAME)

        url_queue_file = open(self.URL_QUEUE_FILE_NAME, "wb")
        url_set_file = open(self.URL_SET_FILE_NAME, "wb")
        fetched_file = open(self.FETCHED_FILE_NAME, "wb")
        pickle.dump(self.urls_queue, url_queue_file)
        pickle.dump(self.urls_set, url_set_file)
        pickle.dump(self.fetched, fetched_file)

    def load_frontier(self):
        """
        loads the previous state of the frontier into memory, if exists
        """
        if os.path.isfile(self.URL_QUEUE_FILE_NAME) and os.path.isfile(self.URL_SET_FILE_NAME) and\
                os.path.isfile(self.FETCHED_FILE_NAME):
            try:
                self.urls_queue = pickle.load(open(self.URL_QUEUE_FILE_NAME, "rb"))
                self.urls_set = pickle.load(open(self.URL_SET_FILE_NAME, "rb"))
                self.fetched = pickle.load(open(self.FETCHED_FILE_NAME, "rb"))
                logger.info("Loaded previous frontier state into memory. Fetched: %s, Queue size: %s", self.fetched,
                            len(self.urls_queue))
            except:
                pass
        else:
            logger.info("No previous frontier state found. Starting from the seed URL ...")
            self.add_url("http://www.ics.uci.edu/")

    def __len__(self):
        return len(self.urls_queue)

