import hashlib
import os
from urllib.parse import urlparse

from cbor import cbor


class Corpus:
    """
    This class is responsible for handling corpus related functionalities like mapping a url to its local file name
    """

    def __init__(self, corpus_base_dir):
        self.corpus_base_dir = os.path.join(corpus_base_dir, "")

    def get_file_name(self, url):
        """
        Given a url, this method looks up for a local file in the corpus and, if existed, returns the file address. Otherwise
        returns None
        """

        pd = urlparse(url)
        if pd.path:
            path = pd.path[:-1] if pd.path[-1] == "/" else pd.path
        else:
            path = ""
        url = pd.netloc + path + (("?" + pd.query) if pd.query else "")

        try:
            hashed_link = hashlib.sha224(url).hexdigest()
        except (UnicodeEncodeError, TypeError):
            try:
                hashed_link = hashlib.sha224(url.encode("utf-8")).hexdigest()
            except UnicodeEncodeError:
                hashed_link = str(hash(url))

        if os.path.exists(os.path.join(self.corpus_base_dir, hashed_link)):
            return os.path.join(self.corpus_base_dir, hashed_link)
        return None

    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus and return a dictionary representing
        the repsonse to the given url. The dictionary contains the following keys:

        url: the requested url to be downloaded
        content: the content of the downloaded url in binary format. None if url does not exist in the corpus
        size: the size of the downloaded content in bytes. 0 if url does not exist in the corpus
        content_type: Content-Type from the response http headers. None if the url does not exist in the corpus or content-type wasn't provided
        http_code: the response http status code. 404 if the url does not exist in the corpus
        is_redirected: a boolean indicating if redirection has happened to get the final response
        final_url: the final url after all of the redirections. None if there was no redirection.

        :param url: the url to be fetched
        :return: a dictionary containing the http response for the given url
        """

        file_name = self.get_file_name(url)
        if file_name is None:
            url_data = {
                "url": url,
                "content": None,
                "http_code": 404,
                "headers": None,
                "size": 0,
                "content_type": None,
                "is_redirected": False,
                "final_url": None
            }
        else:
            data_dict = cbor.load(open(file_name, "rb"))

            def get_content_type(data):
                if b'http_headers' not in data: return None

                hlist = data_dict[b"http_headers"][b'value']
                for header in hlist:
                    if header[b'k'][b'value'] == b'Content-Type':
                        return str(header[b'v'][b'value'])
                return None

            url_data = {
                "url": url,
                "content": data_dict[b'raw_content'][b'value'] if b'raw_content' in data_dict and b'value' in data_dict[b'raw_content'] else "",
                "http_code": int(data_dict[b"http_code"][b'value']),
                "content_type": get_content_type(data_dict),
                "size": os.stat(file_name).st_size,
                "is_redirected": data_dict[b'is_redirected'][b'value'] if b'is_redirected' in data_dict and b'value' in data_dict[b'is_redirected'] else False,
                "final_url": data_dict[b'final_url'][b'value'] if b'final_url' in data_dict and b'value' in data_dict[b'final_url'] else None
            }

        return url_data
