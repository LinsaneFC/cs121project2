import logging
import re
from urllib.parse import urlparse

from lxml import html
from lxml import etree

logger = logging.getLogger(__name__)
# content_types = set()

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

        # For analytics
        self.subdomains = dict()
        self.outlinks = dict()
        self.downloaded = set()
        self.traps = set()
        self.max_words = ('', 0)
        
        self.wordOccur = dict()
        self.stopWords = []
        stopWords = open('stopwords.txt', 'r')
        for word in stopWords:
            self.stopWords.append(word.strip())

        


    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            if url_data['url'] != '':
                self.downloaded.add(url_data['url'])

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.outlinks[url] = 1 if url not in self.outlinks else (self.outlinks[url] + 1)                    
                        domain = (urlparse(next_link).hostname)
                        if domain != '':
                            self.subdomains[domain] = 1 if (domain not in self.subdomains) else (self.subdomains[domain] + 1)
                                
                        self.frontier.add_url(next_link)

        # # To check specific links one at a time
        # url = "http://mondego.ics.uci.edu/datasets/maven-contents.txt"
        # url_data = self.corpus.fetch_url(url)
        # links = self.extract_next_links(url_data)

        # Subdomains visited and how many URLs each one processed
        analytics1 = open('analytics1.txt', 'w')
        analytics1.write(f'Key: subdomain, number of URLs processed\n\n')
        for subdomain in self.subdomains:
            analytics1.write(f'{subdomain}, {self.subdomains[subdomain]}\n')
        analytics1.close()

        # Page with most valid out links
        analytics2 = open('analytics2.txt', 'w')
        if len(self.outlinks) == 0:
            analytics2.write('No pages found')
        else:
            most_outlinks = max(self.outlinks, key=self.outlinks.get)
            analytics2.write(f'The page {most_outlinks} had the most out links, with {self.outlinks[most_outlinks]} out links.')
        analytics2.close()

        # List of downloaded URLs
        analytics3a = open('analytics3a.txt', 'w', encoding='utf-8')
        analytics3a.write('List of downloaded URLs:\n\n')
        for url in self.downloaded:
            analytics3a.write(url + '\n')
        analytics3a.close()

        # List of identified traps
        analytics3b = open('analytics3b.txt', 'w', encoding='utf-8')
        analytics3b.write('List of identified traps:\n\n')
        for url in self.traps:
            analytics3b.write(url + '\n')
        analytics3b.close()

        # Page with most number of words
        analytics4 = open('analytics4.txt', 'w', encoding = 'utf-8')
        analytics4.write('Longest page URL:\t' + str(self.max_words[0]) + "\n")
        analytics4.write('Number of words on page:\t' + str(self.max_words[1]))
        analytics4.close()

        # Top 50 most common words
        analytics5 = open('analytics5.txt', 'w', encoding = 'utf-8')
        analytics5.write("Most occured 50 words and their frequencies:\n")
        currentCount = 0
        for word, count in {k : v for k, v in sorted(self.wordOccur.items(), key= lambda item: item[1], reverse = True)}.items():
            if currentCount >= 50:
                break
            if word not in self.stopWords:
                currentCount += 1
                analytics5.write('{:<30} {:>4}\n'.format(word, count))

        


    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        
        outputLinks = []
        html_data = url_data['content']
        html_data = html_data.strip()

        # If the url is not in the corpus or if content data only had whitespace (such as \n)
        # content_types.add(url_data['content_type'])
        # Also don't bother checking content if return http status is 404 or if content is empty
        if url_data['http_code'] == 404 or url_data['size'] == 0 or html_data in ["", b'']:
            return outputLinks

        try:
            doc = html.fromstring(html_data)
        except etree.ParserError: 
            # In the case where there is a parsererror due to non ascii text, strip out all 
            # non-ascii text and try again.  
            new_html_data = ""
            for c in html_data.decode():
                if ord(c) >= 1 and ord(c) <= 127:
                    new_html_data += c
            try:
                doc = html.fromstring(new_html_data)
            except etree.ParserError:
                pass

        doc.make_links_absolute(url_data['url'])
        href_links = doc.xpath('//a/@href')
        outputLinks.extend(href_links)  

        etree.strip_elements(doc, 'script')
        
        raw_text = doc.text_content().split()
        # Analytics 4
        page_text_len = len(raw_text)
        if  page_text_len > self.max_words[1]:
            self.max_words = (url_data['url'], page_text_len)

        # Analytics 5
        for word in raw_text:
            word = word.lower()
            if word.isalnum() and len(word) > 2:
                self.wordOccur[word] = 1 if word not in self.wordOccur else (self.wordOccur[word] + 1)


        return outputLinks


    # Checks if URL path has repeating folders
    def check_path(self, path):
        if re.search("/search/|/calendar/|/date/|/filter/|query", path) != None:
            return True
        folders = path.split("/")
        folders = list(filter(lambda x: x != "", folders))
        return len(set(folders)) != len(folders)

    def check_query(self, query):
        if re.search("sid=|year=|date=|sort=", query) != None:
            return True
        return query.count("&") > 2 or query.count("=") > 2 or query.count("%") > 2


    def check_fragment(self, fragment):
        return re.search("respond|comment|branding|year", fragment) != None

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        url = str(url)
        if parsed.scheme not in ["http", "https"]:
            if url != '':
                self.traps.add(url)
            return False
        try:
            if ".ics.uci.edu" not in parsed.hostname \
                   or re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
                if url != '':
                    self.traps.add(url)
                return False

        except TypeError:
            print("TypeError for ", parsed)
            return False

        try:
            if re.match(".*\.(sql*|ds_store|pdf)$", parsed.path.lower()) \
                    or self.check_path(parsed.path.lower()) \
                    or self.check_query(parsed.query.lower())\
                    or self.check_fragment(parsed.fragment.lower())\
                    or re.search("fano", parsed.hostname.lower()) != None\
                    or re.search("html#", url.lower()) != None:
                if url != '':
                    self.traps.add(url)
                return False
        except:
            if url != '':
                self.traps.add(url)
            return False

        return True
