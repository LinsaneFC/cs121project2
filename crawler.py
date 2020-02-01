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
                        if url not in self.outlinks:
                            self.outlinks[url] = 1
                        else:
                            self.outlinks[url] += 1
                        domain = (urlparse(next_link).hostname)
                        if domain != '':
                            if domain not in self.subdomains:
                                self.subdomains[domain] = 1
                            else:
                                self.subdomains[domain] += 1
                        self.frontier.add_url(next_link)

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
        # if isinstance(html_data, bytes):
        #     html_data = url_data['content'].decode() #decode from bytes to string

        # If the url is not in the corpus or if content data only had whitespace (such as \n)
        # content_types.add(url_data['content_type'])
        html_data = html_data.strip()
        if html_data == "" or html_data == b'': 
            return outputLinks
        
        #file_obj1 = open("first.txt", 'w')
        #file_obj2 = open("second.txt", 'w')
        
        try:
            doc = html.fromstring(html_data)
        except etree.ParserError:
            #print("parse error first one", url_data['url'])
            #file_obj1.write(url_data['url'])
            
            new_html_data = ""
            for c in html_data.decode():
                if ord(c) >= 1 and ord(c) <= 127:
                    new_html_data += c
            try:
                doc = html.fromstring(new_html_data)
            except etree.ParserError:
                #print("parse error second one", url_data['url'])
                #file_obj2.write(url_data['url'])
                pass

            
        doc.make_links_absolute(url_data['url'])
        href_links = doc.xpath('//a/@href')
        outputLinks.extend(href_links)    

        return outputLinks


    # Checks if URL path has repeating folders
    def check_repeating_folders(self, url):
        url = str(url)
        folders = url.split("/")
        folders = list(filter(lambda x: x != "", folders))
        return len(set(folders)) != len(folders)


    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        url = str(url)
        if parsed.scheme not in set(["http", "https"]):
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
                    or re.match("/search/|/calendar/|/date/|/filter/|query", parsed.path.lower())\
                    or re.match("sid=|year=|date=|sort=|\?|&|%|\+|\$", url.lower())\
                    or self.check_repeating_folders(url)\
                    or url.count("&") > 2 or url.count("=") > 2 or url.count("%") > 2 \
                    or 'fano' in parsed.hostname\
                    or parsed.fragment == 'respond'\
                    or 'year' in parsed.fragment\
                    or 'html#' in url\
                    or '#comment' in url\
                    or '#branding' in url:
                if url != '':
                    self.traps.add(url)
                return False
        except:
            if url != '':
                self.traps.add(url)
            return False

        return True
