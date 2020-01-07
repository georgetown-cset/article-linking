import logging
import string
from typing import Dict
from abc import ABC, abstractmethod


class AbstractArticleLinkingMethod(ABC):
    # declare your own logger like this when implementing your transformer.
    log = logging.getLogger(__name__)

    @staticmethod
    def simple_clean_string(unclean_s):
        '''
        Basic string cleaning method - removes punctuation, downcases, strips
        :param unclean_s: raw string
        :return: cleaned string
        '''
        clean_s = unclean_s.strip()
        clean_s = clean_s.translate(str.maketrans('', '', string.punctuation))
        return clean_s.lower()

    @abstractmethod
    def set_cluster_key(self, article: Dict, key: str = "cluster_key") -> Dict:
        '''
        sets `key` in `article` to a cluster id for the article
        :param article: article to add a key to
        :param key: keyname to use for the cluster id
        :return: article with key specified
        '''
        pass

    @abstractmethod
    def compare(self, article1: Dict, article2: Dict) -> bool:
        '''
        Compares Article1 to Article2
        :param article1: Article to compare to article 2
        :param article2: Article to compare to article 1
        :return: True if this metric thinks article1 and article2 are the same, False otherwise
        '''
        pass