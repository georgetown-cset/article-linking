import logging
from typing import Dict
from .template import AbstractArticleLinkingMethod


class ExactTitleMatchLinker(AbstractArticleLinkingMethod):
    # declare your own logger like this when implementing your transformer.
    log = logging.getLogger(__name__)

    def set_cluster_key(self, article: Dict, key: str = "cluster_key") -> Dict:
        '''
        sets `key` in `article` to a cluster id for the article
        :param article: article to add a key to
        :param key: keyname to use for the cluster id
        :return: article with key specified
        '''
        article[key] = article["year"]
        return article

    def compare(self, article1: Dict, article2: Dict) -> bool:
        '''
        Compares Article1 to Article2
        :param article1: Article to compare to article 2
        :param article2: Article to compare to article 1
        :return: True if this metric thinks article1 and article2 are the same, False otherwise
        '''
        return self.simple_clean_string(article1["title"]) == self.simple_clean_string(article2["title"])