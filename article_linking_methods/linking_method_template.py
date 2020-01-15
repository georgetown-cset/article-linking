import logging
import pickle
import apache_beam as beam
from subprocess import Popen, PIPE


class ExactMatchArticleLinker(beam.DoFn):
    # declare your own logger like this when implementing your transformer.
    log = logging.getLogger(__name__)

    def __init__(self, comparison_map_location):
        self.comparison_map_location = comparison_map_location
        self.comparison_map = None

    def start_bundle(self):
        if self.comparison_map is None:
            dl_cmd = f"gsutil cp {self.comparison_map_location} ."
            proc = Popen(dl_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            output, _ = proc.communicate()
            self.comparison_map = pickle.load(open(self.comparison_map.split("/")[-1]))

    def get_exact_matches(self, record, fields):
        id_to_num_matches = {}
        for field in fields:
            rf = record["ds_"+field]
            potential_matches = [] if rf not in self.comparison_map["wos_"+field] else self.comparison_map["wos_"+field][rf]
            for potential_match in potential_matches:
                if potential_match not in id_to_num_matches:
                    id_to_num_matches[potential_match] = 0
                id_to_num_matches[potential_match] += 1
        # return records where all fields were matched
        return [{"ds_id": record["ds_id"], "wos_id": match} for match in id_to_num_matches if
                id_to_num_matches[match] == len(fields)]

    def get_one_match(self, record, field):
        rf = record["ds_"+field]
        if (rf in self.comparison_map["wos_"+field]) and (len(self.comparison_map["wos_"+field].strip()) > 0):
            return self.comparison_map["wos_"+field][rf]

    def process(self, record):
        for match in self.get_exact_matches(record, ["title", "abstract"]):
            yield match

# TODO:
# 1.) run this pipeline successfully
# 2.) implement in-memory precision/recall/f1 calculator and calculate performance
# 3.) implement v2 with n^2 comparison on the remainder, partitioned first on title exact match


#    @abstractmethod
#    @staticmethod
#    def compare(article1: Dict, article2: Dict) -> bool:
#        '''
#        Compares Article1 to Article2
#        :param article1: Article to compare to article 2
#        :param article2: Article to compare to article 1
#        :return: True if this metric thinks article1 and article2 are the same, False otherwise
#        '''
#        pass
