import logging
import time
import pickle
import os
import apache_beam as beam
from subprocess import Popen, PIPE


class ExactMatchArticleLinker(beam.DoFn):
    # declare your own logger like this when implementing your transformer.
    log = logging.getLogger(__name__)

    def __init__(self, comparison_map_location):
        self.comparison_map_location = comparison_map_location
        self.comparison_map = None

    @staticmethod
    def read_pickle(location, num_workers=1):
        loc_basename = location.split("/")[-1]
        loc_files = [fi for fi in os.listdir(os.getcwd()) if fi.startswith(loc_basename+"-") and fi.endswith(".pkl")]
        if len(loc_files) < num_workers:
            loc_path = f"{os.getcwd()}/{loc_basename}-{os.getpid()}"
            dl_cmd = f"gsutil cp {location} {loc_path}"
            proc = Popen(dl_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            output, _ = proc.communicate()
            return pickle.load(open(loc_path, mode="rb"))
        else:
            pkl = None
            max_delay = 5
            while max_delay > 0:
                try:
                    pkl = pickle.load(open(os.path.join(os.getcwd(), loc_files[0]), mode="rb"))
                except pickle.UnpicklingError:
                    max_delay -= 1
                    time.sleep(1)
            return pkl

    def start_bundle(self):
        if self.comparison_map is None:
            self.comparison_map = self.read_pickle(self.comparison_map_location)

    def get_exact_matches(self, record, fields):
        id_to_num_matches = {}
        for field in fields:
            rf = record["ds_"+field]
            potential_matches = []
            if rf in self.comparison_map["wos_"+field]:
                potential_matches = self.comparison_map["wos_"+field][rf]
            for potential_match in potential_matches:
                if len(potential_match.strip()) > 0:
                    if potential_match not in id_to_num_matches:
                        id_to_num_matches[potential_match] = 0
                    id_to_num_matches[potential_match] += 1
        # return records where all fields were matched
        return [{"ds_id": record["ds_id"], "wos_id": match} for match in id_to_num_matches if
                id_to_num_matches[match] == len(fields)]

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
