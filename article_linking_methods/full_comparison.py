import logging
import os
import pickle
import json
import time
import apache_beam as beam
from subprocess import Popen, PIPE


class FullComparisonLinkerTitleFilter(beam.DoFn):
    log = logging.getLogger(__name__)
    empty = -1

    def __init__(self, id_map_location, threshold):
        self.id_map = None
        self.id_map_location = id_map_location
        self.threshold = threshold
        self.high_sim = (1+self.threshold)/2 # bigger than threshold, less than one

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
        super().start_bundle()
        if self.id_map is None:
            self.id_map = self.read_pickle(self.id_map_location, num_workers=2)

    def calculate_pct_overlap(self, txt1, txt2):
        if (len(txt1.strip()) == 0) or (len(txt2.strip()) == 0):
            # no point in proceeding further. We do _not_ consider two empty fields to have a perfect match!
            return self.empty
        longer = txt1 if len(txt1) > len(txt2) else txt2
        shorter = txt2 if len(txt2) < len(txt1) else txt1
        # return a high similarity so if there is a perfect match we'll take that instead
        overlap_sim = self.high_sim if (longer.find(shorter) > -1) and (len(shorter.split()) > 10) else 0
        words_to_counts = {}
        for txt in [txt1, txt2]:
            seen_words = set()
            for word in txt.strip().split():
                if word not in words_to_counts:
                    words_to_counts[word] = 0
                if word not in seen_words:
                    words_to_counts[word] += 1
                    seen_words.add(word)
        num_overlap = sum([1 for word in words_to_counts if words_to_counts[word] == 2])
        # the second argument to max will be 1 if all of the words that occur in the shorter string occur in the longer
        # it is possible this could false alarm for short strings matched against very long strings
        return max(overlap_sim, num_overlap/min([len(txt1.split()), len(txt2.split())]))

    @staticmethod
    def norm_ds(record, field):
        ds_f = record["ds_" + field]
        if (ds_f is None) or (len(ds_f) == 0):
            return ""
        if field.endswith("_names"):
            ds_f = " ".join(sorted([x.split()[-1] for x in ds_f if len(x.split()) > 0]))
        return ds_f

    def get_max_similarity(self, record, comparison_ids, comparison_fields):
        global_max, max_id = self.empty, None
        last_empty_id = None
        for cmp_id in comparison_ids:
            matched_title_or_abstract = False
            cmp_rec = self.id_map[cmp_id]
            similarities = []
            num_empty_fields = 0
            for field in comparison_fields:
                ds_f = self.norm_ds(record, field)
                sim = self.calculate_pct_overlap(ds_f, cmp_rec["wos_"+field])
                if (field == "last_names") and (sim >= 0.5):
                    # perhaps questionably, considering >= 1/2 of the authors matching a "high similarity" match
                    sim = min(sim, self.high_sim)
                if sim == self.empty:
                    num_empty_fields += 1
                else:
                    if field != "last_names":
                        matched_title_or_abstract = True
                    similarities.append(sim)
            if num_empty_fields == len(comparison_fields):
                last_empty_id = cmp_id
            else:
                this_max = sum(similarities)/len(similarities)
                if (this_max > global_max) and matched_title_or_abstract:
                    global_max = this_max
                    max_id = cmp_id
        #if max_id is not None:
        return global_max, max_id
        #return self.empty, last_empty_id

    def process(self, record_str):
        record = json.loads(record_str)
        max_sim, max_id = self.get_max_similarity(record, (k for k in self.id_map), ["title", "abstract", "last_names"])
        if max_sim > self.threshold:
            yield {"ds_id": record["ds_id"], "wos_id": max_id, "similarity": max_sim}
            return

