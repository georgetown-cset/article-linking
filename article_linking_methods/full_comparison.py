import logging
from .linking_method_template import ExactMatchArticleLinker


class FullComparisonLinkerTitleFilter(ExactMatchArticleLinker):
    log = logging.getLogger(__name__)

    def __init__(self, comparison_map_location, id_map_location):
        super().__init__(comparison_map_location)
        self.id_map = None
        self.id_map_location = id_map_location

    def start_bundle(self):
        super().start_bundle()
        if self.id_map is None:
            self.id_map = self.read_pickle(self.id_map_location)

    @staticmethod
    def calculate_pct_overlap(txt1, txt2):
        words_to_counts = {}
        for txt in [txt1, txt2]:
            for word in txt.strip().split():
                if word not in words_to_counts:
                    words_to_counts[word] = 0
                words_to_counts[word] += 1
        num_overlap = sum([1 for word in words_to_counts if words_to_counts[word] == 2])
        return num_overlap/len(words_to_counts)

    def get_max_similarity(self, record, comparison_ids, comparison_fields):
        max_sim, max_id = -1, None
        for cmp_id in comparison_ids:
            cmp_rec = self.id_map[cmp_id]
            sum_sim = 0
            for field in comparison_fields:
                sum_sim += self.calculate_pct_overlap(record["ds_"+field], cmp_rec["wos_"+field])
            avg_sim = sum_sim/len(comparison_fields)
            if avg_sim > max_sim:
                max_sim = avg_sim
                max_id = cmp_id
        return max_sim, max_id

    def process(self, record):
        matches = self.get_exact_matches(record, ["title", "abstract"])
        if len(matches) > 0:
            for match in matches:
                yield match
        else:
            title_matches = self.get_exact_matches(record, ["title"])
            if len(title_matches) > 0:
                # try looking at just the same titles
                max_sim, max_id = self.get_max_similarity(record, title_matches, ["title", "abstract"])
            else:
                # back off to n^2 comparison
                max_sim, max_id = self.get_max_similarity(record, self.id_map.keys(), ["title", "abstract"])
            # pass this in as an arg to init and (eventually) sweep the value
            if max_sim > 0.9:
                yield {"ds_id": record["ds_id"], "wos_id": max_id}




