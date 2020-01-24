import logging
from .linking_method_template import ExactMatchArticleLinker


class FullComparisonLinkerTitleFilter(ExactMatchArticleLinker):
    log = logging.getLogger(__name__)
    empty = -1

    def __init__(self, comparison_map_location, id_map_location, threshold):
        super().__init__(comparison_map_location)
        self.id_map = None
        self.id_map_location = id_map_location
        self.threshold = threshold

    def start_bundle(self):
        super().start_bundle()
        if self.id_map is None:
            self.id_map = self.read_pickle(self.id_map_location, num_workers=2)

    def calculate_pct_overlap(self, txt1, txt2):
        if (len(txt1.strip()) == 0) or (len(txt2.strip()) == 0):
            return self.empty
        words_to_counts = {}
        for txt in [txt1, txt2]:
            for word in txt.strip().split():
                if word not in words_to_counts:
                    words_to_counts[word] = 0
                words_to_counts[word] += 1
        num_overlap = sum([1 for word in words_to_counts if words_to_counts[word] == 2])
        return num_overlap/len(words_to_counts)

    def get_max_similarity(self, record, comparison_ids, comparison_fields):
        max_sim, max_id = self.empty, None
        last_empty_id = None
        for cmp_id in comparison_ids:
            cmp_rec = self.id_map[cmp_id]
            sum_sim = 0
            num_empty_fields = 0
            for field in comparison_fields:
                sim = self.calculate_pct_overlap(record["ds_"+field], cmp_rec["wos_"+field])
                if sim == self.empty:
                    num_empty_fields += 1
                else:
                    sum_sim += sim
            if num_empty_fields == len(comparison_fields):
                last_empty_id = cmp_id
            else:
                avg_sim = sum_sim/(len(comparison_fields) - num_empty_fields)
                if avg_sim > max_sim:
                    max_sim = avg_sim
                    max_id = cmp_id
        if max_id is not None:
            return max_sim, max_id
        return self.empty, last_empty_id

    def process(self, record):
        matches = self.get_exact_matches(record, ["abstract", "title"])
        if len(matches) > 0:
            for match in matches:
                yield match
        else:
            title_matches = self.get_exact_matches(record, ["title"])
            max_sim, max_id, title_empty_id = -1, None, None
            if len(title_matches) > 0:
                # try looking at just the same titles
                wos_ids = [m["wos_id"] for m in title_matches]
                max_sim, max_id = self.get_max_similarity(record, wos_ids, ["abstract"])
                if max_sim == self.empty:
                    title_empty_id = max_id
            else:
                # back off to n^2 comparison, but not to the empty value, since if we get this far we want
                # evidence in at least one of the title or abstract
                max_sim, max_id = self.get_max_similarity(record, (k for k in self.id_map), ["title", "abstract"])
            # pass this in as an arg to init and (eventually) sweep the value
            if (max_sim > self.threshold) or (title_empty_id is not None):
                yield {"ds_id": record["ds_id"], "wos_id": title_empty_id if title_empty_id is not None else max_id}

