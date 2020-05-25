import json
import rrcf
import boto3


class AnomalyScorer:

    def __init__(self, bucket, key):
        """
        Constructor of the class.
        :param bucket: S3 bucket name where the json is stored.
        :param key: S3 resource key of the json file.
        """
        self.bucket = bucket
        self.key = key
        self.forest = self.create_forest()

    def create_forest(self):
        """
        Converts a dictionary containing Robust Random Cut Trees into a forest of them.
        :param forest_dict: Dictionary conatining Robust Random Cut Trees.
        :return: Forest consisting in a list of Robust Random Cut Trees.
        """
        s3 = boto3.resource('s3')
        obj = s3.Object(self.bucket, self.key)
        forest_dict = json.load(obj.get()['Body'])
        forest = []
        for i in range(len(forest_dict)):
            key_name = "tree_" + str(i + 1)
            new_tree = rrcf.RCTree()
            new_tree.load_dict(forest_dict[key_name])
            forest.append(new_tree)
        return forest

    def score_point(self, point_value, point_index):
        """
        Scores a point using Collusive Diplacement measure.
        :param point_value: Numeric value of the point.
        :param point_index: Index to identify the point. May be an string or a number.
        :param forest: List of Robust Random Cut Trees.
        :return: Score of the point.
        """
        point_codisp = 0
        for tree in self.forest:
            tree.insert_point(point_value, index=point_index)
            point_codisp += tree.codisp(point_index)
            tree.forget_point(point_index)
        point_score = point_codisp / len(self.forest)
        return point_score
