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
        self.session = boto3.Session(profile_name="coptero")
        self.forest = self.create_forest()
        self.statistic_threshold = self.get_statistic_threshold()
        self.incident_threshold = self.get_incident_threshold()

    def get_statistic_threshold(self):
        s3 = self.session.resource('s3')
        obj = s3.Object(self.bucket, self.key)
        data_dict = json.load(obj.get()['Body'])
        statistic_threshold = data_dict["statistic_threshold"]
        return statistic_threshold

    def get_incident_threshold(self):
        s3 = self.session.resource('s3')
        obj = s3.Object(self.bucket, self.key)
        data_dict = json.load(obj.get()['Body'])
        if "incident_threshold" in data_dict.keys():
            incident_threshold = data_dict["incident_threshold"]
        else:
            incident_threshold = float("inf")
        return incident_threshold

    def create_forest(self):
        """
        Converts a dictionary containing Robust Random Cut Trees into a forest of them.
        :param forest_dict: Dictionary conatining Robust Random Cut Trees.
        :return: Forest consisting in a list of Robust Random Cut Trees.
        """
        s3 = self.session.resource('s3')
        obj = s3.Object(self.bucket, self.key)
        data_dict = json.load(obj.get()['Body'])
        forest = []
        for i in range(len(data_dict)):
            key_name = "tree_" + str(i + 1)
            if key_name in data_dict.keys():
                new_tree = rrcf.RCTree()
                new_tree.load_dict(data_dict[key_name])
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
        point_score = round(point_codisp / len(self.forest), 2)
        statistic_alarm = float(point_score > self.statistic_threshold)
        incident_alarm = float(point_score > self.incident_threshold)
        return point_score, statistic_alarm, incident_alarm
