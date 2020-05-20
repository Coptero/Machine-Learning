import datetime
import numpy as np
import rrcf


def nearest_after(dates_list, date):
    """
    Returns the nearest date from a list of dates after a given one.
    :param dates_list: List containing all dates.
    :param date: Date which we want to obtain the nearest afer.
    :return: Nearest date after the given one.
    """
    date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    dates_list = [item for item in dates_list if item > date]
    return str(min(dates_list, key=lambda x: abs(x - date)))


def calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences, beta_value,
                      num_incidents):
    """
    Returns performance metrics for a certain anomaly threshold.
    :param anomaly_scores: An array of anomaly scores.
    :param incidents: Binary array where 1 values represent incident points.
    :param prev_points: Number of previous point to take into account for calculate performance metrics.
    :param threshold: Threshold for the anomaly scores.
    :param resolved_differences: List of differences in time points term between submit_dates and last_resolved_dates.
    :param beta_value: Beta of the F-score formula.
    :param num_incidents: Number of registered incidents present in the traffic series.
    :return: Multiple performance metrics.
    """
    tp = 0
    fn = num_incidents
    incidents_index = np.where(incidents == 1)[0]
    incidents_intervals_index = []
    for i in range(len(incidents_index)):
        incidents_intervals_index.extend(
            (range(incidents_index[i] - prev_points, incidents_index[i] + 1 + int(resolved_differences[i]))))
    for index in incidents_index:
        scores = anomaly_scores[list(range(index - prev_points, index + 1))]
        if np.any(scores > threshold):
            tp += 1
            fn -= 1
    fp = np.sum(anomaly_scores[~np.isin(np.arange(len(anomaly_scores)), incidents_intervals_index)] > threshold)
    recall = tp / (tp + fn)
    if fp == 0 and tp == 0:
        precision = 0
    else:
        precision = tp / (tp + fp)
    if precision == 0 and recall == 0:
        fbeta_score = 0
    else:
        fbeta_score = (1 + beta_value ** 2) * precision * recall / ((beta_value ** 2 * precision) + recall)
    return fp, fn, tp, recall, precision, fbeta_score


def optimal_fbeta(anomaly_scores, incidents, prev_points, resolved_differences, lower_threshold, upper_threshold,
                  beta_value, num_incidents):
    """
    Returns the threshold value from which we obtain the maximal Fbeta-score.
    :param anomaly_scores: An array of anomaly scores.
    :param incidents: Binary array where 1 values represent incident points.
    :param prev_points: Number of previous point to take into account for calculate performance metrics.
    :param resolved_differences: List of differences in time points term between submit_dates and last_resolved_dates.
    :param lower_threshold: Initial threshold to take into account.
    :param upper_threshold: Last threshold to take into account.
    :param beta_value: Beta of the F-score formula.
    :param num_incidents: Number of registered incidents present in the traffic series.
    :return: Optimal threshold value that maximizes Fbeta_score.
    """
    fbeta_score_values = []
    threshold_range = list(np.arange(lower_threshold, upper_threshold + 0.5, 0.5))
    for threshold in threshold_range:
        recall = calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences, beta_value,
                                   num_incidents)[3]
        precision = \
        calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences, beta_value,
                          num_incidents)[4]
        if precision == 0 and recall == 0:
            fbeta_score_value = 0
        else:
            fbeta_score_value = (1 + beta_value ** 2) * precision * recall / ((beta_value ** 2 * precision) + recall)
        fbeta_score_values.append(fbeta_score_value)
    optimal_threshold = threshold_range[np.argmax(fbeta_score_values)]
    return optimal_threshold


def optimal_fbeta_gradient(anomaly_scores, incidents, prev_points, resolved_differences, iniitial_threshold, beta_value,
                           iterations=10000, lr=0.5):
    """
    Returns the threshold value from which we obtain the maximal Fbeta-score using gradient-descent algorithm.
    :param anomaly_scores: An array of anomaly scores.
    :param incidents: Binary array where 1 values represent incident points.
    :param prev_points: Number of previous point to take into account for calculate performance metrics.
    :param resolved_differences: List of differences in time points term between submit_dates and last_resolved_dates.
    :param iniitial_threshold: Threshold used for the first iteration of the algorithm.
    :param beta_value: Beta of the F-score formula.
    :param iterations: Maximum number of iterations to perform.
    :param lr: Learning rate uses in every iteration.
    :return: Optimal threshold value that maximizes Fbeta_score.
    """
    fbeta_score_values = []
    threshold_values = []
    error_values = []
    threshold = iniitial_threshold
    recall = calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences)[3]
    precision = calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences)[4]
    if precision == 0 and recall == 0:
        fbeta_score_value = 0
    else:
        fbeta_score_value = (1 + beta_value ** 2) * precision * recall / ((beta_value ** 2 * precision) + recall)
    error = 1 / fbeta_score_value
    threshold_values.append(threshold)
    fbeta_score_values.append(fbeta_score_value)
    error_values.append(error)
    # print('Iteración número {}, threshold: {}, error: {}'.format(1, threshold, error))
    prev_threshold = threshold
    prev_error = error
    threshold = threshold + lr
    iterations -= 1
    for iteration in range(iterations):
        recall = calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences)[3]
        precision = calculate_metrics(anomaly_scores, incidents, prev_points, threshold, resolved_differences)[4]
        if precision == 0 and recall == 0:
            fbeta_score_value = 0
        else:
            fbeta_score_value = (1 + beta_value ** 2) * precision * recall / ((beta_value ** 2 * precision) + recall)
        if fbeta_score_value == 0:
            break
        error = 1 / fbeta_score_value
        threshold_values.append(threshold)
        fbeta_score_values.append(fbeta_score_value)
        error_values.append(error)
        if (error - prev_error) == 0:
            prev_threshold = threshold
            prev_error = error
            # print('Iteración número {}, threshold: {}, error: {}'.format(iteration, threshold, error))
            threshold = threshold + 0.1
        else:
            grad = (error - prev_error) / (threshold - prev_threshold)
            # print('Iteración número {}, threshold: {}, error: {}'.format(iteration, threshold, error))
            prev_threshold = threshold
            prev_error = error
            threshold = threshold - lr * grad
        iteration += 1
    optimal_threshold = threshold_values[np.argmin(error_values)]
    return optimal_threshold


def dict_to_forest(forest_dict):
    """
    Converts a dictionary containing Robust Random Cut Trees into a forest of them.
    :param forest_dict: Dictionary conatining Robust Random Cut Trees.
    :return: Forest consisting in a list of Robust Random Cut Trees.
    """
    forest = []
    for i in range(len(forest_dict)):
        key_name = "tree_" + str(i + 1)
        new_tree = rrcf.RCTree()
        new_tree.load_dict(forest_dict[key_name])
        forest.append(new_tree)
    return forest


def forest_to_dict(forest):
    """
    Converts a list of Robust Random Cut Trees (forest) to a dictionary.
    :param forest: List of Robust Random Cut Trees.
    :return: Dictionary of Robust Random Cut Trees.
    """
    forest_dict = {}
    for i in range(len(forest)):
        key_name = "tree_" + str(i + 1)
        forest_dict[key_name] = forest[i].to_dict()
    return forest_dict


def score_point(point_value, point_index, forest):
    """
    Scores a point using Collusive Diplacement measure.
    :param point_value: Numeric value of the point.
    :param point_index: Index to identify the point. May be an string or a number.
    :param forest: List of Robust Random Cut Trees.
    :return: Score of the point.
    """
    point_codisp = 0
    for tree in forest:
        tree.insert_point(point_value, index=point_index)
        point_codisp += tree.codisp(point_index)
        tree.forget_point(point_index)
    point_score = point_codisp / len(forest)
    return point_score
