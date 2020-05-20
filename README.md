# Machine-Learning; Anomaly detection

In this project we are trying to solve an anomaly detection problem in traffic streaming data. In order to do that, we apply the Robust Random Cut Forest algorithm, designed for tasks such as this. 

We have two different sources of data:

* Traffic data: Historic traffic data values, measured every 5 minutes.
* Incident data: Registered incidents due to anomalies in historic data.

We show two differents approaches for calculating the threshold. If there are sufficient incident data registered, we can use it to set the optimal threshold. On the other hand, if not enough data is available, we may use an statistical threshold.
