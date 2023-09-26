# Apache Spark Based Amazon Spam Review Classification

# Introduction
In the era of big data, efficient and scalable data processing is imperative for handling vast datasets and conducting sophisticated analytics.The performance of Apache Spark is a multifaceted aspect influenced by various factors related to cluster configuration, resource management, data processing, and workload characteristics.The primary objective of this project was to evaluate the efficiency and scalability of Apache Spark when confronted with large-scale datasets. The classification of Amazon reviews into ham and spam categories serves as a representative use case, given its relevance to e-commerce and online content moderation. 

Key performance metrics, such as training time and model accuracy, are meticulously recorded and analysed for each cluster setup. By utilizing Spark's distributed processing capabilities, we try to efficiently manage and process large volumes of textual data, allowing us to train and evaluate classification models effectively

# Objective
* To evaluate Apache Spark’s capabilities in managing and processing Big Data.
* To build a classification model that distinguishes between ham and spam reviews.
* To analyse the performance of the classification model by creating different clusters for achieving the best model training and prediction times

# Models
* Naive Bayes Classifier
* Logistic Regression

# Experimental Setup
The model was trained on stand-alone system by creating multiple clusters without GPU and on a stand-alone system without clusters but with GPU to find the optimal configurations. The model was then evaluated using the classification metrics.
- On Stand-alone systems by setting up Spark Clusters without GPU.
  * Required Libraries
  * Spark: 3.4.1
  * Hadoop: 3.0
  * PySpark: 3.4.1
  * pandas: 1.3.4
  * JDK: 17.0.8.1
- Using GPU
  * Spark 3.1.1
  * Hadoop: 3.2
  * PySpark: 3.1.1
  * Java: 8
 
# Dataset
For the model training we have chosen a secondary dataset comprising [Amazon product reviews](https://www.kaggle.com/datasets/naveedhn/amazon-product-review-spam-and-non-spam) for various categories. The dataset comprised reviews for Cell Phones and Accessories, Clothing, Shoes and Jewelry, Electronics, Home and Kitchen, Sports and Outdoors, Toys and Games. The data is stored in JSON form and contains 26.8 million reviews and 15.4 million reviewers.

We have chosen only the Clothing, Shoes and Jewelry category for training, which is of 3.21GB consisting of 55,04,331 data points. The class label is spam and not spam, where "0" indicates not spam and "1" indicates spam reviews.

# Results
* The data loading and session initialization did not vary much in terms of the number of Worker Nodes assigned. But the training workflow had significant difference. The training Workflow consisted of Pipeline for preprocessing the data, model fitting and model evaluation. For both the models, the training workflow time decreased by 30 minutes when the worker nodes were increased by one in each cluster. Thus, it suggests that the additional worker node contributed to parallel distributing processing leading to faster model training times.

* The second setup which consisted of GPU processing using the Kaggle Notebook without any Cluster setup had reduced training time compared to the first setup. The data loading and session initializer are comparatively much faster than the cluster setup. The training workflow of the GPU setup is similar to the cluster setup having two and three Worker Nodes respectively.

* When the same dataset was loaded on a single node computing system it took nearly 6.5 hours to load the data. Therefore Apache Spark proved to be efficient enough in handling large volumes of data at a faster rate due to its distributed framework.

# Conclusion

The current project focused on analysing the efficiency of data processing framework of Apache Spark in the field of Big Data and distributed computing. The classification model was
built on the 3.21GB dataset used in the analysis for spam review classification. The setups used in training the model proved that, when more Worker Nodes are added, the distributed computation and parallelism increases and, multiple tasks can be executed simultaneously speeding up the data processing and training tasks. The different setups proved that even without a GPU, the Spark can process and run on clusters of standard CPU through its distributed data processing.
Furthermore, the choice of GPU again depends on the type of data we are using the kind of models we would be using. GPUs would be useful when training deep learning models within a spark environment.

# References

* (https://www.kaggle.com/datasets/naveedhn/amazon-product-review-spam-and-non-spam)
* (Naveed Hussain, Hamid Turab Mirza, Ibrar Hussain, Faiza Iqbal, and Imran Memon, “Spam Review Detection using the Linguistic and Spammer Behavioral Methods,” IEEE Access (2020), United States, pp: 53801-53816, Vol: 8, Standard: 2169-3536.)
  

 
