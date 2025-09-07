# 🚀 Customer Behavior Analysis with PySpark 🚀

This project performs large-scale analysis of e-commerce customer data to uncover purchasing patterns, segment customers, and identify behavioral trends using Apache Spark's distributed computing capabilities via PySpark. The entire pipeline—from data generation to machine learning and insight reporting—is executed in a distributed manner, making it suitable for big data workloads.

---

## 📊 Project Pipeline

1. **Data Generation**  
   A scalable function generates a large synthetic dataset of customers and transactions directly within the Spark cluster, avoiding memory bottlenecks on the driver node.

2. **Feature Engineering**  
   Raw transaction data is enriched to create key metrics like Recency, Frequency, and Monetary (RFM) values, along with customer lifetime metrics.

3. **Customer Segmentation (RFM Analysis)**  
   Customers are segmented into distinct groups like "Champions," "Loyal Customers," and "At Risk" based on their RFM scores.

4. **Machine Learning Clustering**  
   K-Means clustering is applied to group customers into distinct personas based on demographic and behavioral features.

5. **Behavioral Pattern Analysis**  
   The analysis explores purchasing patterns related to the day of the week, product categories, and payment methods.

6. **Insights and Recommendations**  
   The final step summarizes key business metrics and provides actionable recommendations based on the analysis.

---

## 🛠 Technologies Used

- **Apache Spark & PySpark**: For distributed data processing and analytics.  
- **PySpark SQL**: For data manipulation and feature engineering.  
- **PySpark MLlib**: For K-Means clustering.  
- **Python**: For scripting and data generation logic.  

---

## 📊 Results & Key Insights

The analysis provides a comprehensive overview of the customer base. Sample outputs include customer segmentation and churn risk.  

## 🖼Output Preview
Here is a screenshot of the output console of Customer Behavior Analysis project:

![Dashboard Preview](https://github.com/sahil-analytics/big-data-analysis-pyspark/blob/main/screenshots/1_terminal_output_results.png)


**Key Findings:**

- **Customer Segmentation**: Identification of high-value "Champions" and "Loyal Customers" crucial for business revenue.  
- **Churn Risk**: Significant number of "At Risk" customers, allowing targeted retention campaigns.  
- **Behavioral Patterns**: Trends in weekend vs. weekday purchasing and popular product categories.

---


## ✅Conclusion
This project was created to demonstrate end-to-end data analysis and engineering using distributed computing with PySpark.

