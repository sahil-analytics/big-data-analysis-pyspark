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

## 🖼Dashboard Preview
Here is a screenshot of the Customer Behavior Analysis project:

![Dashboard Preview](https://github.com/sahil-analytics/big-data-analysis-pyspark/blob/main/screenshots/1_terminal_output_results.png)


**Key Findings:**

- **Customer Segmentation**: Identification of high-value "Champions" and "Loyal Customers" crucial for business revenue.  
- **Churn Risk**: Significant number of "At Risk" customers, allowing targeted retention campaigns.  
- **Behavioral Patterns**: Trends in weekend vs. weekday purchasing and popular product categories.

---

## 🏃 Running the Project

### Prerequisites

- Java 8/11  
- Apache Spark 3.x  
- Python 3.7+  


### 2. Install Dependencies
It's recommended to use a virtual environment.
   ```

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
   ```

### 3. Run the PySpark Script
Use spark-submit to run the analysis script. Adjust the driver memory or executor cores as needed for your environment.
   ```

spark-submit customer_analysis.py
   ```

### Spark Performance & Monitoring
The script is designed to run efficiently on a distributed cluster. The Spark UI provides insights into job execution, task distribution, and resource utilization.

## Spark Jobs UI
The Jobs tab shows the high-level Spark jobs triggered by the PySpark script. Each job corresponds to an action in the code.

## Stages UI
Each job is broken down into stages. This view is useful for identifying performance bottlenecks in the execution plan.

## Executors UI
This view shows the distribution of tasks and storage across the cluster's worker nodes (executors).

> 💡 **This project was created to demonstrate end-to-end data analysis and engineering using distributed computing with PySpark.**

