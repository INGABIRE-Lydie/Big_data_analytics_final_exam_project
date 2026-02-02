Big Data Analytics Final Exam Project

Project Overview



This project implements a distributed multi-model analytics system for large-scale e-commerce data. It demonstrates how MongoDB, HBase, and Apache Spark can be used together based on data structure, access patterns, and analytical needs.



All datasets were synthetically generated using Python (dataset\_generator.py) to simulate realistic e-commerce activity, including users, products, transactions, and browsing sessions.



**Technologies Used**



-MongoDB – structured transactional data



\-HBase – time-series session data



\-Apache Spark (PySpark) – batch and integrated analytics



\-Python – data generation and analysis



\-Docker – HBase environment



**Data Storage Design**



\-MongoDB stores users, products, categories, and transactions with optimized indexing.



\-HBase stores high-volume session data using row keys of the form user\_id#timestamp and product metrics as product\_id#date.



Only a subset of 5,000 sessions was loaded from HBase for Spark analytics to ensure performance.



**Analytics Performed**



\-Data cleaning and normalization



\-Revenue by category



\-Top products by revenue



\-Product co-occurrence (“users who bought X also bought Y”)



\-Customer Lifetime Value (CLV) analysis integrating:



User profiles (MongoDB)



Transactions (MongoDB)



Session engagement (HBase)



**Visualizations**



\-Engagement vs total spending (CLV)



\-Conversion funnel (browsed → abandoned → converted)



\-Revenue by category



\-Top products by revenue





**Conclusion**



The project shows how combining MongoDB, HBase, and Apache Spark enables scalable storage and advanced analytics for large e-commerce datasets, producing actionable business insights.

