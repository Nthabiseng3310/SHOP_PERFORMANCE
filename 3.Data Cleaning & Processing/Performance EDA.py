# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC IMPORT LIABRARIES

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib as mpl

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.DATA INGESTION

# COMMAND ----------

# calling our table with spark from databricks
customers=spark.table('workspace.default.customers_perfomance')

# converting our table to pandas

customers=customers.toPandas()

display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA CLEANING

# COMMAND ----------

# checking for number of rows and columns
customers.shape

# COMMAND ----------

# checking for structure and data types
customers.info()

# COMMAND ----------

# Checking for Data types
print(customers.dtypes)

# COMMAND ----------

# changing the Age type from float to in64
customers["Age"] = customers["Age"].astype('Int64')


# COMMAND ----------

# checking statisctics for the numerical column
customers["Age"].describe().T 

# COMMAND ----------

# (SIMALAR CODE) checking for statistical records on Age column
print(customers["Age"].min())

      


# COMMAND ----------

# checking for the mean Age
print(customers["Age"].mean())

# COMMAND ----------

# Checking for the maximum Age
print(customers["Age"].max())

# COMMAND ----------

# checking for missing values per column
print(customers.isnull().sum())

# COMMAND ----------

# checking for Column names
print(customers.columns)

# COMMAND ----------

# creating the Age group bucket
customers["Age_Group"]=pd.cut(customers["Age"],bins=[1,2,12,17,35,50,60,float("inf")],labels=["Infant","kids","Youth","Young Youth","Adult","Elder","Pensioner"])


# COMMAND ----------

# replacing the null or missing values on Age_Group column with mode number
customers["Age_Group"] = customers["Age_Group"].astype(object)
customers["Age_Group"] = customers["Age_Group"].fillna(customers["Age_Group"].mode()[0])

# COMMAND ----------

# replacing the null or missing values on City column with 'Unknown'
customers["City"]=customers["City"].fillna('Unknown')

# COMMAND ----------

# Checking for the number of duplicated values in a column
customers.duplicated().sum()

# COMMAND ----------

# checking for the number of  Unique  city names and thier value_count
print(customers["City"].value_counts())

# COMMAND ----------

# Standardize 'Mashad' to 'Mashhad'
customers["City"] = customers["City"].replace({"Mashad": "Mashhad"})

# COMMAND ----------

# Re-check the value counts.After Standardizing Mashhad
print(customers["City"].value_counts())

# COMMAND ----------

# checking for Unique Cust_Segmantations
print(customers["CustomerSegment"].unique())

# COMMAND ----------

# checking for the number of unique cust_segmantations
print(customers["CustomerSegment"].value_counts())

# COMMAND ----------

# Converting SignupDate to Date From Object
customers["SignupDate"]=pd.to_datetime(customers["SignupDate"],errors="coerce")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extracting New columns from the SignUpDate

# COMMAND ----------

# Creating the Year column
customers["SignupYear"]=customers["SignupDate"].dt.year

# COMMAND ----------

# Creating the month(number) Column
customers["SignupMonth"]=customers["SignupDate"].dt.month

# COMMAND ----------

# Creating the month_name column
customers["SignupMonth_name"]=customers["SignupDate"].dt.month_name

# COMMAND ----------

# Again Double check for missing values
print(customers.isnull().sum())

# COMMAND ----------

# Since I extracted a new Age Group bucket from the Age,  I can safely drop that column
# Drop the original Age column
customers = customers.drop(columns=["Age"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the Final clean_dataframe

# COMMAND ----------

# Display the new table
print(customers.head())

# COMMAND ----------

print(customers.shape)

# COMMAND ----------

customers["SignupMonth_name"] = customers["SignupDate"].dt.month_name()
display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC **ORDERS_TABLE**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Data ingestion

# COMMAND ----------

# calling our table with spark from databricks
orders=spark.table('workspace.default.orders_performance')

# converting our table to pandas

orders=orders.toPandas()

display(orders)

# COMMAND ----------

# checking for number of rows and coloumns
orders.shape

# COMMAND ----------

# checking for data structure and types
orders.info()

# COMMAND ----------

# converting OrderDate from Object to DateTime format
orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce")
orders = orders.drop(columns=["ordersDate"], errors="ignore")

# COMMAND ----------

# converting Quantity from float64 to Int64
orders["Quantity"]=orders["Quantity"].astype("Int64")

# COMMAND ----------

# Checking for the column names
print(orders.columns)

# COMMAND ----------

# checking for duplicates
print("Duplicates", orders.duplicated().sum())

# COMMAND ----------

# dropping the 120 duplicates
orders=orders.drop_duplicates()

# COMMAND ----------

# checking for missing values per column
print(orders.isnull().sum())

# COMMAND ----------

# replacing the null or missing values in the OrderDate column with the previous row date
orders["OrderDate"]=orders["OrderDate"].ffill()

# COMMAND ----------

# replacing the null or missing values in the quantity column with median
orders["Quantity"]=orders["Quantity"].fillna(orders["Quantity"].median())

# COMMAND ----------

# replacing the null or missing values in the Discount column with zero
orders["Discount"]=orders["Discount"].fillna(0)

# COMMAND ----------

# replacing the null or missing values in the payment method column with Unknown
orders["PaymentMethod"]=orders["PaymentMethod"].fillna('Unknown')

# COMMAND ----------

# checking for distinct payment method names
print(orders["PaymentMethod"].unique())

# COMMAND ----------

 #checking for distinct status
print(orders["Status"].unique())

# COMMAND ----------

# New Date Columns
orders["year"]=orders["OrderDate"].dt.year

# COMMAND ----------

# Creating month_name column
orders["Month_name"]=orders["OrderDate"].dt.month_name

# COMMAND ----------

# Creating Month number
orders["Month"]=orders["OrderDate"].dt.month

# COMMAND ----------

# extracting day_name
orders["Day_name"]=orders["OrderDate"].dt.day_name

# COMMAND ----------

# displaying the cleaned table
orders["Month_name"] = orders["OrderDate"].dt.month_name()
orders["Day_name"] = orders["OrderDate"].dt.day_name()
display(orders)

# COMMAND ----------

#in my cleaned orders table..print the rows and columns
print(orders.shape)

# COMMAND ----------

# DBTITLE 1,Merge orders and customers
# merging orders (left) with customers (base) on CustomerID
customers_orders = pd.merge(orders, customers, on="CustomerID", how="left")
display(customers_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PAYMENTS DATAFRAME

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Ingestion

# COMMAND ----------

# calling our table with spark from databricks
payments=spark.table('workspace.default.payments_performance')

# converting our table to pandas

payments=payments.toPandas()

display(payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the data

# COMMAND ----------

# checking for the first 5 rows
print(payments.head())

# COMMAND ----------

# checking for number of rows and columns
print(payments.shape)

# COMMAND ----------

# Checking for the column names
print(payments.columns)

# COMMAND ----------

#checking what kind of data it holds
print(payments.dtypes)

# COMMAND ----------

#converting paymentdate from object to date time
payments["PaymentDate"]=pd.to_datetime(payments["PaymentDate"],errors="coerce")

# COMMAND ----------

# checking for duplicates
print("duplicates;",payments.duplicated().sum())

# COMMAND ----------

# checking for missing values
print(payments.isnull().sum())

# COMMAND ----------

# replacing the null or missing values in the paymentDate column with the previous row date
payments["PaymentDate"]=payments["PaymentDate"].ffill()

# COMMAND ----------

print(payments["PaymentStatus"].value_counts())

# COMMAND ----------

# Displaying the cleaned table
display(payments)

# COMMAND ----------

# Merging the three tables
customers_orders_payments = pd.merge(
    customers,orders,
    payments,
    on="OrderID",
    how="left"
)

# COMMAND ----------

customers_orders_payments = pd.merge(payments, customers_orders, on="OrderID", how="left")
display(customers_orders_payments)

# COMMAND ----------

# Replace null values with the most common age group
customers_orders_payments["Age_Group"] = customers_orders_payments["Age_Group"].fillna(
    customers_orders_payments["Age_Group"].mode()[0]
)

display(customers_orders_payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products Table

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA INGESTION

# COMMAND ----------



# COMMAND ----------

# calling the products table with spark from databricks
products=spark.table('workspace.default.products_performance')

# converting our table to pandas

products=products.toPandas()

display(products)

# COMMAND ----------

# Checking for the first 5 rows
print(products.head())

# COMMAND ----------

# Checking howmany rows and columns my table has
print(products.shape)


# COMMAND ----------

# checking for the datatypes each coulmn has
print(products.dtypes)

# COMMAND ----------

# Checking for duplicates
print("Duplicates:", products.duplicated().sum())

# COMMAND ----------

# Checking for missing or null values
print(products.isnull().sum())

# COMMAND ----------

# Checking for unique Categories and thier values
print(products["Category"].unique())

# COMMAND ----------

#Checking how many values are there in each category
print(products["Category"].value_counts())

# COMMAND ----------

# Joing the four tables
Shop_Perfomance = pd.merge(customers_orders_payments, products, on="ProductID", how="left")
display(Shop_Perfomance)

# COMMAND ----------

# Drop the unwanted PaymentID and Signup columns
Shop_Perfomance = Shop_Perfomance.drop(
    columns=["PaymentID","SignupYear", "SignupMonth", "SignupMonth_name"],
    errors="ignore"
)

# COMMAND ----------

# Creating the Revenue column
Shop_Perfomance["Revenue"] = (
    Shop_Perfomance["Quantity"]
    * Shop_Perfomance["UnitPrice"]
    * (1 - Shop_Perfomance["Discount"])
)

# COMMAND ----------

# Displaying the Final Table
display(Shop_Perfomance)

# COMMAND ----------

# Rename the column and change its data type
Shop_Perfomance = Shop_Perfomance.rename(columns={"Revenue": "Revenue_ZAR"}).astype({"Revenue_ZAR": "int64"})


# COMMAND ----------

Shop_Perfomance.dtypes

# COMMAND ----------

# DBTITLE 1,Save Shop_Perfomance as table
# Save Shop_Perfomance as a new table in Unity Catalog
spark.createDataFrame(Shop_Perfomance).write.mode("overwrite").saveAsTable("workspace.default.shp_performance")
print("Table workspace.default.shp_performance saved successfully")

# COMMAND ----------

Shop_Perfomance = spark.table("workspace.default.shp_performance").toPandas()
display(Shop_Perfomance)