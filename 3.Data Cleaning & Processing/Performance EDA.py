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

# printing rows where City is missing (null)
missing_city = customers[customers["City"].isnull()]
print(f"Number of missing City values: {missing_city.shape[0]}")
display(missing_city)

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

# replacing the null or missing values on City column with the mode (most mentioned city)
print(f"Missing City values before fillna: {customers['City'].isnull().sum()}")
customers["City"] = customers["City"].fillna(customers["City"].mode()[0])
print(f"Missing City values after fillna: {customers['City'].isnull().sum()}")
print(f"Mode city used: {customers['City'].mode()[0]}")

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

# Standardize 'tehran' to 'Tehran'
customers["City"] = customers["City"].replace({"tehran": "Tehran"})

# COMMAND ----------

# Re-check the value counts.After Standardizing Mashhad and Tehran
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

# calling the products table with spark from databricks
products=spark.table('workspace.default.products_performance')

# converting our table to pandas

products=products.toPandas()

display(products)

# COMMAND ----------

# Checking for the first 5 rows
print(products.head())

# COMMAND ----------

# Checking how many rows and columns my table has
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

# COMMAND ----------

# Calculate net revenue by order status
# Completed orders = positive revenue, Cancelled & Returned = deductions
net_revenue_by_status = (
    Shop_Perfomance.groupby("Status")["Revenue_ZAR"]
    .sum()
    .sort_values(ascending=False)
)
print("Revenue by Status:")
print(net_revenue_by_status)

# Net revenue = completed revenue minus cancelled & returned revenue
completed_revenue = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Completed", "Revenue_ZAR"].sum()
cancelled_revenue = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Cancelled", "Revenue_ZAR"].sum()
returned_revenue = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Returned", "Revenue_ZAR"].sum()

net_revenue = completed_revenue - cancelled_revenue - returned_revenue
print(f"\nCompleted Revenue:  R {completed_revenue:,.0f}")
print(f"Cancelled Revenue: R {cancelled_revenue:,.0f}")
print(f"Returned Revenue:  R {returned_revenue:,.0f}")
print(f"\nNet Revenue:       R {net_revenue:,.0f}")

# COMMAND ----------

# Checking for any possible missing values on the new dataframe
print((Shop_Perfomance).isnull().sum())

# COMMAND ----------

# Replace missing City values with the most common city (mode)
Shop_Perfomance["City"] = Shop_Perfomance["City"].fillna(
    Shop_Perfomance["City"].mode()[0]
)

# Verify no more missing values in City
print("Missing City values after fill:", Shop_Perfomance["City"].isnull().sum())

# COMMAND ----------

# Replace missing CustomerSegment values with the most common segment (mode)
Shop_Perfomance["CustomerSegment"] = Shop_Perfomance["CustomerSegment"].fillna(
    Shop_Perfomance["CustomerSegment"].mode()[0]
)

# Verify no more missing values in CustomerSegment
print("Missing CustomerSegment values after fill:", Shop_Perfomance["CustomerSegment"].isnull().sum())

# COMMAND ----------

# Replace missing SignupDate values with the most common SignupDate (mode)
Shop_Perfomance["SignupDate"] = Shop_Perfomance["SignupDate"].fillna(
    Shop_Perfomance["SignupDate"].mode()[0]
)

# Verify no more missing values in SignupDate
print("Missing SignupDate values after fill:", Shop_Perfomance["SignupDate"].isnull().sum())

# COMMAND ----------

# Count distinct city names
distinct_cities = Shop_Perfomance["City"].nunique()
print("Distinct city names:", distinct_cities)

# COMMAND ----------

# Standardize 'tehran' to 'Tehran'
customers["City"] = customers["City"].replace({"tehran": "Tehran"})

# COMMAND ----------

import matplotlib.pyplot as plt
df=orders.groupby('Status')['CustomerID'].nunique().reset_index()
display(df)

# COMMAND ----------

df.plot(kind='bar', x='Status', y='CustomerID', color='teal', legend=True)

plt.title("Customers who ordered")
plt.xticks(rotation=0)
plt.ylabel("Number of Customers")
plt.show()

# COMMAND ----------

# Pie chart: Payment Method by number of customers from the orders table
payment_counts = orders.groupby('PaymentMethod')['CustomerID'].nunique()

plt.figure(figsize=(8, 8))
plt.pie(payment_counts, labels=payment_counts.index, autopct='%1.1f%%', startangle=90, colors=plt.cm.Set3.colors)
plt.title("Payment Method by Number of Customers")
plt.axis('equal')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # QUESTION.5&6 **BUSINESS QUESTIONS AND VISUALIZATIONS**

# COMMAND ----------

# DBTITLE 1,QUESTION.5 Key Performance  Indicators
# KPI Cards (Net Revenue accounts for Completed, Cancelled & Returned orders)
import pandas as pd

completed = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Completed", "Revenue_ZAR"].sum()
cancelled = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Cancelled", "Revenue_ZAR"].sum()
returned = Shop_Perfomance.loc[Shop_Perfomance["Status"] == "Returned", "Revenue_ZAR"].sum()
net_revenue = completed - cancelled - returned

kpis = {
    "Net Revenue (ZAR)": f"R {net_revenue:,.0f}",
    "Completed Revenue (ZAR)": f"R {completed:,.0f}",
    "Cancelled Revenue (ZAR)": f"R {cancelled:,.0f}",
    "Returned Revenue (ZAR)": f"R {returned:,.0f}",
    "Average Order Value (ZAR)": f"R {Shop_Perfomance['Revenue_ZAR'].mean():,.0f}",
    "Total Quantity": f"{Shop_Perfomance['Quantity'].sum():,.0f}",
    "Distinct Categories": f"{Shop_Perfomance['Category'].nunique()}",
    "Record Count": f"{len(Shop_Perfomance):,}",
}

kpi_df = pd.DataFrame(list(kpis.items()), columns=["KPI", "Value"])
display(kpi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Which products and categories bring in the most revenue? Which sell the most units? Are these the same?**

# COMMAND ----------

import matplotlib.pyplot as plt

# Net revenue: Completed = positive, Cancelled & Returned = negative
net_rev = Shop_Perfomance["Revenue_ZAR"].where(
    Shop_Perfomance["Status"] == "Completed", -Shop_Perfomance["Revenue_ZAR"]
)

# Group net revenue by month and plot a line graph
monthly_net = net_rev.groupby(Shop_Perfomance["OrderDate"].dt.to_period("M")).sum()

plt.figure(figsize=(10, 6))
monthly_net.plot(kind="line", marker="o", color="teal")
plt.title("Net Revenue by Month")
plt.xlabel("Month")
plt.ylabel("Net Revenue (ZAR)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Net revenue: Completed = positive, Cancelled & Returned = negative
net_rev = Shop_Perfomance["Revenue_ZAR"].where(
    Shop_Perfomance["Status"] == "Completed", -Shop_Perfomance["Revenue_ZAR"]
)

# Group net revenue by Category and sort descending
category_net = net_rev.groupby(Shop_Perfomance["Category"]).sum().sort_values(ascending=False)

# Create a bar graph
plt.figure(figsize=(10, 6))
category_net.plot(kind="bar", color="teal")
plt.title("Net Revenue by Category")
plt.xlabel("Category")
plt.ylabel("Net Revenue (ZAR)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Net revenue: Completed = positive, Cancelled & Returned = negative
net_rev = Shop_Perfomance["Revenue_ZAR"].where(
    Shop_Perfomance["Status"] == "Completed", -Shop_Perfomance["Revenue_ZAR"]
)

# Group net revenue by ProductName and sort descending
product_net = net_rev.groupby(Shop_Perfomance["ProductName"]).sum().sort_values(ascending=False)

plt.figure(figsize=(10, 6))
product_net.plot(kind="bar", color="teal")
plt.title("Net Revenue by ProductName")
plt.xlabel("ProductName")
plt.ylabel("Net Revenue (ZAR)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Which cities and customer segments (New, Regular, VIP) are most valuable?

# COMMAND ----------

# DBTITLE 1,Customer_Segments
import matplotlib.pyplot as plt

net_rev = Shop_Perfomance["Revenue_ZAR"].where(Shop_Perfomance["Status"] == "Completed", -Shop_Perfomance["Revenue_ZAR"])
segment_net = net_rev.groupby(Shop_Perfomance["CustomerSegment"]).sum()

plt.figure(figsize=(7, 7))
plt.pie(segment_net, labels=segment_net.index, autopct="%1.1f%%", startangle=90, colors=plt.cm.Set3.colors)
plt.title("Net Revenue by Customer Segment")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Net Revenue by City
import matplotlib.pyplot as plt

# Standardize city names
Shop_Perfomance["City"] = Shop_Perfomance["City"].replace({"Mashad": "Mashhad", "tehran": "Tehran"})

# Net revenue: Completed = positive, Cancelled & Returned = negative
net_rev = Shop_Perfomance["Revenue_ZAR"].where(Shop_Perfomance["Status"] == "Completed", -Shop_Perfomance["Revenue_ZAR"])
city_net = net_rev.groupby(Shop_Perfomance["City"]).sum().sort_values(ascending=False)

plt.figure(figsize=(10, 6))
city_net.plot(kind="bar", color="teal")
plt.title("Net Revenue by City")
plt.xlabel("City")
plt.ylabel("Net Revenue (ZAR)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Cancelled/Returned/Completed_Orders
import matplotlib.pyplot as plt

# Count orders by status
status_counts = orders.groupby("Status")["OrderID"].nunique()

# Doughnut chart
fig, ax = plt.subplots(figsize=(7, 7))
wedges, texts, autotexts = ax.pie(
    status_counts,
    labels=status_counts.index,
    autopct="%1.1f%%",
    startangle=90,
    colors=plt.cm.Set3.colors,
    wedgeprops=dict(width=0.4),  # doughnut hole
)
ax.set_title("Share of Orders by Status")
plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Group by Discount: average quantity per order and total revenue
discount_summary = (
    Shop_Perfomance.groupby("Discount")
    .agg(AvgQuantity=("Quantity", "mean"), TotalRevenue=("Revenue_ZAR", "sum"))
    .reset_index()
    .sort_values("Discount")
)

fig, ax1 = plt.subplots(figsize=(10, 6))

# Line 1: Average quantity per order (left y-axis)
ax1.plot(discount_summary["Discount"], discount_summary["AvgQuantity"],
         color="teal", marker="o", label="Avg Quantity per Order")
ax1.set_xlabel("Discount")
ax1.set_ylabel("Avg Quantity per Order", color="teal")
ax1.tick_params(axis="y", labelcolor="teal")

# Line 2: Total revenue (right y-axis)
ax2 = ax1.twinx()
ax2.plot(discount_summary["Discount"], discount_summary["TotalRevenue"],
         color="salmon", marker="s", label="Total Revenue (ZAR)")
ax2.set_ylabel("Total Revenue (ZAR)", color="salmon")
ax2.tick_params(axis="y", labelcolor="salmon")

plt.title("Discount vs Order Size and Revenue")
fig.tight_layout()
plt.show()