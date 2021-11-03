#!/usr/bin/env python
# coding: utf-8

#  # TECHNICAL TEST : ANALYZE E-COMMERCE DATA
# 

# ### INSTRUCTIONS
You were recently hired by an E-commerce company. Your mission is to provide insights on sales.

There are four datasets :
* *Products*: a list of available products.
* *Items*: a list of items.
* *Orders*: a list of customer orders on the website.
* *Customers*: a list of customers.

Centralize your projet and code in a Github repository and provide the url once the test is completed.

**To Dos**

1. Create and insert data in an local PostgreSQL.
2. Each day we want to compute summary statistics by customers every day (spending, orders etc.)
    Create a script to compute for a given day these summary statistics.
3. Run that script over the necessary period to inject historic data. Then, identify the top customers
4. How many customers are repeaters ?
5. Package your script in Docker container so that it can be run each day. We expect a `docker-compose.yml` and a CLI to get stats for a specific day.

# ### Import Libraries

# In[15]:


import os
import pandas as pd
import seaborn as sns
import psycopg2
import matplotlib.pyplot as plt


# ### Import Datasets

# In[299]:


os.chdir(r'C:\Users\aurel\OneDrive\Bureau\data-engineering-test-2-\data_engineering_test\data')
os.getcwd()


# In[300]:


customer = pd.read_csv("customers2.csv")
items = pd.read_csv("items2.csv")
orders = pd.read_csv("orders2.csv")
products = pd.read_csv("products2.csv")


# In[301]:


print( customer.shape, items.shape, orders.shape, products.shape)


# ### Basic cleaning

# In[302]:


customer.isnull().sum()


# In[303]:


customer=customer[customer['customer_unique_id'].notna()]
customer=customer[customer['customer_city'].notna()]


# In[304]:


products.isnull().sum()


# In[305]:


products=products[products['product_category_name'].notna()]


# In[306]:


products.isnull().sum()


# In[307]:


items.isnull().sum()


# In[308]:


orders.isnull().sum()


# ### Change to correct format

# In[309]:


orders['order_date']=pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_date'] = orders['order_date'].dt.date
orders


# ### Merge datasets

# In[310]:


main = orders.merge(customer, on='customer_id', how='left')
main = main.merge(items, on='order_id', how='left')
main = main.merge(products, on='product_id', how='left')
main


# In[311]:


main['order_date_str']=main['order_date'].map(str)
main['year'] = main['order_date_str'].str[:4]
main['year'] = main['year'].astype(int)
main


# #### Clear errors in dataset

# In[312]:


main = main[main['year'] < 2022]
main


# ## Summary satistics

# #### How many orders by year ? 

# In[292]:


year_ = main.groupby(['year'])['price'].sum()
year_.sort_values().plot(kind='barh',y = ['year'])


# #### By day ? For2017,  the 10  most interesting days

# In[214]:


main2 = main[main['year'] == 2017]
chartmain2 = main2.groupby(['order_date'])['price'].sum()
chartmain2.sort_values().head(20).plot(kind='barh',y = ['order_date'])


# #### Top 10 types of product our customer spend more their money on

# In[230]:


group = main.groupby(['product_category_name_english'])['price'].sum()
group.sort_values(ascending=True).head(10).plot(kind='barh',y = ['product_category_name_english'])


# ### Who are our best customers ? 

# #### Top 5 who spend the most

# In[228]:


customers = main.groupby(['customer_id'])['price'].sum()
customers.sort_values(ascending=True).head(5).plot(kind='barh',y = ['customer_id'])


# #### Top 10 customers with many orders

# In[235]:


customers2 = main.groupby(['customer_id'])['order_id'].count()
customers2.sort_values(ascending=False).head(10).plot(kind='barh', x= ['order_id'], y = ['customer_id'])


# #### How many customers are repeaters ? 

# In[279]:


customers = main.groupby(['customer_id'])['order_id'].count()
r = customers[customers > 1].count()
tot = main['customer_id'].count()
ratio = round(r/tot*100,2)
print('There are', r, 'customers that are repeaters, (meaning that they ordered more than one of our products) out of', tot,'.','It is about', ratio, '% of our customers.' )

