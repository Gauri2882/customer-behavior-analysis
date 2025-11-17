# -*- coding: utf-8 -*-
"""
Customer Shopping Behavior Analysis
Data Cleaning, Preprocessing, Feature Engineering & MySQL Upload
"""

import pandas as pd
from sqlalchemy import create_engine
from mysql.connector import connect

# -----------------------------
# Load Dataset
# -----------------------------
df = pd.read_csv("customer_shopping_behavior.csv")

# Basic checks
print(df.head())
print(df.info())
print(df.describe())
print(df.describe(include="all"))

# -----------------------------
# Data Cleaning
# -----------------------------

# Check missing values
print(df.isnull().sum())

# Fill missing Review Ratings with Category-wise median
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(
    lambda x: x.fillna(x.median())
)

print(df.isnull().sum())

# Standardize column names
df.columns = df.columns.str.lower().str.replace(' ', '_')
df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

print(df.columns)

# -----------------------------
# Feature Engineering
# -----------------------------

# Age groups using quantiles
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)

# Purchase frequency mapping
frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-Weekly': 14,
    'Annually': 365,
    'Every 3 Month': 90
}
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

# Check similarity between discount_applied & promo_code_used
same_values = (df['discount_applied'] == df['promo_code_used']).all()
print("Are both columns identical? ->", same_values)

# Dropped identical column
df = df.drop('promo_code_used', axis=1)

# -----------------------------
# Test MySQL Connection
# -----------------------------

conn = connect(
    host="localhost",
    user="username",            # Your MySQL username
    password="password",        # Your MySQL password
    database="test"         # Your database name
)

cursor = conn.cursor()
cursor.execute("SELECT VERSION()")
print("MySQL Version:", cursor.fetchone())
conn.close()

# -----------------------------
# Upload DataFrame to MySQL using SQLAlchemy
# -----------------------------

# Table name
table_name = "customer"

# Create engine
engine = create_engine("mysql+pymysql://username:password@localhost/test")

# Insert data
df.to_sql(table_name, engine, if_exists='replace', index=False)

print("Data inserted successfully!")
