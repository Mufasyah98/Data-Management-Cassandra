# 🎬 MovieLens 100k Analysis using Cassandra and Spark

## 📊 Project Overview

This project applies **Cassandra Query Language (CQL)** and **Apache Spark** to perform analytical queries on the **MovieLens 100k (ml-100k)** dataset. The objectives include:

- Calculating the average rating for each movie  
- Identifying the top ten movies with the highest average ratings  
- Finding users who have rated at least 50 movies and identifying their favourite movie genres  
- Finding users aged below 20  
- Finding users with the occupation “scientist” and aged between 30 and 40
- 
---

## 🛠️ Technology Stack

- **Hortonworks HDP Sandbox**: Version 2.6.5.0  
- **Apache Cassandra**: Version 3.0.9  
- **Apache Spark2**: Version 2.3.0  
- **PuTTY**: Version 0.81  
- **Apache Zeppelin**: Built-in with HDP for notebook interface  

---

## 🛢️ About Apache Cassandra

[Apache Cassandra](https://cassandra.apache.org/) is a highly scalable, distributed NoSQL database designed for large volumes of data across multiple nodes with no single point of failure.

Key features:
- High availability and fault tolerance
- Linear scalability
- Multi-datacenter replication
- SQL-like query language: CQL (Cassandra Query Language)

---

## 📁 Dataset Information

The [MovieLens 100k dataset](https://grouplens.org/datasets/movielens/100k/) contains 100,000 ratings from 943 users on 1,682 movies. The dataset files used:

- **`u.user`** – User metadata:  
  `user_id | age | gender | occupation | zip_code`

- **`u.data`** – Ratings data:  
  `user_id | movie_id | rating | timestamp`

- **`u.item`** – Movie metadata:  
  `movie_id | title | release_date | genres (e.g. action, drama, comedy, etc.)`

These files were parsed and loaded into Cassandra tables:
- `users`  
- `ratings`  
- `movies`

---

## 📌 Example Use Cases

- Real-time movie rating analytics  
- User segmentation based on behaviour and demographics  
- Genre popularity analysis  

---

## 📂 Folder Structure
