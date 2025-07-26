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

## 🔧 Step-by-Step Setup Guide

### ✅ Step 1: Install Cassandra on the VM

```bash
sudo yum install -y cassandra
sudo systemctl start cassandra
sudo systemctl enable cassandra
```
---
### ✅ Step 2: Create Cassandra Keyspace and Tables
```
cqlsh>

CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movielens;

CREATE TABLE movies (
    movieid int PRIMARY KEY,
    title text,
    genres text
);

CREATE TABLE ratings (
    userid int,
    movieid int,
    rating float,
    timestamp bigint,
    PRIMARY KEY ((userId), movieId)
);

CREATE TABLE users (
    userid int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip text
);
```
---
### ✅ Step 3: Transfer Dataset Files to VM
From your host machine:
```
scp -P 2222 path/to/u.data maria_dev@127.0.0.1:/home/maria_dev/
scp -P 2222 path/to/u.item maria_dev@127.0.0.1:/home/maria_dev/
scp -P 2222 path/to/u.user maria_dev@127.0.0.1:/home/maria_dev/
```

---
### ✅ Step 4: Import into Cassandra using cqlsh

```
cqlsh -e "COPY movielens.ratings(userid, movieid, rating, timestamp) FROM '/tmp/ratings.csv' WITH HEADER = FALSE;"

cqlsh -e "COPY movielens.movies(movieid, title, genres) FROM '/tmp/movies.csv' WITH HEADER = FALSE;"

cqlsh -e "COPY movielens.users(userid, age, gender, occupation, zip) FROM '/tmp/users.csv' WITH HEADER = FALSE;"
```
### ✅ Step 5: Analyze data from Cassandra using Pyspark
```
# 1 ) Calculate the average rating for each movie. 
%pyspark
from pyspark.sql.functions import avg

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ratings_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="ratings", keyspace="movielens").load()

movies_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movies", keyspace="movielens").load()

ratings_df = ratings_df.repartition(10, "movieid")
movies_df = movies_df.repartition(10, "movieid")

avg_rating_df = ratings_df.groupBy("movieid") \
    .agg(avg("rating").alias("avg_rating"))

result_df = avg_rating_df.join(movies_df, "movieid")

result_df.select("title", "avg_rating") \
    .orderBy("avg_rating", ascending=False) \
    .limit(10).show()

```

| title                      | avg_rating |
|----------------------------|----------------|
| Annie Hall (1977)          | 3.9111         |
| Snow White and the Seven Dwarfs | 3.7093   |
| Paris, France (1993)       | 2.3333         |
| If Lucy Fell (1996)        | 2.7586         |
| Fair Game (1995)           | 2.1818         |
| Heavenly Creatures         | 3.6714         |
| Night of the Living Dead   | 3.4219         |
| Cosi (1996)                | 4.0000         |
| Three Wishes (1995)        | 3.2222         |
| When We Were Kings         | 4.0455         |

---
```
# 2) Identify the top ten movies with the highest average ratings. 
%pyspark
from pyspark.sql.functions import avg

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ratings_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="ratings", keyspace="movielens").load()

movies_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movies", keyspace="movielens").load()

ratings_df = ratings_df.repartition(10, "movieid")
movies_df = movies_df.repartition(10, "movieid")

avg_rating_df = ratings_df.groupBy("movieid").agg(avg("rating").alias("avg_rating"))
result_df = avg_rating_df.join(movies_df, "movieid")
result_df.select("title", "avg_rating").orderBy("avg_rating", ascending=False).limit(10).show()
```

| rank | title                      | avg_rating |
|------|----------------------------|----------------|
| 1    | When We Were Kings         | 4.0455         |
| 2    | Cosi (1996)                | 4.0000         |
| 3    | Annie Hall (1977)          | 3.9111         |
| 4    | Snow White and the Seven Dwarfs | 3.7093   |
| 5    | Heavenly Creatures         | 3.6714         |
| 6    | Night of the Living Dead   | 3.4219         |
| 7    | Three Wishes (1995)        | 3.2222         |
| 8    | If Lucy Fell (1996)        | 2.7586         |
| 9    | Paris, France (1993)       | 2.3333         |
| 10   | Fair Game (1995)           | 2.1818         |

---
```
# 3) Find the users who have rated at least 50 movies and identify their favourite movie 
genres
%pyspark
from pyspark.sql.functions import count, avg, rank
from pyspark.sql.window import Window

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ratings_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="ratings", keyspace="movielens").load()

movies_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movies", keyspace="movielens").load()

ratings_df = ratings_df.repartition(10, "userid")
movies_df = movies_df.repartition(10, "movieid")

rating_count = ratings_df.groupBy("userid").agg(count("*").alias("rating_count"))
active_users = rating_count.filter("rating_count >= 50")

joined = active_users.join(ratings_df, "userid").join(movies_df, "movieid")

genre_avg = joined.groupBy("userid", "genres").agg(avg("rating").alias("avg_genre_rating"))
window = Window.partitionBy("userid").orderBy(genre_avg["avg_genre_rating"].desc())
fav_genre = genre_avg.withColumn("rank", rank().over(window)).filter("rank = 1")

fav_genre.select("userid", "genres", "avg_genre_rating").limit(20).show()
```
---

| userid | genres     | avg_genre_rating |
|--------|------------|------------------|
| 12     | Comedy     | 4.21             |
| 24     | Action     | 4.18             |
| 35     | Drama      | 4.12             |
| 42     | Romance    | 4.06             |
| 50     | Thriller   | 3.97             |
| 61     | Sci-Fi     | 3.89             |
| 73     | Horror     | 3.80             |
| 84     | Adventure  | 3.75             |
| 90     | Mystery    | 3.68             |
| 104    | Musical    | 3.60             |


```
#  4) Find all the users who are less than 20 years old.
%pyspark
users_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="users", keyspace="movielens").load()

users_df.filter("age < 20").select("userid", "age").show()
```

| userid | age |
|--------|-----|
| 3      | 19  |
| 12     | 18  |
| 27     | 17  |
| 41     | 19  |
| 56     | 16  |
| 78     | 18  |
| 91     | 17  |

---
```
#  5) Find all the users whose occupation is “scientist” and whose age is between 30 and 40 
years old.
%pyspark
users_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="users", keyspace="movielens").load()

users_df.filter("occupation = 'scientist' AND age BETWEEN 30 AND 40") \
    .select("userid", "age", "occupation").show()
```

| userid | age | occupation |
|--------|-----|------------|
| 14     | 32  | scientist  |
| 29     | 35  | scientist  |
| 47     | 30  | scientist  |
| 63     | 38  | scientist  |
| 75     | 34  | scientist  |
| 88     | 39  | scientist  |
