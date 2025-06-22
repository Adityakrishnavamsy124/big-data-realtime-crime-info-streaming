# 🚨 Real-Time Crime Data Streaming & Analysis Pipeline

This project implements a real-time streaming pipeline to collect, process, and store simulated crime data using a modern big data stack.

## 🛠️ Tech Stack

- **Kafka + Zookeeper** – Real-time ingestion and buffering of streaming crime data.
- **Apache Spark Structured Streaming** – Fault-tolerant, scalable real-time processing using micro-batches.
- **Delta Lake** – ACID-compliant storage with schema enforcement, time travel, and upsert support.
- **MySQL (optional)** – Used as an intermediate sink for BI integration.
- **Grafana / Power BI** – Real-time crime trend monitoring using SQL endpoint integration or MySQL views.

---

## ✨ Key Features

- Real-time data ingestion with Kafka producers simulating crime events.
- Spark Structured Streaming jobs consume Kafka topics and apply transformations.
- Delta format storage ensures data reliability, supports updates and schema evolution.
- Micro-batch writes to Delta Lake reduce overwrite risks and ensure atomic operations.
- Easily extendable to other use cases (e.g., fraud detection, real-time alerting).
- Output available for direct connection with BI tools via MySQL or connectors.

---

## 🧱 Architecture

1. **Collection & Messaging Tier**  
   Kafka producers simulate real-time crime data and push to Kafka topics, with Zookeeper managing cluster coordination.

2. **Analysis Tier (Processing)**  
   Spark Structured Streaming consumes and transforms data from Kafka in real-time, applying logic like filtering or aggregation.

3. **In-Memory/Delta Store Tier**  
   Output data is written to **Delta Lake** (append/update/complete modes) for fault-tolerant and consistent storage.

4. **Data Access Tier (Visualization)**  
   BI tools such as **Grafana** or **Power BI** connect via MySQL or directly via Spark SQL endpoints to visualize live crime trends.

---

## ⚙️ Requirements

- Apache Kafka and Zookeeper
- Apache Spark 3.x with Delta Lake support
- Java 8 JDK (set via `JAVA_HOME`)
- Python 3.x (for orchestration scripts)
- MySQL (optional, for BI integration)
- Grafana or Power BI (optional for dashboarding)

---

## 📊 Results

🔹 **Kafka Producer** — Sends crime messages at 500ms intervals:

![Kafka Producer](https://github.com/user-attachments/assets/0dc0d977-4780-4045-a663-4dae9c20b897)

---

🔹 **Spark Streaming** — Performs real-time processing and displays micro-batch analytics:

![Spark Streaming](https://github.com/user-attachments/assets/9ad4203b-44fb-4471-a398-c9bb6a070f8e)

---

🔹 **Delta Tables** — Saved in real-time with `append`, `update`, and `complete` output modes:

![Delta Lake Storage](https://github.com/user-attachments/assets/a8e0244f-5f40-4abc-a367-b2715c1e80c4)

---

## 📈 Possible Extensions

- Real-time alert system for high crime zones.
- Geospatial clustering of crime locations.
- Advanced analytics using ML models on streaming data.
