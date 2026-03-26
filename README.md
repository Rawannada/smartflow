# SmartFlow: Enterprise Logistics Intelligence & Real-Time Alerting Pipeline

###  Project Vision
In the high-stakes world of last-mile delivery (e.g., **Bosta**, **Noon**, **Amazon**), "Visibility" is the thin line between profit and loss. **SmartFlow** is an end-to-end data engineering ecosystem that transforms raw GPS pings and order lifecycle events into actionable business intelligence. The system proactively detects delivery bottlenecks and automates customer satisfaction workflows before delays escalate into cancellations.

---

###  System Architecture
The pipeline follows a **Modular Near Real-Time Batch Architecture** to balance high-performance processing with operational cost-efficiency.



1.  **Data Generation (The Street):** A custom **Python Multi-threaded Simulator** mimics 1,000+ couriers, generating real-time GPS coordinates, speed metrics, and order status updates.
2.  **Landing Zone (Raw Storage):** Incoming events are persisted as partitioned **JSON** files, simulating a cloud-native Data Lake environment.
3.  **Orchestration (The Brain):** **Apache Airflow** manages the end-to-end workflow, scheduling processing jobs every hour with built-in retry logic and data integrity checks.
4.  **Processing (The Engine):** **Apache Spark (PySpark)** handles heavy-duty computations:
    * **Geospatial Analysis:** Calculating Haversine distances between couriers and delivery targets.
    * **Anomaly Detection:** Identifying "Critical Stalls" (Speed = 0 for >10 mins on orders valued > 1,000 EGP).
5.  **Data Warehouse (The Truth):** **PostgreSQL** serves as the structured repository for enriched, cleaned, and categorized logistics data.
6.  **Transformation (The Logic):** **dbt (Data Build Tool)** applies software engineering rigor to SQL, creating modular, tested tables for business reporting.

---

###  Tech Stack

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Language** | Python 3.9+ | Simulation logic and PySpark scripting. |
| **Orchestrator** | Apache Airflow | Workflow DAG scheduling and monitoring. |
| **Processing** | Apache Spark | Distributed big data processing engine. |
| **Modeling** | dbt (Core) | SQL modeling, testing, and documentation. |
| **Database** | PostgreSQL | Centralized analytical storage. |
| **DevOps** | Docker & Compose | Containerization for environment parity. |

---

###  Core Business Logic

#### 1. Predictive Delay Detection
The Spark engine doesn't just move data—it evaluates risk. A record is flagged as `URGENT_DELAY` if:
`Speed == 0` **AND** `Elapsed Time > 10m` **AND** `Order Value > 1,000 EGP`
This enables the business to prioritize high-value asset protection.

#### 2. Geospatial Intelligence
Using Latitude/Longitude pings, the system dynamically calculates the **Estimated Time of Arrival (ETA)**. Unlike static estimates, this is updated every batch based on the courier's real-time average moving speed.

#### 3. Financial Risk Modeling
Using **dbt**, we calculate **"Revenue at Risk"**—the total monetary value of all orders currently flagged with critical delays that are statistically likely to be canceled.

---

###  Getting Started

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/Rawannada/smartflow](https://github.com/Rawannada/smartflow)
    cd smartflow
    ```
2.  **Spin up Infrastructure:**
    ```bash
    docker-compose up -d
    ```
3.  **Initiate Simulator:**
    ```bash
    python src/simulator/courier_gen.py
    ```
4.  **Trigger Pipeline:** Access the Airflow UI at `localhost:8080` and enable the `logistics_hourly_processing` DAG.

---

###  Roadmap
* **ML Integration:** Deploying Spark MLlib to predict traffic congestion patterns.
* **Live Monitoring:** Integrating **Streamlit** for a real-time fleet map visualization.
* **Cloud Scaling:** Migrating the storage and compute to **AWS (S3, EMR, Redshift)**.

---

###  Author
**Rawan Samy Nada**
* Senior Computer Science Student @ Tanta University 
