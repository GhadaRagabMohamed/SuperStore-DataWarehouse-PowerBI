# 🛒 SuperStore Data Warehouse & Power BI Dashboard

## 📌 Project Overview
This project demonstrates the design of a **Data Warehouse** in SQL Server with a complete **ETL process** and interactive **Power BI dashboards** for the popular **SuperStore dataset**.  
The goal is to analyze sales, profit, customers, and product performance through a well-structured data model and intuitive dashboards.

---

## 🛠️ Tech Stack
- **SQL Server** → Data Warehouse, ETL, Stored Procedures  
- **Power BI** → Interactive Dashboards, KPIs, Visualizations  
- **Data Modeling** → Star Schema (Fact + Dimensions)  

---

## 📂 Project Structure
- `ProjectDataAnalysis.sql` → SQL script:
  - Create dimensions & fact tables  
  - ETL process with **Stored Procedure**  
  - Data loading & updating using **MERGE**  
  - Performance optimization with **Indexes**  
- `SuperStoreAnalysis.pbix` → Power BI dashboard file  
- `Screenshots/` → Preview of dashboard visuals  

---

## 📌 Dataset
- **Name**: SuperStore Dataset (Sample Superstore)  
- **Source**: [Kaggle - Superstore Sales Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)  

---

## 🚀 How to Use
1. Run the `ProjectDataAnalysis.sql` script in **SQL Server** to create dimensions & fact tables and execute the ETL process.  
2. Load the sample data into **staging tables**.  
3. The stored procedure will handle loading and updating data using **MERGE** and create necessary **indexes**.  
4. Open the `SuperStoreAnalysis.pbix` file in **Power BI** to explore dashboards.  

---

## 📈 Sample Dashboards
Here are some previews of the dashboards included in the project:  

📊 **Sales & Profit Analysis**  
📊 **Customer Analysis**  
📊 **Product Analysis**  
