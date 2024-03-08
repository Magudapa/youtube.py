# youtube.py
**YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit**


**Problem Statement:**
The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
 * Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
* Option to store the data in a MongoDB database as a data lake.
* Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
* Option to select a channel name and migrate its data from the data lake to a SQL database as tables. 
* Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.


**Getting Started**
Install/Import the necessary modules: Streamlit, Pandas, PyMongo, Psycopg2, Googleapiclient, and Isodate.
Ensure you have access to MongoDB Atlas and set up a PostgresSQL DBMS on your local environment.

**Methods:**

 * Get YouTube Channel Data: Fetches YouTube channel data using a Channel ID and creates channel details in JSON format.

* Get Playlist Videos: Retrieves all video IDs for a provided playlist ID.

* Get Video and Comment Details: Returns video and comment details for the given video IDs.

* Get All Channel Details: Provides channel, video, and playlist details in JSON format.

* Merge Channel Data: Combines channel details, video details, and playlist details into a single JSON format.

* Insert Data into MongoDB: Inserts channel data into MongoDB Atlas as a document.

* Get Channel Names from MongoDB: Retrieves channel names from MongoDB documents.

* Convert MongoDB Document to Dataframe: Fetches MongoDB documents and converts them into dataframes for SQL data insertion.

* Data Transformation for SQL: Performs data transformation for loading into SQL.

* Data Load to SQL: Loads data into SQL.

* Data Analysis: Conducts data analysis using SQL queries and Python integration.

* Manage MongoDB Documents: Manages MongoDB documents with various options.

* Delete SQL Records: Deletes SQL records related to the provided YouTube channel data with various options.


**Tools Expertise**

* Python (Scripting)

* Data Collection

* MongoDB

* SQL

* API Integration

* Data Management using MongoDB (Atlas) and PostgreSQL

* VS CODE


****  Result :****

This project focuses on harvesting YouTube data using the YouTube API, storing it in MongoDB, converting to SQL for analysis. Utilizes Streamlit, Python, and various methods for ETL. Expertise includes Python, MongoDB, SQL, API integration, and data management tools . This project maily reduces 80% percentage of manually data processing and data storing work effectively.
