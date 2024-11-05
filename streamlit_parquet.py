import streamlit as st
import pandas as pd
import jaydebeapi
import pyarrow.parquet as pq
import pyarrow as pa
import os
from io import BytesIO
import time
import zipfile
import re

# Database connection details
dsn_database = "DWHDBPRD"
dsn_hostname = "10.55.53.70"
dsn_port = "5480"
dsn_uid = "etlprod"
dsn_pwd = "password"
jdbc_driver_name = "org.netezza.Driver"
jdbc_driver_loc = os.path.join('D:\\nzjdbc.jar')

# Function to connect to Netezza
def connect_to_netezza():
    conn = jaydebeapi.connect(
        jdbc_driver_name,
        f"jdbc:netezza://{dsn_hostname}:{dsn_port}/{dsn_database}", 
        {"user": dsn_uid, "password": dsn_pwd},
        jdbc_driver_loc
    )
    return conn

# Function to execute SQL query and retrieve results
def execute_query(query):
    conn = connect_to_netezza()
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None
    finally:
        conn.close()

# Function to save DataFrame as Parquet file in buffer
def save_as_parquet_to_buffer(df):
    try:
        buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        return buffer
    except Exception as e:
        st.error(f"Error saving file: {e}")
        return None

# Function to create ZIP file from multiple Parquet files
def create_zip_file(parquet_buffers, file_names):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for i, buffer in enumerate(parquet_buffers):
            zip_file.writestr(f"{file_names[i]}.parquet", buffer.getvalue())
    return zip_buffer

# Function to extract table name from the query condition (first part of the query)
def extract_table_name(conditions):
    # Split the conditions by space and return the first part (assumed table name)
    first_part = conditions.split()[0]
    return re.sub(r'[\\/*?:"<>|]', '', first_part)  # Sanitize the table name

# Streamlit app
def main():
    st.title("Netezza to Parquet Converter - Download Individually or All")

    # Text area for entering table names and conditions, separated by semicolons
    queries_input = st.text_area("Enter table names and conditions, separated by semicolons (;)", 
                                  "BMIRPT.CIF_INDIVIDU_MASTER_TEMP_VITO LIMIT 100;", 
                                  height=200)

    if st.button("Execute Queries"):
        # Split the queries by semicolons
        queries = [q.strip() for q in queries_input.split(';') if q.strip()]

        parquet_buffers = []  # Buffer for all Parquet files
        file_names = []       # List for file names based on table names

        progress_bar = st.progress(0)  # Initialize progress bar
        progress_step = 100 / len(queries)  # Calculate progress step
        current_progress = 0

        # Execute each query and save the result as Parquet
        for i, conditions in enumerate(queries):
            # Construct the full SQL query
            query = f"SELECT * FROM {conditions}"  # Prepend standard SELECT statement
            st.write(f"Executing Query {i + 1}: {query}")
            df = execute_query(query)
            if df is not None:
                st.write(f"Query {i + 1} Results:", df.head(5))

                # Extract table name from the condition for the file name
                table_name = extract_table_name(conditions)
                file_names.append(table_name)  # Use extracted table name for the file name

                # Save query result to Parquet buffer
                parquet_buffer = save_as_parquet_to_buffer(df)

                if parquet_buffer is not None:
                    # Save buffer to list for ZIP
                    parquet_buffers.append(parquet_buffer)

                    # Show download button for each query result
                    st.download_button(
                        label=f"Download Parquet for Query {i + 1}",
                        data=parquet_buffer.getvalue(),
                        file_name=f"{table_name}.parquet",  # Use extracted table name
                        mime="application/octet-stream"
                    )

            # Update progress bar
            current_progress += progress_step
            progress_bar.progress(min(int(current_progress), 100))  # Update bar to current progress
            time.sleep(0.5)  # Simulate processing time (optional)

        progress_bar.progress(100)  # Set progress to 100% after all queries are done

        # Download all Parquet files as a ZIP
        if parquet_buffers:
            zip_buffer = create_zip_file(parquet_buffers, file_names)
            st.download_button(
                label="Download All Parquet Files as ZIP",
                data=zip_buffer.getvalue(),
                file_name="all_queries_parquet.zip",
                mime="application/zip"
            )

if __name__ == "__main__":
    main()
