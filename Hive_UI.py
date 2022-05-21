import streamlit as st
import re
import pandas as pd
import time
from pyhive import presto,exc
from datetime import date

#setting page layout to wide
st.set_page_config(layout="wide")


def DBConnection():
    Hive_Connection = "Connection_String" #replace with presto connection with username and port
    return Hive_Connection


@st.cache(allow_output_mutation=True,suppress_st_warning=True)
def get_database_connection():
    return DBConnection()

@st.cache(hash_funcs={DBConnection: id})
def get_data(query,connection):
    data = pd.read_sql_query(query,connection)
    return data


@st.cache
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')


today = date.today()
today_date = today.strftime('%d/%m/%Y')
file_name=f"Query_data - {today_date}" #default filename
file_data=st.file_uploader("Load a txt file",type=["txt","sql"])
query_text=st.text_area("Query",height=250,key="query")
sub=st.button("Search")
data=None
if query_text is not None:
    data=query_text
if file_data is not None:
        data=str(file_data.read(),"utf-8")
        file_name=file_data.name.split('.')[0] #filename based on uploaded file


#scrape date and table details from query
if data:
    dates=re.findall('\d{4}-\d{2}-\d{2}',data)
    tables=re.findall(r'(\w{5,}[.]\w+)', data)
    if dates:
        st.code(f"Date : {set(dates)}")
    if tables:
        table=pd.DataFrame(set(tables),columns=['Table'])
        st.table(table)


if sub:
    st.write("Connected to DB",get_database_connection())
    st.info("Query Executed")
    Start=time.time()
    data_fetched =  None
    attempts = 0
    while attempts < 3:
        try:
            with st.spinner('Fetching Data..Please wait'):
                data_fetched= get_data(data,get_database_connection())
            break
        except exc.Error or ConnectionError:
            attempts += 1
            st.error(f"Error with Database - Retry_{attempts}")
    End=time.time()
    time_took=None
    if round((End-Start))<=60:
        time_took= f"{round((End-Start))} Seconds" 
    else:
        time_took=f"{round((End-Start)/60,2)} Minutes"
    if not data_fetched.empty:
        st.code(f"Fetched {data_fetched.shape[0] } rows and {data_fetched.shape[1]} Columns in {time_took}")
        st.write(data_fetched.head())
        csv = convert_df(data_fetched)
        today = date.today()
        st.download_button(
            "Download",
            csv,
            f"{file_name}.csv",
            "text/csv",
            key='download-csv'
            )
    else:
        st.write("Fetched 0 rows")
        st.download_button(
            "Download",
            data_fetched.to_csv(index=False),
            f"{file_name}.csv",
            "text/csv",
            key='download-csv'
            )

