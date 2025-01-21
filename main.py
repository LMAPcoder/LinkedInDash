import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import re
from datetime import datetime, timedelta
import geocoder


st.set_page_config(
    page_title="Visor", # The page title, shown in the browser tab.
    page_icon=":material/stacked_line_chart:",
    layout="wide", # How the page content should be laid out.
    initial_sidebar_state="auto", # How the sidebar should start out.
    menu_items={ # Configure the menu that appears on the top-right side of this app.
        "Get help": "https://github.com/LMAPcoder" # The URL this menu item should point to.
    }
)

@st.cache_data
def fetch_table(location=None, keyword=None, date=None):
    # Start with an empty filter expression
    filter_expression = None

    # Add filter for location if it's provided
    if location is not None:
        filter_expression = Attr('search_location').eq(location)

    # Add filter for status if it's provided
    if keyword is not None:
        if filter_expression:
            filter_expression = filter_expression & Attr('keyword').eq(keyword)
        else:
            filter_expression = Attr('keyword').eq(keyword)

    # Add filter for other_attribute if it's provided
    if date is not None:
        if filter_expression:
            filter_expression = filter_expression & Attr('time_stamp').gt(date)
        else:
            filter_expression = Attr('time_stamp').gt(date)

    # Perform scan, using the filter if any filters are specified
    if filter_expression:
        response = table.scan(
            FilterExpression=filter_expression
        )
    else:
        # No filters, retrieve entire table
        response = table.scan()

    return response.get('Items', [])

def extract_hours(time_string):
    # Match the number and unit (minute, hour, or day)
    match = re.match(r"(\d+)\s*(minute|hour|day)s?\s*ago", time_string)
    if match:
        number = int(match.group(1))  # Extract the number
        unit = match.group(2)  # Extract the unit

        # Convert based on the unit
        if unit == "minute":
            return round(number / 60, 1)
        elif unit == "hour":
            return number
        elif unit == "day":
            return number * 24
    return 0  # In case of no match

def extract_integer(s):
    # Find the first occurrence of an integer in the string
    match = re.search(r'\d+', s)
    if match:
        return int(match.group())
    return None  # return None if no number is found

def geocode(location):
    try:
        location_obj = geocoder.arcgis(location)
        return location_obj.y, location_obj.x
    except:
        return None, None

def convert_timestamp(unix_timestamp):
    dt_object = datetime.utcfromtimestamp(int(unix_timestamp))
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

if 'run' not in st.session_state:
    st.session_state.run = False

def on_button_click():
    st.session_state.run = True

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('job_postings')



st.title("LinkedIn database")

col1, col2, col3 = st.columns(3, gap="small")

with col1:
    LOCATION = st.selectbox(
        label="Location",
        options=[None, "Argentina", "United States", "Spain"],
        index=1,
    )

with col2:
    KEYWORD = st.selectbox(
        label="Keyword",
        options=[None, "data", "python", "immigration", "nonprofit"],
        index=1,
    )

with col3:
    DAYS = st.slider(
        label="Retrieve time: last x days",
        min_value=1,
        max_value=30,
        value=3
    )

now = datetime.now()

date = now - timedelta(days=DAYS)

date = int(date.timestamp())

if st.button('Search', on_click=on_button_click):
    st.session_state.run = True

if st.session_state.run:

    response = fetch_table(location=LOCATION, keyword=KEYWORD)

    if response == []:
        st.subheader("No records")
    else:
        column_order = [
            "search_location",
            "keyword",
            "time_stamp",
            "job_id",
            "job_link",
            "job_title",
            "job_function",
            "seniority_level",
            "company_name",
            "industries",
            "location",
            "time_posted",
            "num_applicants",
            "recruiter",
            "job_description",
            "description_language",
            "role_name",
            "work_arrangement",
            "employment_type",
            "duration"
        ]

        all_columns = list(response[0].keys())

        remaining_columns = [col for col in all_columns if col not in column_order]

        column_order = column_order + remaining_columns

        df = pd.DataFrame(response, columns=column_order)

        df['time_stamp'] = df['time_stamp'].apply(convert_timestamp)
        df.insert(12, 'hours_posted', df['time_posted'].apply(extract_hours))
        df.insert(14, 'n_applicants', df['num_applicants'].apply(extract_integer))

        st.dataframe(df)


    st.header("map")

    df['latitude'], df['longitude'] = zip(*df['location'].apply(geocode))

    df.dropna(subset=['latitude', 'longitude'])

    st.map(df)

    st.session_state.run = False

