import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client()

# ==================================================================================================
#                                         CONFIGURATION
# ==================================================================================================
# * CONFIG SOURCE DATASET AND DATE
DATASET_ID = input('Enter your dataset ID: ')
SCHEMA_ID = input('Enter your schema ID: ')
INTRADAY = input('Enter your intraday(e.g: 20250805): ')
TABLE_ID = 'events_intraday_' + INTRADAY
START_DATE = input("Enter start date for your data(e.g: 20250721): ")
END_DATE = datetime.today().strftime('%Y%m%d')
PROJECT_FOLDER = input('Enter your project folder to save queries: ')

# * CONFIG RAW DATASET
RAW_DATASET_ID = DATASET_ID
RAW_SCHEMA_ID = input('Enter your clustered source table schema ID: ')
SOURCE_TABLE_ID = input('Enter your clustered source table : ')

#* CONFIG DEFAULT EVENTS AND PARAMETERS
DEFAULT_PARAMS_FIREBASE = ['ad_unit_code', 'ad_unit_name', 'campaign_info_source'
						   'click_timestamp', 'engaged_session_event', 'entrances', 
						   'error_message', 'firebase_conversion', 'firebase_event_origin',
						   'firebase_previous_class', 'firebase_previous_id',
						   'firebase_previous_screen', 'firebase_screen',
						   'firebase_screen_class', 'firebase_screen_id', 'gclid',
						   'gad_source', 'ga_dedupe_id', 'load_config_failed_message',
						   'medium', 'message_device_time', 'message_time', 
						   'session_engaged', 'source', 'system_app',
						   'system_app_update', 'timestamp', 'update_with_analytics',
						   'validated']
DEFAULT_EVENTS_FIREBASE = ['app_clear_data', 'app_update', 'firebase_campaign',
						   'in_app_purchased', 'inter_attempt', 'os_update',
						   'reward_attempt']


## SELECTION CREATE OR USE CLUSTERED SOURCE TABLE
select_type = int(input('Do you have a clustered source table? (0/1): ').strip())
if select_type == 0:
    # If no source table, create a new one
    #==================================================================================================
    #                                   CREATE CLUSTERED RAW TABLE
    #==================================================================================================
    source_table_clustered = f"""
    CREATE SCHEMA IF NOT EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}`;
    DROP TABLE IF EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`;
    CREATE OR REPLACE TABLE `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
    PARTITION BY event_date
    CLUSTER BY event_name, version, country, platform AS
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        event_timestamp,
        event_value_in_usd,
        app_info.version,
        geo.country,
        geo.continent,
        geo.region,
        geo.sub_continent,
        platform,
        device.mobile_brand_name,
        device.mobile_model_name,
        device.advertising_id,
        device.time_zone_offset_seconds,
        user_ltv.revenue as ltv_revenue,
        event_params
    FROM `{DATASET_ID}.{SCHEMA_ID}.events_intraday_*`
    WHERE _TABLE_SUFFIX >= '{START_DATE}'
        AND _TABLE_SUFFIX < '{END_DATE}'
    ;
    """

    with open(f'../{PROJECT_FOLDER}/source_table_clustered.sql', 'w+') as f:
        f.write(source_table_clustered)

else:
    # If source table exists, use it and create flattened tables and update queries
    # ==================================================================================================
    #                                         GET EVENTS
    # ==================================================================================================
    events = client.query(f"""
    SELECT DISTINCT event_name
    FROM `{DATASET_ID}.{SCHEMA_ID}.{TABLE_ID}`
    ORDER BY event_name;
    """).to_dataframe()

    events_list = events['event_name'].tolist()
    invalid_events = [event for event in events_list if event.split('_')[-1].isdigit()]
    events_list = [event for event in events_list if event not in invalid_events
                   and event not in DEFAULT_EVENTS_FIREBASE]
    #==================================================================================================
    #                                      CREATE FLATTENED TABLE
    #==================================================================================================
    events_dict = {}
    DATE = pd.to_datetime(INTRADAY, format='%Y%m%d').strftime('%Y-%m-%d')

    for event in events_list:
        query = f"""
        SELECT
            ep.key AS key,
            ep.value.int_value AS int_value,
            ep.value.string_value AS string_value,
            ep.value.double_value AS double_value
        FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`,
            UNNEST(event_params) ep
        WHERE event_date = '{DATE}'
            AND event_name = '{event}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ep.key ORDER BY event_timestamp) = 1
        """

        events_dict[event] = client.query(query).to_dataframe()

    events_params_dict = {}

    for event in events_list:
        if event in DEFAULT_EVENTS_FIREBASE:
            continue

        event_df = events_dict[event]
        events_params_dict[event] = {}
        for index, row in event_df.iterrows():
            if row['key'] not in DEFAULT_PARAMS_FIREBASE:
                if not pd.isna(row['string_value']):
                    try:
                        value = int(row['string_value'])
                        events_params_dict[event][row['key']] = 'cast_int_value'
                    except:
                        events_params_dict[event][row['key']] = 'string_value'
                elif not pd.isna(row['int_value']):
                    events_params_dict[event][row['key']] = 'int_value'
                elif not pd.isna(row['double_value']):
                    events_params_dict[event][row['key']] = 'double_value'

    for event in list(events_params_dict.keys()):
        if not events_params_dict[event]:
            del events_params_dict[event]

    create_raw_table = """"""

    for event in events_params_dict:
        query_string_list = []
        for key in events_params_dict[event]:
            if events_params_dict[event][key] == 'cast_int_value':
                query_string_list.append(f"CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = '{key}') AS INT64) AS {key}")
            else:
                query_string_list.append(f"(SELECT value.{events_params_dict[event][key]} FROM UNNEST(event_params) WHERE key = '{key}') AS {key}")

        if event == 'in_app_purchase':
            query_string_list.append('event_value_in_usd')

        if event == 'first_open':
            query_string_list.extend(['advertising_id', 'time_zone_offset_seconds'])

        additional_query_string = ', \n\t'.join(query_string_list)

        create_raw_table += f"""
        DROP TABLE IF EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{event}`;
        CREATE OR REPLACE TABLE `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{event}`
        PARTITION BY event_date
        CLUSTER BY version, country, platform AS
        SELECT
            user_pseudo_id,
            event_date,
            event_timestamp,
            version,
            country,
            platform,
            {additional_query_string}
        FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
        WHERE event_name = '{event}'
        ;
        """

    user_fact_raw_query = f"""
    DROP TABLE IF EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.user_fact`;
    CREATE OR REPLACE TABLE `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.user_fact`
    PARTITION BY event_date
    CLUSTER BY version, event_name, country, platform AS
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp,
        event_name,
        version,
        country,
        continent,
        region,
        sub_continent,
        platform,
        mobile_brand_name,
        mobile_model_name,
        ltv_revenue,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as ga_session_number
    FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
    ;
    """

    create_raw_table += user_fact_raw_query
    with open(f'../{PROJECT_FOLDER}/create_flatten_table.sql', 'w+') as f:
        f.write(create_raw_table)

    #==================================================================================================
    #                                         CREATE UPDATED TABLE
    #==================================================================================================

    update_raw_table = """"""

    for event in events_params_dict:
        query_string_list = []
        for key in events_params_dict[event]:
            if events_params_dict[event][key] == 'cast_int_value':
                query_string_list.append(
                    f"CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = '{key}') AS INT64) AS {key}")
            else:
                query_string_list.append(
                    f"(SELECT value.{events_params_dict[event][key]} FROM UNNEST(event_params) WHERE key = '{key}') AS {key}")

        if event == 'in_app_purchase':
            query_string_list.append('event_value_in_usd')

        additional_query_string = ', \n\t'.join(query_string_list)

        update_raw_table += f"""
    INSERT INTO `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{event}`
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp,
        version,
        country,
        platform,
        {additional_query_string}
    FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
    WHERE event_name = '{event}'
        AND event_timestamp > (SELECT MAX(event_timestamp) FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{event}`)
        AND event_timestamp < (SELECT UNIX_MICROS(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY))) 
    ;
    """

    user_fact_update_raw_query = f"""
    INSERT INTO `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.user_fact`
    SELECT
        user_pseudo_id,
        event_date AS event_date,
        event_timestamp,
        event_name,
        version,
        country,
        region,
        platform,
        mobile_brand_name,
        mobile_model_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as ga_session_number
    FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.user_fact`)
        AND event_timestamp < (SELECT UNIX_MICROS(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY))) 
    ;
    """

    update_soure_table = f"""
    INSERT INTO`{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        event_timestamp,
        event_value_in_usd,
        app_info.version,
        geo.country,
        geo.continent,
        geo.region,
        geo.sub_continent,
        platform,
        device.mobile_brand_name,
        device.mobile_model_name,
        device.advertising_id,
        device.time_zone_offset_seconds,
        user_ltv.revenue as ltv_revenue,
        event_params
    FROM `{DATASET_ID}.{SCHEMA_ID}.events_intraday_*`
    WHERE _TABLE_SUFFIX BETWEEN 
      FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) 
      AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND event_timestamp > (SELECT MAX(event_timestamp) FROM `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`)
      AND event_timestamp < (SELECT UNIX_MICROS(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)))
    ;
    """

    update_raw_table += user_fact_update_raw_query
    update_raw_table += update_soure_table

    with open(f'../{PROJECT_FOLDER}/update_raw_table.sql', 'w+') as f:
        f.write(update_raw_table)