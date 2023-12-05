from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

# Create a string for the ages_served column from the Oklahoma dataset
def ages_served_ok_string_agg(row):
    values = [str(row['ages accepted 1']), str(row['aa2']), str(row['aa3']), str(row['aa4'])]
    non_none_values = [val for val in values if val and val != 'nan']
    return ', '.join(non_none_values)

# Create a string for the ages_served column from the Texas dataset
def ages_served_tx_string_agg(row):
    return ', '.join([col for col in ['infant', 'toddler', 'preschool', 'school'] if row[col] == 'Y'])

# Create a string for the schedule column from the Oklahoma dataset
def schedule_string_agg(row):
    values = [(col, str(row[col])) for col in ['mon', 'tues', 'wed', 'thurs', 'friday', 'saturday', 'sunday']]
    return ', '.join([f"{col} {val}" for col, val in values])

# Change dataframe schema to match that of another dataframe
def standardize_schema(df_1, df_2):
    missing_columns = list(set(df_1.columns) - set(df_2.columns))
    for col in missing_columns:
        df_2[col] = None
    df_2 = df_2[df_1.columns]
    df_2 = df_2.astype(df_1.dtypes)
    return df_2

# Get created_at timestamp
current_time = datetime.now()

# Design final merged dataframe schema
merged_df_schema = {
    'accepts_financial_aid': 'object',
    'ages_served': 'object',
    'capacity': 'Int64',
    'certificate_expiration_date': 'datetime64[ns]',
    'city': 'object',
    'address1': 'object',
    'address2': 'object',
    'company': 'object',
    'phone': 'object',
    'phone2': 'object',
    'county': 'object',
    'curriculum_type': 'object',
    'email': 'object',
    'first_name': 'object',
    'language': 'object',
    'last_name': 'object',
    'license_status': 'object',
    'license_issued': 'datetime64[ns]',
    'license_number': 'object',
    'license_renewed': 'datetime64[ns]',
    'license_type': 'object',
    'licensee_name': 'object',
    'max_age': 'Int64',
    'min_age': 'Int64',
    'operator': 'object',
    'provider_id': 'object',
    'schedule': 'object',
    'state': 'object',
    'title': 'object',
    'website_address': 'object',
    'zip': 'object',
    'facility_type': 'object',
    'created_at': 'datetime64[ns]',
    'source': 'object'
}
merged_df = pd.DataFrame(columns=merged_df_schema.keys()).astype(merged_df_schema)

# Extract and tranform data from Nevada CSV
nevada_filename = '07-07-2023 Nevada Dept of Public _ Behavioral Health.csv'
nevada_columns = ['Expiration Date', 'Address', 'Name', 'Phone#', 'County', 'Primary Contact Name', 'Status', \
    'First Issue Date', 'Credential Number', 'Credential Type', 'State']
nevada_df = pd.read_csv(nevada_filename, usecols=nevada_columns, parse_dates=['Expiration Date', 'First Issue Date'])
nevada_df.columns = map(str.lower, nevada_df.columns)
nevada_column_mapping = {
    'expiration date': 'certificate_expiration_date',
    'address': 'address1',
    'name': 'company',
    'phone#': 'phone',
    'status': 'license_status',
    'first issue date': 'license_issued',
    'credential number': 'license_number',
    'credential type': 'license_type'
}
nevada_df = nevada_df.rename(columns=nevada_column_mapping)
nevada_df['first_name'] = nevada_df['primary contact name'].str.split().str[0]
nevada_df['last_name'] = nevada_df['primary contact name'].str.split().str[1]
nevada_df['licensee_name'] = nevada_df['company']
nevada_df['operator'] = nevada_df['company']
nevada_df['zip'] = nevada_df['address1'].str.split().str[-1]
nevada_df['facility_type'] = nevada_df['license_type']
nevada_df['created_at'] = current_time
nevada_df['source'] = nevada_filename
nevada_df = nevada_df.drop(columns=['primary contact name'])

# Extract and tranform data from Oklahoma CSV
oklahoma_filename = '07-07-2023 Oklahoma Human Services.csv'
oklahoma_columns = ['Accepts Subsidy', 'Ages Accepted 1', 'AA2', 'AA3', 'AA4', 'Total Cap', 'City', 'Address1', \
    'Address2', 'Company', 'Phone', 'Email', 'Primary Caregiver', 'Type License', 'Mon', 'Tues', 'Wed', 'Thurs', \
    'Friday', 'Saturday', 'Sunday', 'State', 'Zip']
oklahoma_df = pd.read_csv(oklahoma_filename, usecols=oklahoma_columns)
oklahoma_df.columns = map(str.lower, oklahoma_df.columns)
oklahoma_column_mapping = {
    'accepts subsidy': 'accepts_financial_aid',
    'total cap': 'capacity'
}
oklahoma_df = oklahoma_df.rename(columns=oklahoma_column_mapping)
oklahoma_df['ages_served'] = oklahoma_df.apply(ages_served_ok_string_agg, axis=1)
oklahoma_df['first_name'] = oklahoma_df['primary caregiver'].str.split().str[0]
oklahoma_df['last_name'] = oklahoma_df['primary caregiver'].str.split().str[1]
oklahoma_df['license_number'] = oklahoma_df['type license'].str.split().str[-1]
oklahoma_df['license_type'] = oklahoma_df['type license'].str.split('-').str[0].str.strip()
oklahoma_df['licensee_name'] = oklahoma_df['company']
oklahoma_df['operator'] = oklahoma_df['company']
oklahoma_df['schedule'] = oklahoma_df.apply(schedule_string_agg, axis=1)
oklahoma_df['facility_type'] = oklahoma_df['license_type']
oklahoma_df['created_at'] = current_time
oklahoma_df['source'] = oklahoma_filename
oklahoma_df = oklahoma_df.drop(columns=['type license', 'mon', 'tues', 'wed', 'thurs', 'friday', 'saturday', \
    'sunday', 'primary caregiver', 'ages accepted 1', 'aa2', 'aa3', 'aa4'])

# Extract and tranform data from Texas CSV
texas_filename = '07-07-2023 Texas DHHS.csv'
texas_columns = ['Infant', 'Toddler', 'Preschool', 'School', 'Capacity', 'City', 'Address', 'Operation/Caregiver Name', \
    'Phone', 'County', 'Email Address', 'Status', 'Issue Date', 'Type', 'Facility ID', 'State', 'Zip']
texas_df = pd.read_csv(texas_filename, usecols=texas_columns, parse_dates=['Issue Date'])
texas_df.columns = map(str.lower, texas_df.columns)
texas_column_mapping = {
    'address': 'address1',
    'operation/caregiver name': 'company',
    'email address': 'email',
    'status': 'license_status',
    'issue date': 'license_issued',
    'type': 'license_type',
    'facility id': 'provider_id'
}
texas_df = texas_df.rename(columns=texas_column_mapping)
texas_df['ages_served'] = texas_df.apply(ages_served_tx_string_agg, axis=1)
texas_df['licensee_name'] = texas_df['company']
texas_df['operator'] = texas_df['company']
texas_df['facility_type'] = texas_df['license_type']
texas_df['created_at'] = current_time
texas_df['source'] = texas_filename
texas_df = texas_df.drop(columns=['infant', 'toddler', 'preschool', 'school'])

# Change dataframe schemas to match the final schema
nevada_df = standardize_schema(merged_df, nevada_df)
oklahoma_df = standardize_schema(merged_df, oklahoma_df)
texas_df = standardize_schema(merged_df, texas_df)

# Merge the dataframes and then load the merged dataframe into a Postgres table
merged_df = pd.concat([nevada_df, oklahoma_df, texas_df], ignore_index=True)
engine = create_engine('postgresql://@localhost:5432/brightwheel')
merged_df.to_sql('leads', engine, index=False)
