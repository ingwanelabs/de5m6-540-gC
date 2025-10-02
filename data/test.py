import pandas as pd
from datetime import datetime

# Original enriched customers
enriched_customers = {
    'customer_id': [1001, 1002, 1003, 1004, 1005, 1006],
    'first_name': ['John', 'Jane', 'Mike', 'Sarah', 'Bob', 'Alice'],
    'last_name': ['Smith', 'Doe', 'Johnson', 'Wilson', 'Brown', 'Cooper'],
    'email': ['john@email.com', 'jane@email.com', 'mike@techcorp.com', 
              'sarah@retailplus.com', 'bob@email.com', 'alice@freelance.com'],
    'phone': ['01234567890', '01987654321', '01555123456', 
              '01777888999', '01111222333', '01444555666'],
    'postcode': ['SW1A 1AA', 'M1 1AF', 'B1 1BB', 'LS1 2AJ', 'NE1 3NG', 'CF10 2HH'],
    'region': ['London', 'North West', 'West Midlands', 'Yorkshire and The Humber', 'North East', 'Wales'],
    'country': ['England', 'England', 'England', 'England', 'England', 'Wales'],
    'district': ['Westminster', 'Manchester', 'Birmingham', 'Leeds', 'Newcastle', 'Cardiff'],
    'longitude': [-0.1419, -2.2426, -1.8904, -1.5491, -1.6131, -3.1791],
    'latitude': [51.5014, 53.4794, 52.4796, 53.7997, 54.9738, 51.4816],
    'geo_enriched': [1, 1, 1, 1, 1, 1],
    'company': ['', '', 'TechCorp Ltd', 'Retail Plus', '', 'Freelance Design'],
    'company_size': ['Individual', 'Individual', 'Medium (50-250 employees)', 'Large (250+ employees)', 'Individual', 'Micro (1-10 employees)'],
    'industry': ['Personal', 'Personal', 'Technology', 'Retail', 'Personal', 'Creative Services'],
    'annual_revenue': ['N/A', 'N/A', '£2M-£10M', '£10M+', 'N/A', '£0-£100K'],
    'is_business': [0, 0, 1, 1, 0, 1],
    'calculated_risk': ['Low', 'Low', 'Medium', 'High', 'Low', 'Medium'],
    'risk_score_numeric': [0, 0, 2, 5, 0, 1],
    'risk_factors': ['Standard profile', 'Standard profile', 'High-risk region', 
                     'Account suspended; High-risk region', 'Standard profile', 'Small business'],
    'status': ['active', 'active', 'active', 'suspended', 'active', 'active'],
    'processed_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 6,
    'data_source': ['ETL_Pipeline_v1'] * 6,
    'enrichment_status': ['Fully Enriched'] * 6
}

df_enriched = pd.DataFrame(enriched_customers)
df_enriched.to_csv('new_user.csv', index=False)

# Updated customers
updated_customers = {
    'customer_id': [1001, 1002, 1007, 1008],
    'first_name': ['John', 'Jane', 'David', 'Emma'],
    'last_name': ['Smith', 'Doe', 'Taylor', 'Watson'],
    'email': ['john.smith@newemail.com', 'jane@email.com', 'david@company.com', 'emma@startup.com'],
    'phone': ['01234567890', '01987654321', '01666777888', '01999888777'],
    'postcode': ['SW1A 1AA', 'M1 1AH', 'E1 0AD', 'EC1A 1BB'],
    'region': ['London', 'North West', 'London', 'London'],
    'country': ['England', 'England', 'England', 'England'],
    'district': ['Westminster', 'Manchester', 'Tower Hamlets', 'City of London'],
    'longitude': [-0.1419, -2.2426, -0.0713, -0.0982],
    'latitude': [51.5014, 53.4794, 51.5206, 51.5155],
    'geo_enriched': [1, 1, 1, 1],
    'company': ['', '', 'Tech Solutions Ltd', 'Innovation Startup'],
    'company_size': ['Individual', 'Individual', 'Small (10-50 employees)', 'Micro (1-10 employees)'],
    'industry': ['Personal', 'Personal', 'Technology', 'Technology'],
    'annual_revenue': ['N/A', 'N/A', '£100K-£2M', '£0-£100K'],
    'is_business': [0, 0, 1, 1],
    'calculated_risk': ['Low', 'Low', 'Medium', 'High'],
    'risk_score_numeric': [0, 0, 2, 3],
    'risk_factors': ['Standard profile', 'Standard profile', 'High-risk region', 
                     'New business; High-risk region'],
    'status': ['active', 'active', 'active', 'active'],
    'processed_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 4,
    'data_source': ['ETL_Pipeline_v1_Update'] * 4,
    'enrichment_status': ['Fully Enriched'] * 4
}

df_updates = pd.DataFrame(updated_customers)
df_updates.to_csv('update_user.csv', index=False)
