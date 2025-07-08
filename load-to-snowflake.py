 
import pandas as pd
import snowflake.connector


df = pd.read_csv(r'C:\Users\Teves\OneDrive\Desktop\clinical-data-pipeline\raw_data\clinical_trials.csv',dtype={'Patient_id': str})

df.columns = [col.strip() for col in df.columns] # Clean any trailing spaces


conn = snowflake.connector.connect(
    user='sona2708',
    password='Cyrusteso@2708',
    account='lgamrhl-ah61469',  # like abcd-xy123
    warehouse='clinical_wh',
    database='clinical_db',
    schema='raw_schema'
 )

existing_ids_df = pd.read_sql("SELECT Patient_id FROM raw_clinical_data", conn)
existing_ids = existing_ids_df.iloc[:, 0].tolist()


#  Filter out duplicates
df_new = df[~df['Patient_id'].isin(existing_ids)]

cursor = conn.cursor()
for _, row in df_new.iterrows():
    cursor.execute(
        """
        INSERT INTO raw_clinical_data (Patient_id, Weight, Height)
        VALUES (%s, %s, %s)
        """,
        (row['Patient_id'], row['Weight'], row['Height'])
    )
print(f"✅ Inserted {len(df_new)} new records into raw_clinical_data.")
