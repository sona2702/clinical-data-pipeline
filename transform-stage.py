import pandas as pd
import snowflake.connector


df = pd.read_csv(r'C:\Users\Teves\OneDrive\Desktop\clinical-data-pipeline\raw_data\clinical_trials.csv',dtype={'Patient_id': str})

df.columns = [col.strip() for col in df.columns]  
df['BMI'] = df.apply(lambda row: row['Weight'] / ((row['Height'] / 100) ** 2), axis=1)

# Step 4: Classify Risk Level
def risk_category(bmi):
    if bmi < 18.5: return 'Underweight'
    elif bmi < 25: return 'Normal'
    elif bmi < 30: return 'Overweight'
    else: return 'Obese'

df['Risk_Level'] = df['BMI'].apply(risk_category)

# Step 5: Final columns for stage
df = df[['Patient_id', 'Weight', 'Height', 'BMI', 'Risk_Level']]

conn = snowflake.connector.connect(
    user='sona2708',
    password='Cyrusteso@2708',
    account='lgamrhl-ah61469',  
    warehouse='clinical_wh',
    database='clinical_db',
    schema='raw_schema'
 )
cursor = conn.cursor()

# Step 7: Insert into stage table
for _, row in df.iterrows():
    cursor.execute(
        """
        INSERT INTO STAGE_CLINICAL_DATA (Patient_id, Weight, Height, BMI, Risk_Level)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (row['Patient_id'], row['Weight'], row['Height'], row['BMI'], row['Risk_Level'])
    )

conn.close()
print("✅ Stage layer loaded successfully!")