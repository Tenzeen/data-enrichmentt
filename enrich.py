#import necessary packages
import pandas as pd
import numpy as np

#load both datasets into python
sparcs = pd.read_csv('data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2015 (1).csv', low_memory=False)

adi = pd.read_csv('data/NY_2015_ADI_9 Digit Zip Code_v3.1.csv', low_memory=False)

#inspecting columns to determine key columns. 
sparcs.columns #SPARCS dataset contains 3-digit zipcodes

adi.columns #ADI dataset contains 9-digit zipcode

##### sparcs dataset cleaning #####
sparcs.columns = sparcs.columns.str.replace('[^A-Za-z0-9]+', '_') #regex formatting

sparcs.columns = sparcs.columns.str.lower() #lowercase column headers

sparcs.dtypes #make sure assigned datatypes are appropriate

sparcs.replace(to_replace='', value=np.nan, inplace=True) #replace empty values with nan

sparcs.replace(to_replace=' ', value=np.nan, inplace=True) #replace empty values with nan

sparcs.dropna(inplace=True) #drop missing data

sparcs.drop_duplicates(inplace=True) #drop duplicates

###### adi dataset cleaning #####
adi.columns = adi.columns.str.replace('[^A-Za-z0-9]+', '_') #regex formatting

adi.columns = adi.columns.str.lower() #lowercase column headers

adi.dtypes #make sure assigned datatypes are appropriate

adi.replace(to_replace='', value=np.nan, inplace=True) #replace empty values with nan

adi.replace(to_replace=' ', value=np.nan, inplace=True) #replace empty values with nan

adi.dropna(inplace=True)  #drop missing data

adi.drop_duplicates(inplace=True) #drop duplicates 

adi['zip3'] = adi['zipid'].str.slice(1, 4) #only keep first 3 digits to match sparcs dataset zipcode column

#check to see if zip format is now the same
sparcs['zip_code_3_digits']

adi['zip3'] #upon checking, both datasets now contain 3 digit zipcodes

##### enrichment #####
#create smaller dataset to reduce file sizes in order to have a successful merge
sample_adi = adi.sample(100)

sample_sparcs = sparcs.sample(100)

#select columns we need to reduce file size
sample_adi_small = sample_adi[['zip3', 'adi_natrank', 'adi_staternk']]

sample_sparcs_small = sample_sparcs[[            
    'hospital_county',
    'facility_name',
    'age_group', 
    'race',
    'ethnicity',
    'zip_code_3_digits', 
    'length_of_stay',
    'ccs_diagnosis_code',
    'ccs_procedure_code',
    'payment_typology_1', 
    'total_charges', 
    'total_costs']]

#merge datasets with the 3 digit zip code as the key
merged_sparcs_small = sample_sparcs_small.merge(sample_adi_small, how='left', left_on='zip_code_3_digits', right_on='zip3')

#inspect the merged dataset to see if merged correctly
merged_sparcs_small

#save the merged dataset as a new csv file
merged_sparcs_small.to_csv('data/merged_sparcs_small.csv')