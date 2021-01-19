# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 11:18:47 2020

@author: Joyqiao
"""
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import os
import re


'''
stage1 = pd.read_csv('stage1_claims.csv',index_col = 0)
stage2 = pd.read_csv('stage2_claims.csv',index_col = 0)
stage3 = pd.read_csv('stage3_claims.csv',index_col = 0)
stage4 = pd.read_csv('stage4_claims.csv',index_col = 0)
stage5 = pd.read_csv('stage5_claims.csv',index_col = 0)

for i in [stage1,stage2,stage3,stage4,stage5]:
    print(i['TMA Acct#'].nunique())
'''
# df_cleaned = pd.concat([cleaned0,cleaned1,cleaned2,cleaned3,cleaned4,cleaned5,cleaned6,cleaned7,cleaned8,cleaned9,cleaned10,cleaned11,cleaned12,cleaned13,cleaned14,cleaned15,cleaned16])
# df_cleaned = df_cleaned.rename(columns = {'TMA_Acct':'TMA Acct#'})
# df_cleaned = df_cleaned.drop(columns = ['total','cost_abs','count'])     
basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'

os.chdir(basepath+'\data\cleaned_data')
cleaned0=pd.read_csv('cleaned_0.csv',index_col = 0) 
cleaned1=pd.read_csv('cleaned_1.csv',index_col = 0) 
cleaned2=pd.read_csv('cleaned_2.csv',index_col = 0) 
cleaned3=pd.read_csv('cleaned_3.csv',index_col = 0) 
cleaned4=pd.read_csv('cleaned_4.csv',index_col = 0) 
cleaned5=pd.read_csv('cleaned_5.csv',index_col = 0) 
cleaned6=pd.read_csv('cleaned_6.csv',index_col = 0) 
cleaned7=pd.read_csv('cleaned_7.csv',index_col = 0) 
cleaned8=pd.read_csv('cleaned_8.csv',index_col = 0) 
cleaned9=pd.read_csv('cleaned_9.csv',index_col = 0) 
cleaned10=pd.read_csv('cleaned_10.csv',index_col = 0) 
cleaned11=pd.read_csv('cleaned_11.csv',index_col = 0) 
cleaned12=pd.read_csv('cleaned_12.csv',index_col = 0) 
cleaned13=pd.read_csv('cleaned_13.csv',index_col = 0) 
cleaned14=pd.read_csv('cleaned_14.csv',index_col = 0) 
cleaned15=pd.read_csv('cleaned_15.csv',index_col = 0) 
cleaned16=pd.read_csv('cleaned_16.csv',index_col = 0) 
df_cleaned= pd.concat([cleaned0,cleaned1,cleaned2,cleaned3,cleaned4,cleaned5,cleaned6,cleaned7,cleaned8,cleaned9,cleaned10,cleaned11,cleaned12,cleaned13,cleaned14,cleaned15,cleaned16])


#only 2010-1-1 to 2018-12-31
df_cleaned = df_cleaned[df_cleaned['icrd_dt_deidentified']<=3284]
print(df_cleaned['TMA_Acct'].nunique())

#by claim type                                  
mindate_type=pd.DataFrame(df_cleaned.groupby("TMA_Acct")['icrd_dt_deidentified'].agg(min))
mindate_type.to_csv('mindate.csv',index = False,mode = 'a')
df_type=pd.merge(df_cleaned, mindate_type, how='left', left_on='TMA_Acct', right_index=True)
df_type['quarter_year_time']=((df_type['icrd_dt_deidentified_x']-df_type['icrd_dt_deidentified_y'])/91.25+1).apply(np.floor)  
df_time_long_sum_type=df_type.groupby(['TMA_Acct', 'quarter_year_time','claim_type'])['eacl_prv_alcrg_at'].sum().reset_index()
df_time_long_sum_type['TMA_Acct'].nunique()
#only keep the patients with more than three grouped records
connection = sqlite3.connect(':memory:')
df_time_long_sum_type.to_sql('time_long_sum',connection)
query = '''
        select * from time_long_sum
        where TMA_Acct in
        (
        select TMA_Acct from time_long_sum 
        group by TMA_Acct
        having count(TMA_Acct)>=3
        )
        '''
df_time_long_sum_type = pd.read_sql_query(query,connection) 
df_time_long_sum_type.set_index(['index'],inplace = True)
#df_time_long_mean_type=df_type.groupby(['TMA Acct#', 'half_year_time','claim_type'])['eacl_prv_alcrg_at'].mean()
df_time_long_sum_type = df_time_long_sum_type[df_time_long_sum_type['eacl_prv_alcrg_at'] != 0]

# long to wide
def long_to_wide(data,output):
    time_range = int(max(data['quarter_year_time']))
    wide_format = data.pivot_table(index = 'TMA_Acct',columns = 'quarter_year_time',values = 'eacl_prv_alcrg_at',aggfunc=np.sum)                        
    wide_format.columns = ['Cost'+str(i+1) for i in range(time_range)]
    col_to_log = [i for i in wide_format.columns if re.search(r"^Cost", i)]

    for j in range(time_range):
        wide_format['Time_'+str(j+1)] = 0.25*(j+1)
    wide_format[col_to_log] = np.log(wide_format[col_to_log])
    wide_format.to_csv(output)




sum_Inpatient = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'I']
sum_outpatient = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'O']
sum_Drug = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'DR']
sum_Third_party_his = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'HS']
sum_Hearing = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'H']
sum_Dental = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'DT']
sum_BILLING = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'B']
sum_major_medical = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'MM']
sum_vision = df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'V']
sum_Professional= df_time_long_sum_type[df_time_long_sum_type['claim_type'] == 'P']

os.chdir(basepath+'\data\wide_format')

long_to_wide(df_time_long_sum_type, 'wideforamt_sum_total.csv')
long_to_wide(sum_Inpatient, 'wideforamt_sum_inpatient.csv')
long_to_wide(sum_outpatient,'wideforamt_sum_outpatient.csv')
long_to_wide(sum_Drug,'wideforamt_sum_drug.csv')
long_to_wide(sum_Professional,'wideforamt_sum_professional.csv')
long_to_wide(sum_vision,'wideforamt_sum_vision.csv')
long_to_wide(sum_major_medical,'wideforamt_sum_major_medical.csv')


df_time_long_sum_type.TMA_Acct.nunique()
df_time_long_sum_type.claim_type.unique()
