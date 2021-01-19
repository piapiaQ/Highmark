# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 18:02:39 2020

@author: qiaoz
"""
import pandas as pd
import numpy as np
import os

os.chdir(r'D:\CMU\HIghmark Project\data\raw_claim_data')
# =============================================================================
# reorder data and save them into csvs that ordered by patient number(TMA Acct)
# =============================================================================
data1=pd.read_csv('df_claims_for_CMU_deident_part1.csv')
data2=pd.read_csv('df_claims_for_CMU_deident_part2.csv')
data3=pd.read_csv('df_claims_for_CMU_deident_part3.csv')
data4=pd.read_csv('df_claims_for_CMU_deident_part4.csv')
data5=pd.read_csv('df_claims_for_CMU_deident_part5.csv')
data6=pd.read_csv('df_claims_for_CMU_deident_part6.csv')
data7=pd.read_csv('df_claims_for_CMU_deident_part7.csv')
data8=pd.read_csv('df_claims_for_CMU_deident_part8.csv')
data9=pd.read_csv('df_claims_for_CMU_deident_part9.csv')
data10=pd.read_csv('df_claims_for_CMU_deident_part10.csv')
data11=pd.read_csv('df_claims_for_CMU_deident_part11.csv')
data12=pd.read_csv('df_claims_for_CMU_deident_part12.csv')
data13=pd.read_csv('df_claims_for_CMU_deident_part13.csv')
data14=pd.read_csv('df_claims_for_CMU_deident_part14.csv')
data15=pd.read_csv('df_claims_for_CMU_deident_part15.csv')
data16=pd.read_csv('df_claims_for_CMU_deident_part16.csv')
data = pd.concat([data1,data2,data3,data4,data5,data6,data7,data8,data9,data10,data11,data12,data13,data14,data15,data16])

# =============================================================================
# split into multiple files by TMA Acct#

# =============================================================================
def split_into_multiple_file(data,splits,fileprefix,savepath):

    data = data.sort_values(by = 'TMA Acct#')
    data.reset_index(drop = True, inplace = True)                        
    data['TMA Acct#'].nunique()
    for id, df_i in  enumerate(np.array_split(data, splits)):
        df_i.to_csv('{p}{prefix}_{id}.csv'.format(p = savepath, prefix = fileprefix,id=id))
        
# =============================================================================
#create my anomaly mapping
# =============================================================================
split_into_multiple_file(data,16,'data_sorted','D://CMU//HIghmark Project//data//sorted_data//')

#find claims number that matches to multiple tma

mapping = data[['TMA Acct#','claim_number_deidentified']].drop_duplicates(keep = 'first')
grp = mapping.groupby(['claim_number_deidentified'])
size = grp.size()
claim_anomoly = size[size != 1].index
mapping = mapping[mapping.claim_number_deidentified.isin(claim_anomoly)]
mapping.index
dict_anomaly = {}
for i in range(len(mapping)):
    k = mapping.iloc[i,1]
    v = mapping.iloc[i,0]
    if k not in dict_anomaly.keys():
        dict_anomaly[k] = [v]  
    else:
        dict_anomaly[k] = dict_anomaly[k] + [v]
tma_anomaly = list(dict_anomaly.values())
tma_anomaly
res = list(set(map(lambda i: tuple(sorted(i)), tma_anomaly))) 
import csv
os.chdir(r'D:\CMU\HIghmark Project\data\anomaly_claim_data')

with open('tma_anomaly.csv','w',newline = '') as f:
    csv_out = csv.writer(f,delimiter=',')
    for i in res:
        csv_out.writerow(i)