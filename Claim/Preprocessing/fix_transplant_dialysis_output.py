# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 19:26:13 2020

@author: Joyqiao
"""
import os
import pandas as pd


basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'
os.chdir(basepath+'\data\sorted_data')

def fill(data_name,output_name):
    data = pd.read_csv(data_name,index_col = 0)
    data['2'] = data['2'].fillna(method = 'ffill')
    data = data[data['1'].notna()]
    data = data.drop_duplicates(keep = 'first')
    print(data['0'].nunique())
    data.rename(columns = {'0':'TMA_Acct','1':'icrd_dt_deidentified','2':'code'},inplace = True)
    data.to_csv(output_name)

fill('dialysis_patient.csv','dialysis_patient_clean.csv')
fill('transplant_patient.csv','transplant_patient_clean.csv')

#label transplant dialysis patients
dia = pd.read_csv('dialysis_patient_clean.csv')
trans = pd.read_csv('transplant_patient_clean.csv')
dia_trans_p = pd.DataFrame(set(pd.concat([dia.TMA_Acct , trans.TMA_Acct])),columns = ['TMA_Acct'])
dia_trans_p.to_csv('dialysis_transplant_unique_patients.csv',index = False)
