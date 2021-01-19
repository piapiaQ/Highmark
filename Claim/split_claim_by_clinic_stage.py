# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 19:57:42 2020

@author: qiaoz
"""
import pandas as pd
import os
import re

basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'
'''
clinic = pd.read_csv('{}\Clinical Data\Clinical Data\\baseline.csv'.format(basepath),index_col = 0)


clinic.drop(columns = ['stage'],inplace = True)
clinic_kidney_egfr = pd.read_csv('{}\Clinical Data\Clinical Data\egfr_earliest.csv'.format(basepath),index_col = 0)
clinic = clinic_kidney_egfr.merge(clinic,on = 'TMA_Acct',how = 'inner')


clinic['Race'] = clinic['Race'].fillna('White')
#!assume they are still alive
clinic['hypertension'] = 1
clinic.loc[clinic.hyper_date.isna(),'hypertension'] = 0
clinic['diabete'] = 1
clinic.loc[clinic.diabe_date.isna(),'diabete'] = 0
clinic['Sex'] = clinic.Sex.astype('category')
clinic['Race'] = clinic.Race.astype('category')

clinic['Sex_cat'] = clinic.Sex.cat.codes
clinic['Race_cat'] = clinic.Race.cat.codes

clinic.dtypes
'''
os.chdir(basepath+'\data\sorted_data')
tra_dia_p = pd.read_csv('dialysis_transplant_unique_patients.csv')
os.chdir(basepath+'\data\wide_format')


def merge_claim_clinic(claim_type):
    
    claim_wide = pd.read_csv('wideforamt_sum_{}.csv'.format(claim_type))
    for i in ['3a','3b','4']:

        #   merge with final clinic by stage group(after 3 deletion) to get the final sharing patient lists 
        clinic = pd.read_csv('{}\Clinical Data\Clinical Data\\by_stage_tma_lists\\final_base_{}.csv'.format(basepath,i))
        clinic.rename(columns = {'tma_acct':'TMA_Acct','_traj_Group':'traj_Group_clinic'},inplace = True)
        clinic.columns = [re.sub('^_','Clinic_',i) for i in clinic.columns]

        clinic.drop([i for i in clinic.columns if re.match('^t/w+',i)],axis = 1,inplace = True)
        old_name = [i for i in clinic.columns if re.match('^t\d+',i)]
        new_name = [re.sub('t','egfr',i) for i in old_name]
        clinic.rename(columns = dict(zip(old_name,new_name)),inplace = True)
        
        # partial = clinic_n[clinic_n['stage'] == i]
        print('stage{} unique patient:{}'.format(i,clinic.TMA_Acct.nunique()))
        combine = claim_wide.merge(clinic,on = 'TMA_Acct',how = 'inner')
        combine = combine.merge(tra_dia_p,on = 'TMA_Acct',how = 'left',indicator = True)
        
        combine['transplant_dialysis'] = 0
        #combine.loc[combine['_merge'] == 'left_only','_merge']= 0 
        combine.loc[combine['_merge'] == 'both','transplant_dialysis']= 1 
        combine.drop(columns = ['_merge'],inplace = True)
        #combine.rename({'_merge':'transplant_dialysis'},axis = 1,inplace = True)

        print('{}:stage{} unique patient:{}'.format(claim_type,i,combine.TMA_Acct.nunique()))
        combine.to_csv(basepath+'\data\\by_clinical_stage\{}\wide_stage_{}.csv'.format(claim_type,i),index = False)

        
for i in ['total','inpatient','outpatient','drug','major_medical','Professional','vision']:
    merge_claim_clinic(i)
    
    
