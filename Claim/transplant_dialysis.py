"""
Created on Fri Oct  9 21:49:11 2020

@author: Joyqiao
"""


import pandas as pd
import numpy as np
import re
import os

basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'

def target_variable(data,code_type):
    '''
    input data dataframe
          code_tyoe: string
    output target_variables:list
    '''
    
    target_variables = []
    if code_type == 'proc':
        target_variables += [i for i in data.columns if re.search('proc',i)]
    elif code_type == 'diag':
        target_variables += [i for i in data.columns if re.search('(diag)|(dx)',i)]
    elif code_type == 'drg':
        target_variables =['current_drg']
    else:
        print('please specify what kind of code you want to search')
    return target_variables

        

def delete_records_fall_into_condition(target_code,data,code_type,transplant_dialysis):
    '''
    parameter target cpt code: string
    data df
    transplant_dialysis string
    return df
    '''  
    '''
    # transplant cpt code
    if transplant_dialysis == 'transplant':
        target_variables = [i for i in data.columns if re.search('proc',i)]
    elif transplant_dialysis == 'dialysis':
        target_variables = [i for i in data.columns if re.search('(diag)|(dx)',i)]
    else:
        print('please specify what kind of claim you want to search')
    '''    
    target_variables = target_variable(data,code_type)
        
    row_dropper = (data[target_variables] == target_code).any(axis = 'columns')
    if  row_dropper.sum() == 0:
        pass
    else:
        print(row_dropper.sum())
        patient = data.loc[row_dropper,:][['TMA_Acct','icrd_dt_deidentified']].sort_values(by='icrd_dt_deidentified').drop_duplicates(subset = 'TMA_Acct',keep = 'first')
        
        #delete all the records after this date for this patient
        
        data =data.merge(patient,on = 'TMA_Acct',how = 'left',suffixes = ['_original','_criteria'])
        data = data.loc[~((data.TMA_Acct.isin(patient.TMA_Acct))&(data['icrd_dt_deidentified_original']>=data['icrd_dt_deidentified_criteria'])),:]
        data.drop(columns = 'icrd_dt_deidentified_criteria',inplace = True)
        data.rename(columns = {'icrd_dt_deidentified_original':'icrd_dt_deidentified'},inplace = True)
        
        patient = pd.concat([patient,pd.DataFrame([target_code]*patient.shape[0])],axis = 1,ignore_index = True)
        patient.to_csv('{}_patient.csv'.format(transplant_dialysis),mode = 'a')

    return data

def clean_transplant_dialysis(data):
    '''
    Parameters
    ----------
    data : df sorted raw data

    Returns df

    '''


    data = data.replace(r'^\s*$', np.nan, regex=True)
    data = data.replace(r'\s*$','',regex=True)
        
    data['current_drg'] = data['current_drg'].astype('str')

#   for transplant
    DRG_transplant = ['652','8']
    proc_cpt_transplant = ['0TY00Z0','0TY00Z1','0TY00Z2','0TY10Z0','0TY10Z1','0TY10Z2','91','92','93','5561','5569','50360','50365','50380']

    for i in DRG_transplant:
        data = delete_records_fall_into_condition(i,data,'drg','transplant')    
    for i in proc_cpt_transplant:
        data = delete_records_fall_into_condition(i,data,'proc','transplant') 
        
        
 #  for dialysis   

    diag_icd_cm_dialysis = ['V4511','Z992']
    proc_cpt_dialysis = pd.read_excel(basepath+'\\Highmark_CKD___Dialysis_Codes_Used.xlsx',sheet_name=2,header = None)
    #proc_cpt_dialysis = ['90935','90937','90989','90993','90999','90951','90952','90953','90954','90955','90956','90957','90958','90959','90960','90961','90962','90963','90964','90965','90966','90999']
    proc_cpt_dialysis = list(set(proc_cpt_dialysis.values.flatten().tolist()))
    for i in diag_icd_cm_dialysis:
        data = delete_records_fall_into_condition(i,data,'diag','dialysis')    
    for i in proc_cpt_dialysis:
        data = delete_records_fall_into_condition(i,data,'proc','dialysis')    
        
    return data




