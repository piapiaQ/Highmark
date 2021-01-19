# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 22:24:05 2020

@author: Joyqiao
"""
import pandas as pd
import numpy as np
import re
basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'

def evaluation(data):
                        
    data = data[['tma_acct']+[i for i in data.columns if re.search('^_traj',i)]]
    estimate_pct = data[[i for i in data.columns if re.search('traj_ProbG\d$',i)]].mean()
    actual_pct= {}
    prob_given_group = {}
    min_prob = {}
    max_prob = {}
    std_prob = {}
    occ = {}
    data['_traj_Group'].unique()
    for i in data['_traj_Group'].unique():
        c = '_traj_ProbG'+str(i)
        selected_cells = data.loc[data['_traj_Group'] == i,c]
        actual_pct[c] = len(selected_cells)/data.shape[0]
        max_prob[c] = selected_cells.max()
        min_prob[c] = selected_cells.min()
        prob_given_group[c] = selected_cells.mean()
        std_prob[c] = selected_cells.std()
        occ[c] = ((prob_given_group[c])/(1-prob_given_group[c]))/((actual_pct[c])/(1-actual_pct[c]))

    actual_pct_new = pd.Series(actual_pct)
    diff_pct = pd.Series(estimate_pct) - actual_pct_new
    combine = pd.DataFrame()
    combine = pd.concat([combine,estimate_pct,actual_pct_new,diff_pct,pd.Series(max_prob),pd.Series(min_prob),pd.Series(prob_given_group),pd.Series(std_prob),pd.Series(occ)],axis = 1)
    print(estimate_pct,actual_pct_new,diff_pct,max_prob,min_prob,std_prob,occ)
    combine.rename(columns = dict(zip(range(8),['estimate_pct','actual_pct_new','diff_pct','max_prob','min_prob','prob_given_group','std_prob','occ'])),inplace = True)
    return combine
    
def save_performance(file_name):
    data = pd.read_excel(basepath+file_name)
    r = evaluation(data)
    r.to_csv(basepath+file_name+'_performance.csv',index = False)
    
save_performance('\\data\\stata_output\\3a_3331_total_2021.xlsx')
save_performance('\\data\\stata_output\\3a_33331_total_2021.xlsx')

data = pd.read_excel(basepath+'\\data\\stata_output\\3a_3331_total_2021.xlsx')
r = evaluation(data)
r.to_csv(basepath+'\\data\\stata_output\\3a_outpatient_3223_performance.csv',index = False)

data = pd.read_excel(basepath+'\\data\\stata_output\\3a_outpatient_3223.xlsx')
r2 = evaluation(data)
r2.to_csv(basepath+'\\data\\stata_output\\3a_outpatient_3223_performance.csv',index = False)
data = pd.read_excel(basepath+'\\data\\stata_output\\3a_inpatient_32.xlsx')
r3 = evaluation(data)
r3.to_csv(basepath+'\\data\\stata_output\\3a_inpatient_32_performance.csv',index = False)
data = pd.read_excel(basepath+'\\data\\stata_output\\3a_drug_2121.xlsx')
r5 = evaluation(data)
r4.to_csv(basepath+'\\data\\stata_output\\3a_drug_31233_performance.csv',index = False)

data = pd.read_excel(basepath+'\\data\\stata_output\\3a_professional_3313.xlsx')
r6 = evaluation(data)
r6.to_csv(basepath+'\\data\\stata_output\\3a_professional_3313_performance.csv',index = False)
