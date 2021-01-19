# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 14:29:40 2020

@author: Joyqiao
"""


# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 15:58:06 2020

@author: Joyqiao

Assumption: tma is for unique patient; Primeid is for unique account that may involve multiple patients
this time we don't delete the tma which some of their tma links to multiple tma
since i want to confirm if one claim number only link to one tma or one prime id
since after using the mapping file 
we still have aroung 100k records in 1/16 file that are duplicares
also we do not involve the transplant or the claim extracted stage'
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import sys
sys.path.append(r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project\code')
from transplant_dialysis import clean_transplant_dialysis


basepath = r'C:\Users\Joyqiao\Documents\CMU\RA\HIGHMARK Trajectory claim\HIghmark Project'

'''
#2. if still one claim number matches to two TMA in the new mapping, keep the mapping with smallest patient TMA acct value and delete the rest, leave the valid mapping relation to correct the previous one
#save paitnet with multiple TMA acct as a mapping
duplicate_mapping =  mapping[mapping.duplicated(['claim_number_deidentified'],keep = False)]
len(duplicate_mapping['TMA_Acct'].unique())#597 unique tma acct
#since one claim number could only associated to one patient, if we idnetify any claim that matches to multiple TMA Acct, we treat these Acct as one patient.
duplicate_map = duplicate_mapping.sort_values(['TMA_Acct']).groupby(['claim_number_deidentified'])['TMA_Acct'].unique()
duplicate_map = pd.DataFrame([i.tolist() for i in duplicate_map])
duplicate_map.rename(columns = {0:'keep_acct'},inplace = True)
duplicate_map.drop_duplicates(keep = 'first',inplace = True)
duplicate_map.to_csv('patient_with_multiple_acct.csv')

#for i in duplicate_map['keep_acct']:
#    print(duplicate_map[duplicate_map.eq(i).any(1)])

#new mapping(DELETE ALL ERROR LINKS)
mapping = mapping.sort_values(['TMA_Acct']).drop_duplicates(subset = 'claim_number_deidentified',keep = 'first')
len(mapping['claim_number_deidentified'].unique())
mapping['TMA_Acct'].nunique() #9509
'''


def map_process(map_file):
    '''
    find all the claims that link to different tma( which should not occur as it is a mapping file. each 
                                                                    claim should only be matched with A UNIQUE PATIENT)
    Parameters
    ----------
    map_file : TYPE str
        DESCRIPTION. 
          
    Returns mapping
    Write our a csv file of the problematic tma
    ------
    '''
    os.chdir(basepath+'\mapping')
    map1 = pd.read_csv('df_claim_to_member_mapping_part1.csv')
    map2 = pd.read_csv('df_claim_to_member_mapping_part2.csv')
    map3 = pd.read_csv('df_claim_to_member_mapping_part3.csv')
    map4 = pd.read_csv('df_claim_to_member_mapping_part4.csv')
    map5 = pd.read_csv('df_claim_to_member_mapping_part5.csv')
    map6 = pd.read_csv('df_claim_to_member_mapping_part6.csv')
    map7 = pd.read_csv('df_claim_to_member_mapping_part7.csv')
    mapping = pd.concat([map1,map2,map3,map4,map5,map6,map7],ignore_index = True)
    mapping['TMA_Acct'].nunique() #There is total 9668 patients in mapping 
    
    new = pd.read_csv(map_file)
    mapping = mapping.merge(new, how = 'outer',on = 'claim_number_deidentified',indicator = True)
    mapping.loc[mapping['_merge'] == 'both','TMA_Acct'] = mapping['TMA Acct#']
    mapping = mapping.drop(columns = ['TMA Acct#','_merge'])
    mapping.drop_duplicates(keep = False,inplace = True)
    
    #find the lines that claim match with 2 or more TMA matched
    duplicate_mapping =  mapping[mapping.duplicated(['claim_number_deidentified'],keep = False)]
    #extract the involved tma account list
    drop_member_list = duplicate_mapping['TMA_Acct'].drop_duplicates(keep = 'first')
    #clean up the mapping 
    mapping = mapping.drop_duplicates(subset = 'claim_number_deidentified',keep = False)
    #how many claims left in the mapping
    len(mapping['claim_number_deidentified'].unique())
    #how many unique patient left in the mapping
    mapping['TMA_Acct'].nunique() 

    drop_member_list.to_csv(basepath+'\data\sorted_data\drop_member_list.csv',index = False)
    
    return mapping,drop_member_list




# =============================================================================
# rearrange the data to cut the begin and end part into a separate file
# =============================================================================
def cut_startend_patient_todata(read_in,output_filename):
    '''
    since the begin and ending part of each file may exists patients that are in other files. To make the drop_duplicate method works right,
    we extract all the first and last patients that already sorted in each file into the (+1) file
    Parameters
    ----------
    read_in : TYPE str 
        DESCRIPTION. filepath
    output_filename : TYPE str
        DESCRIPTION. 

    Returns
    -------
    data : TYPE  dataframe
        DESCRIPTION. After process df

    '''
    data = pd.read_csv(read_in)
    start_cut = data['TMA Acct#'] ==  int(data[:1]['TMA Acct#'])
    data[start_cut].to_csv(output_filename,mode = 'a')
    data = data[~start_cut]
    
    end_cut = data['TMA Acct#'] == int(data[-1:]['TMA Acct#'])
    data[end_cut].to_csv(output_filename,mode = 'a')
    data = data[~end_cut]
    return data

# =============================================================================
#consider to mannuly match the patient TMA Account for those patients with multiple TMA number according to mapping dataset
#sum(mapping.groupby(['claim_number_deidentified']).count()['TMA_Acct'] > 2)+sum(mapping.groupby(['claim_number_deidentified']).count()['TMA_Acct'] == 1)

# =============================================================================

def drop_problematic_members(data):
    '''
    remove patients fall into patient with multiple TMA(patient_withTMA)


    Parameters
    ----------
    data : TYPE dataframe
        DESCRIPTION.

    Returns
    -------
    data : TYPE dataframe
        DESCRIPTION.

    '''
    data = data[~data['TMA Acct#'].isin(drop_member_list)]
    return data


# =============================================================================
# clean data based on scenarios
# =============================================================================


def clean(df):    
    df = df.drop_duplicates(keep = 'first')                
    
    #select the useful columns for later analysis
    #df = data[['claim_type', 'line_number','eacl_prv_alcrg_at','proc_code','TMA Acct#', 'claim_number_deidentified','icrd_dt_deidentified']]
    
    # =============================================================================
    #1 Scenaro 1  join with upated claim_Tma mapping( To resolve the error that some claim number are matching two TMA )
    df = df.merge(mapping, how = 'left',on = 'claim_number_deidentified')
    df.loc[df.claim_number_deidentified.isin(mapping.claim_number_deidentified),'TMA Acct#'] = df['TMA_Acct']      #TMA Acct if from the df, TMA_Acct is from mapping
    df = df.drop(columns = ['TMA_Acct'])
    

    #verify if all df claim# are covered within mapping data
    len(df) - sum(df.claim_number_deidentified.isin(mapping.claim_number_deidentified))
    #corrected patient number 
    df['TMA Acct#'].nunique()
    df.rename(columns = {'TMA Acct#':'TMA_Acct'},inplace = True)

    #verify there's no claims match to multiple TMA 
    
    '''b=df.groupby(['claim_number_deidentified']).agg({'TMA_Acct':'count'})
    
    print('claims matches to multiple tma {}'.format(sum(b.iloc[:,0]>1)))
    '''
              
    #save the Mapping of all the claim with multiple TMA links
    #df.loc[~df.claim_number_deidentified.isin(mapping.claim_number_deidentified),['TMA Acct#','claim_number_deidentified']].drop_duplicates(keep = 'first').to_csv('One_claim_matches_multiple_TMA.csv')
    
    #delete all the patients that are in patient_withTMA list
    #df = df[~df['TMA Acct#'].isin(patient_withTMA )] #8
    #len(df['TMA Acct#'].unique()) #8160
    
    # =============================================================================
    # rejection rule: as long as claim number, TMA proc code, incurred data cost are the same, keep only one of the duplicates    
    # =============================================================================
    df_filter = df.drop_duplicates(subset = ['TMA_Acct','claim_number_deidentified','proc_code','eacl_prv_alcrg_at','icrd_dt_deidentified','claim_type'],keep = 'first')


    # =============================================================================
    #2 Scenario 2 It means if there is a claim, where all of its lines have values only <= 0, then you should ignore them.
    '''
    grouped=df.groupby(['claim_number_deidentified','proc_code'])
    
    df_filter = grouped.filter(lambda x: (x['eacl_prv_alcrg_at']>0).any())
    #save claims that only have zero or negative costs
    df.merge(df_filter,how = 'outer',indicator = True).loc[lambda x: x['_merge']=='left_only'].to_csv('cost_Negative_only_claims.csv')     
    len(df_filter['TMA_Acct'].unique()) '''
    
                                            
    
     #scenario 3      remove rejected line
    # =============================================================================
    # INSPECT if for the same patient and claim number and proc_code, how many lines have the same absolote cost value 
    #df_filter =df_filter[df_filter['eacl_prv_alcrg_at'] != 0]
    
    #gb_filter = df_filter.groupby(['TMA_Acct','claim_number_deidentified','proc_code','eacl_prv_alcrg_at'],as_index = False).size().reset_index().rename(columns = {0:'count'})
    #gb_filter['count'] = np.where(gb_filter['eacl_prv_alcrg_at']<0,gb_filter['count']*(-1),gb_filter['count'])
    #gb_filter_count = gb_filter.groupby(['TMA_Acct','claim_number_deidentified','proc_code'])['eacl_prv_alcrg_at','count'].sum()
    #gb_filter_other = gb_filter_count[~(gb_filter_count['eacl_prv_alcrg_at'] > -0.001) & (gb_filter_count['eacl_prv_alcrg_at']< 0.001)].reset_index()
    #len(gb_filter_other['TMA_Acct'].unique())
    

    # =============================================================================
    # filter df keep
    # =============================================================================
    connection = sqlite3.connect(':memory:')
    df_filter.to_sql('alldata',connection)
    query1 = '''
            select tab1.*
            from alldata tab1 left join alldata tab2 
            on tab1.eacl_prv_alcrg_at = -tab2.eacl_prv_alcrg_at and
            tab1.proc_code = tab2.proc_code and
            tab1.icrd_dt_deidentified = tab2.icrd_dt_deidentified and
            tab1.TMA_Acct = tab2.TMA_Acct and
            tab1.claim_type = tab2.claim_type and
            tab1.claim_number_deidentified = tab2.claim_number_deidentified
            where tab2.claim_number_deidentified is null
            '''
    df_keep = pd.read_sql_query(query1,connection) 
    df_keep.set_index(['index'],inplace = True)
    df_process = df_filter[~df_filter.index.isin(df_keep.index)]

    #df_keep.to_sql('keepdata',connection,index = False)       
    # =============================================================================
    # record still have negative cost, no matching positive but not all corresponding claims are of negative
    # =============================================================================
    c=df_keep[df_keep['eacl_prv_alcrg_at']  <= 0]
    c.to_csv('negative_record_with_no_matching_one_with_positive_in_claims.csv',mode = 'a')
    df_keep = df_keep[df_keep['eacl_prv_alcrg_at']>0]
    # =============================================================================
    # filter df process
    # total = price for each record* #records
    # =============================================================================
    
    # gb = df_process.groupby(['TMA_Acct','claim_number_deidentified','proc_code','eacl_prv_alcrg_at','icrd_dt_deidentified','claim_type'],as_index = False).size().reset_index().rename(columns = {0:'count'})
    # gb['total'] = gb['count']*gb['eacl_prv_alcrg_at']
    # gb['cost_abs'] = gb['eacl_prv_alcrg_at'].abs()
    
    # #gb['count'] = np.where(gb['eacl_prv_alcrg_at']<0,gb['count']*(-1),gb['count'])
    # gb_count = gb.groupby(['TMA_Acct','claim_number_deidentified','proc_code','cost_abs','icrd_dt_deidentified','claim_type'])['total','count'].sum()
    # if sum(gb_count['total']==0) == len(gb_count):
    df_process =  df_process[df_process['eacl_prv_alcrg_at']>0]



    '''
# =============================================================================
#     even scenario
# =============================================================================
    
    even_keep = gb_count.loc[(gb_count['count']%2 ==0) & (gb_count['total']== 0)].reset_index()
#    for i in range(1, max(gb_count['count'])//2):
#        double_temp = gb_count.loc[(gb_count['count']== 2*i) & (gb_count['total']== 0)]
#        double_keep = double_keep.append([double_temp],ignore_index=True)
    
    gb_odd = gb_count[(gb_count['total'] !=0)].reset_index()
    # =============================================================================
    # triple lines scenario
    # =============================================================================
#    gb_triple = gb_process[gb_process['total']/gb_process['cost_abs'] == gb_process['count']/3]
#    gb_other = gb_process[~gb_process.index.isin(gb_triple.index)]
#    gb_triple['count'] /=3
#    gb_triple.drop(columns = ['total'],inplace = True)
#    
#    for i in range(1, int(max(gb_triple['count']))):
#        gb_temp = gb_triple[gb_triple['count']== (i+1)]
#        gb_triple = gb_triple.append([gb_temp]*i,ignore_index=True)
#    
#    gb_triple.drop(columns = ['count'],inplace = True)
#    triple_keep=gb_triple.rename(columns = {'cost_abs':'eacl_prv_alcrg_at'})
    
    # =============================================================================
    # odd scenario
    # =============================================================================
    gb_odd['triple_count'] = round(gb_odd['total']/gb_odd['cost_abs'])
    gb_odd['double_count'] = (gb_odd['count'] - gb_odd['triple_count'])/2
    
    for i in range(1, int(max(gb_odd['triple_count']))+1):
        if i ==1:
            continue
        else:       
            gb_temp = gb_odd[gb_odd['triple_count']== i]
            gb_odd= gb_odd.append([gb_temp]*(i-1),ignore_index=True)
    for i in range(1, int(max(gb_odd['double_count']))+1):
        gb_temp = gb_odd[gb_odd['double_count']== i]
        gb_odd= gb_odd.append([gb_temp]*i,ignore_index=True)
        
    gb_odd.drop(columns = ['total','count','triple_count','double_count'],inplace = True)
    gb_odd=gb_odd.rename(columns = {'cost_abs':'eacl_prv_alcrg_at'})
    
    df_keep.drop(columns = ['line_number'],inplace = True)
    cleaned=pd.concat([df_keep,even_keep,gb_odd])
   '''
    cleaned=pd.concat([df_keep,df_process])
    return cleaned



# =============================================================================
# main 
    # if to data_15 then will generate data_16 so range_number is 17
# =============================================================================

def main(range_number,read_file_name,output_file_name):
    os.chdir(basepath+'\data\sorted_data')
    os.remove(read_file_name.format(i = (range_number-1)))
    if os.path.exists('transplant_patient.csv'):
        os.remove('transplant_patient.csv')
    else:
        print("Sorry, I can not remove transplant file." )
        
    if os.path.exists('dialysis_patient.csv'):
        os.remove('dialysis_patient.csv')
    else:
        print("Sorry, I can not remove dialysis file.")
    for i in range(0,range_number): 
        print(i)
        if i != int(range_number-1):           
            data = cut_startend_patient_todata(read_file_name.format(i = i),read_file_name.format(i = (range_number-1)))
        #the patient number we have in 
        #print(data['TMA Acct#'].nunique()) 

        data = drop_problematic_members(data)
        #drop transplant records
        cleaned = clean(data)
        
        cleaned = clean_transplant_dialysis(cleaned)

        #print(cleaned['TMA_Acct'].nunique()) 

        cleaned.to_csv(output_file_name.format(i=i))

mapping, drop_member_list = map_process('df_new_claim_to_TMA_mapping.csv')
 
 
main(16+1,'data_sorted_{i}.csv',basepath+'\data\cleaned_data\cleaned_{i}.csv')



