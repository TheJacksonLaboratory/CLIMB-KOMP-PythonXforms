# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:08:14 2020

@author: rushn
"""
import os
from statistics import mean
import numpy as np
import pandas as pd

def listFiles(importPath):
    fileList = [f for f in os.listdir(importPath) if os.path.isfile(os.path.join(importPath, f))]
    return fileList

def get_field_sub(x): 
    return x.split('_')[0]

def best_fit_slope(ys):
    xs=np.array([300,600,900,1200])
    m = (((mean(xs)*mean(ys)) - mean(xs*ys)) /
         ((mean(xs)**2) - mean(xs**2)))
    return m * 100

def readcsv(importPath, skiprows, nrow=None):
    data = pd.read_csv(str(importPath), skiprows=skiprows, nrows=nrow)
    return data

def readxlsx(importPath, skiprows, nrow=None):
    data = pd.read_excel(str(importPath), skiprows=skiprows, nrows=nrow)
    return data

def calculations_LD(data):
    data['Reaction Time'] = data.groupby(['Key'])['LATENCY (CENTROID)'].transform('nth', 0)
    data['Side Changes'] = data.groupby(['Key'])['ENTRY COUNT (CENTROID)'].transform('nth', 0)
    data['Left Side Time Spent (s)'] = data.groupby(['Key'])['DURATION (CENTROID)'].transform('nth', 1)
    data['Left Side Mobile Time Spent (s)'] = data.groupby(['Key'])['AMBULATORY TIME (CENTROID)'].transform('nth', 1)
    data['Right Side Time Spent (s)'] = data.groupby(['Key'])['DURATION (CENTROID)'].transform('nth', 0)
    data['Right Side Mobile Time Spent (s)'] = data.groupby(['Key'])['AMBULATORY TIME (CENTROID)'].transform('nth', 0)
    data['Percent Time in Dark (%)'] = data['Right Side Time Spent (s)']/(1200 - data['Reaction Time'])*100
    data['Percent Time in Light (%)'] = (data['Left Side Time Spent (s)'] - data['Reaction Time'])/(1200 - data['Reaction Time'])*100
    data.loc[(data['DURATION (CENTROID)'] == 0) & (data['ZONE'] == 'Dark'), 'No Transition'] = 'No'
    data.loc[(data['DURATION (CENTROID)'] == 1200) & (data['ZONE'] == 'Light'), 'No Transition'] = 'No'
    data.loc[(data['No Transition'].isnull()), 'No Transition'] = 'Yes'
    data.loc[(data['Left Side Time Spent (s)'] == 1200), 'Reaction Time']=1200
    data.loc[(data['Left Side Time Spent (s)'] == 1200), 'Percent Time in Dark (%)']=0
    data.loc[(data['Left Side Time Spent (s)'] == 1200), 'Percent Time in Light (%)']=100
    return data


def final_data_LD(data):
    colNameNew = ['Key',
                  'Animal',
                  'Sample',
                  'Collected Date',
                  'Collected By',
                  'Reaction Time',
                  'Side Changes',
                  'Left Side Time Spent (s)',
                  'Left Side Mobile Time Spent (s)',
                  'Right Side Time Spent (s)',
                  'Right Side Mobile Time Spent (s)',
                  'Percent Time in Dark (%)',
                  'Percent Time in Light (%)',
                  'No Transition',
                  'Comments']
    #Selecting final table of columns for LIMS
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final

def calculations_CDO_OF(data):
    #data['Start time'] = data.groupby(['Key'])['START TIME'].transform('nth', 0)
    data['Whole Arena Permanence Time (s)'] = data.groupby(['Key'])['Sample'].transform('sum')
    data['Distance Traveled Total (cm)'] = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('sum')
    data['Distance Traveled First Five Minutes (cm)'] = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 0)
    data['Distance Traveled Second Five Minutes (cm)'] = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 1)
    data['Distance Traveled Third Five Minutes (cm)'] = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 2)
    data['Distance Traveled Fourth Five Minutes (cm)'] = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 3)
    data['Whole Arena Average Speed (cm/s)'] = data['Distance Traveled Total (cm)']/1200
    data['Whole Arena Resting Time (s)'] = data.groupby(['Key'])['REST TIME (s)'].transform('sum')
    data['Time Spent Mobile (s)'] = data.groupby(['Key'])['AMBULATORY TIME (s)'].transform('sum')
    data['Number of Rears Total'] = data.groupby(['Key'])['VERTICAL EPISODE COUNT'].transform('sum')
    data['Number of Rears First Five Minutes'] = data.groupby(['Key'])['VERTICAL EPISODE COUNT'].transform('nth', 0)
    data['Number of Rears Second Five Minutes'] = data.groupby(['Key'])['VERTICAL EPISODE COUNT'].transform('nth', 1)
    data['Number of Rears Third Five Minutes'] = data.groupby(['Key'])['VERTICAL EPISODE COUNT'].transform('nth', 2)
    data['Number of Rears Fourth Five Minutes'] = data.groupby(['Key'])['VERTICAL EPISODE COUNT'].transform('nth', 3)
    data['Vertical Time (s)'] = data.groupby(['Key'])['VERTICAL TIME (s)'].transform('sum')
    data['Clockwise'] = data.groupby(['Key'])['CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Counter Clockwise'] = data.groupby(['Key'])['COUNTER-CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Distance Traveled Habituation Ratio (%)'] = ((data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 3)/300*100)-(data.groupby(['Key'])['TOTAL DISTANCE (cm)'].transform('nth', 0)/300*100))/3
    
    distance_avg = data.groupby(['Key'])['TOTAL DISTANCE (cm)'].apply(best_fit_slope)

    distance_avg = distance_avg.rename('Distance Traveled Slope (%)')
    
    data = pd.merge(data, distance_avg, left_on='Key', right_index=True)
    
    return data

def calculations_ZDS_OF(data):
    data[list(data)] = data[list(data)].astype(str)
    data = data.groupby(['Key', 'SAMPLE', 'Collected Date', 'Collected By', 'Comments'], \
                        as_index=False, sort=False).agg(','.join)
    data[['DURATION_P','DURATION_CORNER','DURATION_CENTER']] = data['DURATION (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['ENTRY_P','ENTRY_CORNER','ENTRY_CENTER']] = data['ENTRY COUNT (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['DISTANCE_P','DISTANCE_CORNER','DISTANCE_CENTER']] = data['TOTAL DISTANCE (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['REST_P','REST_CORNER','REST_CENTER']] = data['REST TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['AMBULATORY_P','AMBULATORY_CORNER','AMBULATORY_CENTER']] = data['AMBULATORY TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['Key', 'SAMPLE']] = data[['Key', 'SAMPLE']].apply(pd.to_numeric)
    
    data['Periphery Distance Traveled (cm)'] = data.groupby(['Key'])['DISTANCE_P'].transform('sum')+data.groupby(['Key'])['DISTANCE_CORNER'].transform('sum')
    data['Periphery Resting Time (s)'] = data.groupby(['Key'])['REST_P'].transform('sum')+data.groupby(['Key'])['REST_CORNER'].transform('sum')
    data['Periphery Permanence Time (s)'] = data.groupby(['Key'])['DURATION_P'].transform('sum')+data.groupby(['Key'])['DURATION_CORNER'].transform('sum')
    data['Periphery Average Speed (cm/s)'] = data['Periphery Distance Traveled (cm)']/data['Periphery Permanence Time (s)']
    data['Center Distance Traveled (cm)'] = data.groupby(['Key'])['DISTANCE_CENTER'].transform('sum')
    data['Center Permanence Time (s)'] = data.groupby(['Key'])['DURATION_CENTER'].transform('sum')
    data['Center Resting Time (s)'] = data.groupby(['Key'])['REST_CENTER'].transform('sum')
    data['Center Average Speed (cm/s)'] = data['Center Distance Traveled (cm)']/data['Center Permanence Time (s)']
    data['Number of Center Entries'] = data.groupby(['Key'])['ENTRY_CENTER'].transform('sum')
    data['Percent Time Center (%)'] = data['Center Permanence Time (s)']/1200*100
    data['Corners Permanence Time (s)'] = data.groupby(['Key'])['DURATION_CORNER'].transform('sum')
    data['Percent Time Corners (%)'] = data['Corners Permanence Time (s)']/1200*100
    data['Percent Time Corners Habituation Ratio (%)'] = ((data.groupby(['Key'])['DURATION_CORNER'].transform('nth', 3)/300*100)-(data.groupby(['Key'])['DURATION_CORNER'].transform('nth', 0)/300*100))/3
    data['Percent Time Center Habituation Ratio (%)'] = ((data.groupby(['Key'])['DURATION_CENTER'].transform('nth', 3)/300*100)-(data.groupby(['Key'])['DURATION_CENTER'].transform('nth', 0)/300*100))/3

    corner_avg = data.groupby(['Key'])['DURATION_CORNER'].apply(best_fit_slope)
    corner_avg = corner_avg.rename('Percent Time Corners Slope (%)')
    center_avg = data.groupby(['Key'])['DURATION_CENTER'].apply(best_fit_slope)
    center_avg = center_avg.rename('Percent Time Center Slope (%)')
    
    avgs = pd.merge(corner_avg, center_avg, left_index=True, right_index=True)
    
    data = pd.merge(data, avgs, left_on='Key', right_index=True)
    return data

def data_CDO_OF(data):
    colNameNew = ['Key',
                  'Sample',
                  'Collected Date',
                  'Collected By',
                  'Animal',
                  'Whole Arena Permanence Time (s)',
                  'Distance Traveled Total (cm)',
                  'Distance Traveled First Five Minutes (cm)',
                  'Distance Traveled Second Five Minutes (cm)',
                  'Distance Traveled Third Five Minutes (cm)',
                  'Distance Traveled Fourth Five Minutes (cm)',
                  'Whole Arena Average Speed (cm/s)',
                  'Whole Arena Resting Time (s)',
                  'Time Spent Mobile (s)',
                  'Number of Rears Total',
                  'Number of Rears First Five Minutes',
                  'Number of Rears Second Five Minutes',
                  'Number of Rears Third Five Minutes',
                  'Number of Rears Fourth Five Minutes',
                  'Vertical Time (s)',
                  'Clockwise',
                  'Counter Clockwise',
                  'Distance Traveled Habituation Ratio (%)',
                  'Distance Traveled Slope (%)',
                  'Comment']

        #Selecting final table of columns for LIMS
    data_CDO = data[colNameNew].reset_index(drop=True)
    return data_CDO

def data_ZDS_OF(data):
    colNameNew = ['Key',
                  'Periphery Distance Traveled (cm)',
                  'Periphery Resting Time (s)',
                  'Periphery Permanence Time (s)',
                  'Periphery Average Speed (cm/s)',
                  'Center Distance Traveled (cm)',
                  'Center Resting Time (s)',
                  'Center Permanence Time (s)',
                  'Center Average Speed (cm/s)',
                  'Number of Center Entries',
                  'Percent Time Center (%)',
                  'Percent Time Center Habituation Ratio (%)',
                  'Percent Time Corners (%)',
                  'Percent Time Corners Habituation Ratio (%)',
                  'Corners Permanence Time (s)',
                  'Percent Time Center Slope (%)',
                  'Percent Time Corners Slope (%)']
        
        
    data_ZDS = data[colNameNew].reset_index(drop=True)
    return data_ZDS

def final_data_Hem(data):
    colNameNew = [ 'Key',
                  'White Blood Cells (WBC) (x10e3 cells/ul)',
                  'Red Blood Cells (RBC) (x10e6 cells/ul)',
                  'Measured Hemoglobin (mHGB) (g/dL)',
                  'Hematocrit (HCT) (%)',
                  'Mean Cell Volume (MCV) (fL)',
                  'Mean Corpuscular Hemoglobin (CHg) (pg)',
                  'Mean Cell Hemoglobin Concentration (MCHC) (g/dL)',
                  'Platelet Count (PLT) (x10e3 cells/ul)',
                  'Red Cell Distr. Width (RDW) (%)',
                  'Mean Platelet Volume (MPV) (fL)',
                  'Nucleated Red Blood Cell Count (x10e3 cells/ul)',
                  'Nucleated Red Blood Cell Percentage (%)',
                  'Neurophil Cell Count (x10e3 cells/ul)',
                  'Lymphocyte Cell Count (x10e3 cells ul)',
                  'Monocyte Cell Count (x10e3 cells ul)',
                  'Eosinophil Cell Count (x10e3 cells ul)',
                  'Basophil Cell Count (x10e3 cells ul)',
                  'Neutrophils (NEUT) (%)',
                  'Lymphocytes (LYM) (%)',
                  'Monocytes (MONO) (%)',
                  'Eosinophils (EOS) (%)',
                  'Basophils (BASO) (%)',
                  'Percent Retic (%)',
                  'Reticulocytes (Retic) (x10e9 cells/liter)',
                  'Percent Immature Retic (%)',
                  'Percent Low Fluorescing Retic (%)',
                  'Percent Middle Fluorescing Retic (%)',
                  'Percent High Fluorescing Retic (%)',
                  'Retic Hemoglobin (CHr) (pg)',
                  'Platelet Count (Optical) (x10e3 cells/ul)',
				  'Collected By', 
				  'Collected Date',
				  # CLIMB columns
                  'Animal',  
				  'Sample'
				  ]

    data.columns = colNameNew
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final


def calculations_HB(data):
    values = dict(data.groupby('Key')['HOLE NAME'].apply(list))
    poke_counts = {}
    for key in values:
        test = {x:values[key].count(x) for x in values[key]}
        poke_counts.update({key:test})
    
    counts = pd.DataFrame.from_dict(poke_counts, orient='index')
    counts = counts.reindex(sorted(counts.columns), axis=1)
    counts.columns = ['Total Hole Pokes Hole ' + str(col) for col in counts.columns]
    data = pd.merge(data, counts, left_on = ['Key'], right_index=True)
    
    data['HOLE NAME'] = data['HOLE NAME'].astype(str)
    sequence = data.groupby(['Key'])['HOLE NAME'].apply('-'.join).reset_index()
    sequence = sequence.rename(columns={'HOLE NAME':'Holepoke Sequence'})
    data = pd.merge(data, sequence, on=['Key'])
    data = data.drop(columns = ['HOLE NAME']).drop_duplicates().reset_index(drop=True)
    data.fillna(0, inplace=True)


    return data