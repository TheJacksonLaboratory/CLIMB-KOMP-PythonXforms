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
    data['Reaction Time'] = data.groupby(['Test Code'])['LATENCY (CENTROID)'].transform('nth', 0)
    data['Side Changes'] = data.groupby(['Test Code'])['ENTRY COUNT (CENTROID)'].transform('nth', 0)
    data['Left Side Time Spent'] = data.groupby(['Test Code'])['DURATION (CENTROID)'].transform('nth', 1)
    data['Left Side Mobile Time Spent'] = data.groupby(['Test Code'])['AMBULATORY TIME (CENTROID)'].transform('nth', 1)
    data['Right Side Time Spent'] = data.groupby(['Test Code'])['DURATION (CENTROID)'].transform('nth', 0)
    data['Right Side Mobile Time Spent'] = data.groupby(['Test Code'])['AMBULATORY TIME (CENTROID)'].transform('nth', 0)
    data['Pct Time in Dark'] = data['Right Side Time Spent']/(1200 - data['Reaction Time'])*100
    data['Pct Time in Light'] = (data['Left Side Time Spent'] - data['Reaction Time'])/(1200 - data['Reaction Time'])*100
    data.loc[(data['DURATION (CENTROID)'] == 0) & (data['ZONE'] == 'Dark'), 'No Transition'] = 'No'
    data.loc[(data['DURATION (CENTROID)'] == 1200) & (data['ZONE'] == 'Light'), 'No Transition'] = 'No'
    data.loc[(data['No Transition'].isnull()), 'No Transition'] = 'Yes'
    data.loc[(data['Left Side Time Spent'] == 1200), 'Reaction Time']=1200
    data.loc[(data['Left Side Time Spent'] == 1200), 'Pct Time in Dark']=0
    data.loc[(data['Left Side Time Spent'] == 1200), 'Pct Time in Light']=100
    return data


def final_data_LD(data):
    colNameNew = ['Test Code',
                  'Date of test',
                  'Experimenter ID',
                  'Arena ID',
                  'Start time',
                  'Sample Duration',
                  'Reaction Time',
                  'Side Changes',
                  'Left Side Time Spent',
                  'Left Side Mobile Time Spent',
                  'Right Side Time Spent',
                  'Right Side Mobile Time Spent',
                  'Pct Time in Dark',
                  'Pct Time in Light',
                  'No Transition',
                  'Comments']
    #Selecting final table of columns for LIMS
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final

def calculations_CDO_OF(data):
    data['Start time'] = data.groupby(['Test Code'])['START TIME'].transform('nth', 0)
    data['Whole Arena Permanence Time'] = data.groupby(['Test Code'])['Sample Duration'].transform('sum')
    data['Distance Traveled Total'] = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('sum')
    data['Distance Traveled First 5 Min'] = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 0)
    data['Distance Traveled Second 5 Min'] = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 1)
    data['Distance Traveled Third 5 Min'] = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 2)
    data['Distance Traveled Fourth 5 Min'] = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 3)
    data['Whole Arena Average Speed'] = data['Distance Traveled Total']/1200
    data['Whole Arena Resting Time'] = data.groupby(['Test Code'])['REST TIME (s)'].transform('sum')
    data['Time Spent Mobile'] = data.groupby(['Test Code'])['AMBULATORY TIME (s)'].transform('sum')
    data['Number of Rears Total'] = data.groupby(['Test Code'])['VERTICAL EPISODE COUNT'].transform('sum')
    data['Number of Rears First Five Minutes'] = data.groupby(['Test Code'])['VERTICAL EPISODE COUNT'].transform('nth', 0)
    data['Number of Rears Second Five Minutes'] = data.groupby(['Test Code'])['VERTICAL EPISODE COUNT'].transform('nth', 1)
    data['Number of Rears Third Five Minutes'] = data.groupby(['Test Code'])['VERTICAL EPISODE COUNT'].transform('nth', 2)
    data['Number of Rears Fourth Five Minutes'] = data.groupby(['Test Code'])['VERTICAL EPISODE COUNT'].transform('nth', 3)
    data['Vertical Time'] = data.groupby(['Test Code'])['VERTICAL TIME (s)'].transform('sum')
    data['Clockwise'] = data.groupby(['Test Code'])['CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Counter Clockwise'] = data.groupby(['Test Code'])['COUNTER-CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Distance Traveled Habituation Ratio'] = ((data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 3)/300*100)-(data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].transform('nth', 0)/300*100))/3
    
    distance_avg = data.groupby(['Test Code'])['TOTAL DISTANCE (cm)'].apply(best_fit_slope)

    distance_avg = distance_avg.rename('Distance Traveled Slope')
    
    data = pd.merge(data, distance_avg, left_on='Test Code', right_index=True)
    
    return data

def calculations_ZDS_OF(data):
    data[list(data)] = data[list(data)].astype(str)
    data = data.groupby(['Test Code', 'SAMPLE', 'Date of test', 'Experimenter ID', 'Comments'], \
                        as_index=False, sort=False).agg(','.join)
    data[['DURATION_P','DURATION_CORNER','DURATION_CENTER']] = data['DURATION (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['ENTRY_P','ENTRY_CORNER','ENTRY_CENTER']] = data['ENTRY COUNT (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['DISTANCE_P','DISTANCE_CORNER','DISTANCE_CENTER']] = data['TOTAL DISTANCE (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['REST_P','REST_CORNER','REST_CENTER']] = data['REST TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['AMBULATORY_P','AMBULATORY_CORNER','AMBULATORY_CENTER']] = data['AMBULATORY TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['Test Code', 'SAMPLE']] = data[['Test Code', 'SAMPLE']].apply(pd.to_numeric)
    
    data['Periphery Distance Traveled'] = data.groupby(['Test Code'])['DISTANCE_P'].transform('sum')+data.groupby(['Test Code'])['DISTANCE_CORNER'].transform('sum')
    data['Periphery Resting Time'] = data.groupby(['Test Code'])['REST_P'].transform('sum')+data.groupby(['Test Code'])['REST_CORNER'].transform('sum')
    data['Periphery Permanence Time'] = data.groupby(['Test Code'])['DURATION_P'].transform('sum')+data.groupby(['Test Code'])['DURATION_CORNER'].transform('sum')
    data['Periphery Average Speed'] = data['Periphery Distance Traveled']/data['Periphery Permanence Time']
    data['Center Distance Traveled'] = data.groupby(['Test Code'])['DISTANCE_CENTER'].transform('sum')
    data['Center Permanence Time'] = data.groupby(['Test Code'])['DURATION_CENTER'].transform('sum')
    data['Center Resting Time'] = data.groupby(['Test Code'])['REST_CENTER'].transform('sum')
    data['Center Average Speed'] = data['Center Distance Traveled']/data['Center Permanence Time']
    data['Number of Center Entries'] = data.groupby(['Test Code'])['ENTRY_CENTER'].transform('sum')
    data['PctTime Center'] = data['Center Permanence Time']/1200*100
    data['Corners Permanence Time'] = data.groupby(['Test Code'])['DURATION_CORNER'].transform('sum')
    data['PctTime Corners'] = data['Corners Permanence Time']/1200*100
    data['PctTime Corners Habituation Ratio'] = ((data.groupby(['Test Code'])['DURATION_CORNER'].transform('nth', 3)/300*100)-(data.groupby(['Test Code'])['DURATION_CORNER'].transform('nth', 0)/300*100))/3
    data['PctTime Center Habituation Ratio'] = ((data.groupby(['Test Code'])['DURATION_CENTER'].transform('nth', 3)/300*100)-(data.groupby(['Test Code'])['DURATION_CENTER'].transform('nth', 0)/300*100))/3

    corner_avg = data.groupby(['Test Code'])['DURATION_CORNER'].apply(best_fit_slope)
    corner_avg = corner_avg.rename('PctTime Corners Slope')
    center_avg = data.groupby(['Test Code'])['DURATION_CENTER'].apply(best_fit_slope)
    center_avg = center_avg.rename('PctTime Center Slope')
    
    avgs = pd.merge(corner_avg, center_avg, left_index=True, right_index=True)
    
    data = pd.merge(data, avgs, left_on='Test Code', right_index=True)
    return data

def data_CDO_OF(data):
    colNameNew = ['Test Code',
                  'Date of test',
                  'Experimenter ID',
                  'Arena ID',
                  'Start time',
                  'Sample Duration',
                  'Whole Arena Permanence Time',
                  'Distance Traveled Total',
                  'Distance Traveled First 5 Min',
                  'Distance Traveled Second 5 Min',
                  'Distance Traveled Third 5 Min',
                  'Distance Traveled Fourth 5 Min',
                  'Whole Arena Average Speed',
                  'Whole Arena Resting Time',
                  'Time Spent Mobile',
                  'Number of Rears Total',
                  'Number of Rears First Five Minutes',
                  'Number of Rears Second Five Minutes',
                  'Number of Rears Third Five Minutes',
                  'Number of Rears Fourth Five Minutes',
                  'Vertical Time',
                  'Clockwise',
                  'Counter Clockwise',
                  'Distance Traveled Habituation Ratio',
                  'Distance Traveled Slope',
                  'Comments']

        #Selecting final table of columns for LIMS
    data_CDO = data[colNameNew].reset_index(drop=True)
    return data_CDO

def data_ZDS_OF(data):
    colNameNew = ['Test Code',
                  'Periphery Distance Traveled',
                  'Periphery Resting Time',
                  'Periphery Permanence Time',
                  'Periphery Average Speed',
                  'Center Distance Traveled',
                  'Center Resting Time',
                  'Center Permanence Time',
                  'Center Average Speed',
                  'Number of Center Entries',
                  'PctTime Center',
                  'PctTime Center Habituation Ratio',
                  'PctTime Corners',
                  'PctTime Corners Habituation Ratio',
                  'Corners Permanence Time',
                  'PctTime Center Slope',
                  'PctTime Corners Slope']
        
        
    data_ZDS = data[colNameNew].reset_index(drop=True)
    return data_ZDS

def final_data_Hem(data):
    colNameNew = ['Test Code',
                  'White Blood Cells (WBC)',
                  'Red Blood Cells (RBC)',
                  'Measured Hemoglobin (mHGB)',
                  'Hematocrit (HCT)',
                  'Mean Cell Volume (MCV)',
                  'Mean Corpuscular hemoglobin (CHg)',
                  'Mean Cell Hemoglobin Concentration (MCHC)',
                  'Platelet Count (PLT) (Optical)',
                  'Red Cell Distr. Width (RDW)',
                  'Mean Platelet Volume (MPV)',
                  'Nucleated Red Blood Cell Count',
                  'Nucleated Red Blood Cell Percentage',
                  'Neurophil Cell Count',
                  'Lymphocyte Cell Count',
                  'Monocyte Cell Count',
                  'Eosinophil Cell Count',
                  'Basophil Cell Count',
                  'Neurophils (NEUT)',
                  'Lymphocytes (LYM)',
                  'Monocytes (MONO)',
                  'Eosinophils (EOS)',
                  'Basophils (BASO)',
                  'Percent Retic',
                  'Reticulocytes (Retic)',
                  'Percent Immature Retic',
                  'Percent Low Fluorescing Retic',
                  'Percent Middle Fluorescing Retic',
                  'Percent High Fluorescing Retic',
                  'Retic Hemoglobin (CHr)',
                  'Platelet Count (Impedance)',
                  'Platelet Count (Optical)',
                  'Experimenter ID',
                  'Date and Time of Blood Collection',
                  'Date of Measurement',
                  'Analyst ID',
                  'Comments',
                  'Date and time of sacrifice']
    #Selecting final table of columns for LIMS
    data['Date and time of sacrifice'] = data['Date-Time Sacrifice/Collection']
    data.columns = colNameNew
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final


def calculations_HB(data):
    values = dict(data.groupby('Test Code')['HOLE NAME'].apply(list))
    poke_counts = {}
    for key in values:
        test = {x:values[key].count(x) for x in values[key]}
        poke_counts.update({key:test})
    
    counts = pd.DataFrame.from_dict(poke_counts, orient='index')
    counts = counts.reindex(sorted(counts.columns), axis=1)
    counts.columns = ['Total Hole Pokes Hole ' + str(col) for col in counts.columns]
    data = pd.merge(data, counts, left_on = ['Test Code'], right_index=True)
    
    data['HOLE NAME'] = data['HOLE NAME'].astype(str)
    sequence = data.groupby(['Test Code'])['HOLE NAME'].apply('-'.join).reset_index()
    sequence = sequence.rename(columns={'HOLE NAME':'Hole Poke Sequence'})
    data = pd.merge(data, sequence, on=['Test Code'])
    data = data.drop(columns = ['HOLE NAME']).drop_duplicates().reset_index(drop=True)
    data.fillna(0, inplace=True)


    return data