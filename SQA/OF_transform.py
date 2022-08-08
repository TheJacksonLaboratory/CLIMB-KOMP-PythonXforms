# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 13:59:00 2020

@author: rushn
"""

#import os
#os.chdir('C:/Users/rushn/Documents/Projects/transforms/data_transforms/src')

import pandas as pd
import pandas.api.types as ptypes
import sys
import shutil
import transform_functions as tf
import itertools

#importPath = 'C:/Users/rushn/Documents/Projects/transforms/OpenField/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/OpenField/loaded/'
#importPath = '//jax.org/jax/phenotype/OpenFieldv2/KOMP/transform/'
#loadedPath = '//jax.org/jax/phenotype/OpenFieldv2/KOMP/processed/'

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    fileGroups = [list(v) for k,v in itertools.groupby(fileList,key=tf.get_field_sub)]
    for i in fileGroups:
        ComprehensiveDataOutput = [x for x in i if "Comprehensive" in x][0]
        ZoneDataStandard = [x for x in i if "ZoneData" in x][0]

        #Grabbing the 3 fields from the header rows
#        dataHead = pd.read_csv(str(importPath + ComprehensiveDataOutput), skiprows=42, nrows=2)
        dataHead = tf.readcsv(importPath = str(importPath + ComprehensiveDataOutput), skiprows=42, nrow=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])
        
        data_original = tf.readcsv(importPath = importPath + ComprehensiveDataOutput, skiprows=48)
        
        colNameOld = ['CAGE',
                      'SUBJECT ID',
                      'SAMPLE',
                      'START TIME',
                      'DURATION (s)',
                      'TOTAL DISTANCE (cm)',
                      'REST TIME (s)',
                      'AMBULATORY TIME (s)',
                      'VERTICAL EPISODE COUNT',
                      'VERTICAL TIME (s)',
                      'CLOCKWISE REVOLUTIONS',
                      'COUNTER-CLOCKWISE REVOLUTIONS']
        
        #Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Date of test'] = date
        data['Experimenter ID'] = user
        data['Comments'] = comment
        data['SUBJECT ID'] = data['SUBJECT ID'].astype(int)
        data['CAGE'] = data['CAGE'].str.replace('Cage ','')
        data['CAGE'] = data['CAGE'].astype(int)
        
        to_rename = {'CAGE':'Arena ID',
                        'SUBJECT ID':'Test Code',
                        'DURATION (s)':'Sample Duration'}

        data = data.rename(columns=to_rename)

        ####################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_CDO_OF(data)
        #####################################################################
        data_CDO = tf.data_CDO_OF(data)

        #Testing columns for proper data type
        string_cols = ['Date of test', 
                       'Experimenter ID',  
                       'Start time', 
                       'Comments']

        int_cols    = ['Number of Rears Total',
                       'Number of Rears First Five Minutes',
                       'Number of Rears Second Five Minutes',
                       'Number of Rears Third Five Minutes',
                       'Number of Rears Fourth Five Minutes',
                       'Clockwise',
                       'Counter Clockwise',
                       'Arena ID']

        float_cols = ['Distance Traveled Total',
                      'Distance Traveled First 5 Min',
                      'Distance Traveled Second 5 Min',
                      'Distance Traveled Third 5 Min',
                      'Distance Traveled Fourth 5 Min',
                      'Whole Arena Average Speed',
                      'Whole Arena Resting Time',
                      'Time Spent Mobile',
                      'Vertical Time']
        
        assert all(ptypes.is_string_dtype(data_CDO[col]) for col in string_cols)
        assert all(ptypes.is_integer_dtype(data_CDO[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_CDO[col]) for col in float_cols)
        assert all(data_CDO['Sample Duration']==300)        
        assert all(data_CDO['Whole Arena Permanence Time']==1200)        

        ##################Processing ZoneDataStandard###########################################
        #Grabbing the 3 fields from the header rows
#        dataHead = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=124, nrows=2)
        dataHead = tf.readcsv(importPath = str(importPath + ZoneDataStandard), skiprows=124, nrow=2)

        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])
        
#        data_original = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=130)
        data_original = tf.readcsv(importPath = importPath + ZoneDataStandard, skiprows=130)

        
        colNameOld = ['SUBJECT ID',
                      'SAMPLE',
                      'ZONE',
                      'DURATION (CENTROID)',
                      'ENTRY COUNT (CENTROID)',
                      'TOTAL DISTANCE (CENTROID)',
                      'REST TIME (CENTROID)',
                      'AMBULATORY TIME (CENTROID)']
        
        #Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)
        
        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Date of test'] = date
        data['Experimenter ID'] = user
        data['Comments'] = comment
        
        to_rename = {'CAGE':'Arena ID',
                        'SUBJECT ID':'Test Code',
                        'DURATION (s)':'Sample Duration'}
        
        data = data.rename(columns=to_rename)
        
        ####################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_ZDS_OF(data)
        #####################################################################
        #Selecting final table of columns for LIMS
        data_ZDS = tf.data_ZDS_OF(data)
        #Testing columns for proper data type
        
        int_cols    = ['Number of Center Entries']
        
        float_cols = ['Periphery Distance Traveled',
                      'Periphery Resting Time',
                      'Periphery Permanence Time',
                      'Periphery Average Speed',
                      'Center Distance Traveled',
                      'Center Resting Time',
                      'Center Permanence Time',
                      'Center Average Speed',
                      'PctTime Center',
                      'PctTime Center Habituation Ratio',
                      'PctTime Corners',
                      'PctTime Corners Habituation Ratio',
                      'Corners Permanence Time',
                      'PctTime Center Slope',
                      'PctTime Corners Slope']
        
        assert all(ptypes.is_integer_dtype(data_ZDS[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_ZDS[col]) for col in float_cols)

        data_all = pd.merge(data_CDO, data_ZDS, on = 'Test Code')
        data_all = data_all.drop_duplicates()
        data_all = data_all.round(2)
        data_all.to_csv(loadedPath+'LOADED_'+ ZoneDataStandard.split('_')[0] + '.csv', index=False)
        
        #moving processed file to archive folder
        shutil.move(importPath+ComprehensiveDataOutput, importPath+'\\Archive')
        shutil.move(importPath+ZoneDataStandard, importPath+'\\Archive')

if __name__ == '__main__':
    a = str(sys.argv[1])
    b = str(sys.argv[2])
    transform(a, b)