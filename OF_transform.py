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
from os import listdir
from os.path import isfile, join

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
        data['Collected Date'] = date
        data['Collected By'] = user
        data['Comment'] = comment
        data['SUBJECT ID'] = data['SUBJECT ID'].astype(int)
        data['CAGE'] = data['CAGE'].str.replace('Cage ','')
        data['CAGE'] = data['CAGE'].astype(int)
        data['Animal'] = ''
        
        to_rename = {'CAGE':'Arena ID',
                        'SUBJECT ID':'Key',
                        'DURATION (s)':'Sample'}

        data = data.rename(columns=to_rename)

        ####################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_CDO_OF(data)
        #####################################################################
        data_CDO = tf.data_CDO_OF(data)

        #Testing columns for proper data type
        string_cols = ['Collected Date', 
                       'Collected By',  
					   'Animal',
                       'Comment']

        int_cols    = ['Number of Rears Total',
                       'Number of Rears First Five Minutes',
                       'Number of Rears Second Five Minutes',
                       'Number of Rears Third Five Minutes',
                       'Number of Rears Fourth Five Minutes',
                       'Clockwise',
                       'Counter Clockwise']

        float_cols = ['Distance Traveled Total (cm)',
                      'Distance Traveled First Five Minutes (cm)',
                      'Distance Traveled Second Five Minutes (cm)',
                      'Distance Traveled Third Five Minutes (cm)',
                      'Distance Traveled Fourth Five Minutes (cm)',
                      'Whole Arena Average Speed (cm/s)',
                      'Whole Arena Resting Time (s)',
                      'Time Spent Mobile (s)',
                      'Vertical Time (s)']
        
        assert all(ptypes.is_string_dtype(data_CDO[col]) for col in string_cols)
        assert all(ptypes.is_integer_dtype(data_CDO[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_CDO[col]) for col in float_cols)
        assert all(data_CDO['Sample']==300)        
        assert all(data_CDO['Whole Arena Permanence Time (s)']==1200)        

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
        data['Collected Date'] = date
        data['Collected By'] = user
        data['Comments'] = comment
        
        to_rename = {'CAGE':'Arena ID',
                        'SUBJECT ID':'Key',
                        'DURATION (s)':'Sample'}
        
        data = data.rename(columns=to_rename)
        
        ####################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_ZDS_OF(data)
        #####################################################################
        #Selecting final table of columns for LIMS
        data_ZDS = tf.data_ZDS_OF(data)
        #Testing columns for proper data type
        
        int_cols    = ['Number of Center Entries']
        
        float_cols = ['Periphery Distance Traveled (cm)',
                      'Periphery Resting Time (s)',
                      'Periphery Permanence Time (s)',
                      'Periphery Average Speed (cm/s)',
                      'Center Distance Traveled (cm)',
                      'Center Resting Time (s)',
                      'Center Permanence Time (s)',
                      'Center Average Speed (cm/s)',
                      'Percent Time Center (%)',
                      'Percent Time Center Habituation Ratio (%)',
                      'Percent Time Corners (%)',
                      'Percent Time Corners Habituation Ratio (%)',
                      'Corners Permanence Time (s)',
                      'Percent Time Center Slope (%)',
                      'Percent Time Corners Slope (%)']
        
        assert all(ptypes.is_integer_dtype(data_ZDS[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_ZDS[col]) for col in float_cols)

        data_all = pd.merge(data_CDO, data_ZDS, on = 'Key')
        data_all = data_all.drop_duplicates()
        data_all = data_all.round(2)
        data_all.to_csv(loadedPath+'LOADED_'+ ZoneDataStandard.split('_')[0] + '.csv', index=False)
        
        #moving processed file to archive folder
        shutil.move(importPath+ComprehensiveDataOutput, importPath+'\\Archive')
        shutil.move(importPath+ZoneDataStandard, importPath+'\\Archive')


def fileInDirectory(my_dir: str):
    onlyfiles = [f for f in listdir(my_dir) if isfile(join(my_dir, f))]
    return(onlyfiles)

#function comparing two lists

def listComparison(OriginalList: list, NewList: list):
    differencesList = [x for x in NewList if x not in OriginalList] #Note if files get deleted, this will not highlight them
    return(differencesList)

import time

def fileWatcher(src_dir: str, pollTime: int, dest_dir: str):

    while True:
        if 'watching' not in locals(): #Check if this is the first time the function has run
            previousFileList = fileInDirectory(src_dir)
            watching = 1
            print('First Time')
            print(previousFileList)
        
        time.sleep(pollTime)
		
        newFileList = fileInDirectory(src_dir)
        
        fileDiff = listComparison(previousFileList, newFileList)
        
        previousFileList = newFileList    
        if len(fileDiff) == 0: continue
        else:
            print("Processing files from " + src_dir + '\r')
            transform(src_dir, dest_dir)
		
if __name__ == '__main__':
    src = str(sys.argv[1])
    dest = str(sys.argv[2])
    fileWatcher(src,5,dest)