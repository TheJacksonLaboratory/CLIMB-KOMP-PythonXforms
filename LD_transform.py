# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 14:31:10 2020

@author: rushn
This script takes an Light/Dark data file and transforms the file to allow loading into JaxLIMS
"""

import pandas.api.types as ptypes
import sys
import shutil
import transform_functions as tf
from os import listdir
from os.path import isfile, join
import time


#importPath = 'C:/Users/rushn/Documents/Projects/transforms/LightDark/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/LightDark/loaded/'
#importPath = '//jax.org/jax/phenotype/LightDarkv2/KOMP/transform/'
#loadedPath = '//jax.org/jax/phenotype/LightDarkv2/KOMP/processed/'

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    for i in fileList:
        #Grabbing the 3 fields from the header rows
        dataHead = tf.readcsv(importPath = str(importPath+i), skiprows=124, nrow=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])
        
        data_original = tf.readcsv(importPath = importPath + i, skiprows=130)
        
        colNameOld = ['CAGE',
                      'SUBJECT ID',
                      'START TIME',
                      'DURATION (s)',
                      'ZONE',
                      'DURATION (CENTROID)',
                      'ENTRY COUNT (CENTROID)',
                      'LATENCY (CENTROID)',
                      'AMBULATORY TIME (CENTROID)']
        
        #Ensuring that all of hte above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Collected Date'] = date
        data['Collected By'] = user
        data['Comments'] = comment
        data['Animal'] = ''
        data['Sample'] = ''
        
        to_rename = { 'SUBJECT ID':'Key'}
        
        data = data.rename(columns=to_rename)
        
        ##################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_LD(data)
        #####################################################################
        data_final = tf.final_data_LD(data)
        
        #Testing columns for proper data type
        string_cols = ['Collected Date', 
                       'Collected By', 
					   'Animal',
					   'Sample',
                       'Comments']
        
        int_cols    = ['Side Changes']
        
        float_cols = ['Reaction Time',
                      'Left Side Time Spent (s)',
                      'Left Side Mobile Time Spent (s)',
                      'Right Side Time Spent (s)',
                      'Right Side Mobile Time Spent (s)',
                      'Percent Time in Dark (%)',
                      'Percent Time in Light (%)']
        
        assert all(ptypes.is_string_dtype(data_final[col]) for col in string_cols)
        assert all(ptypes.is_integer_dtype(data_final[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_final[col]) for col in float_cols)

        data_final = data_final.round(2)
        data_final.to_csv(loadedPath+'LOADED_'+i, index=False)
        
        #moving processed file to archive folder
        shutil.move(importPath+i, importPath+'\\Archive')


def fileInDirectory(my_dir: str):
    onlyfiles = [f for f in listdir(my_dir) if isfile(join(my_dir, f))]
    return(onlyfiles)

#function comparing two lists

def listComparison(OriginalList: list, NewList: list):
    differencesList = [x for x in NewList if x not in OriginalList] #Note if files get deleted, this will not highlight them
    return(differencesList)

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