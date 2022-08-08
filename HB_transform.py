# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:45:44 2020

@author: rushn
"""

#import os
#os.chdir('C:/Users/rushn/Documents/Projects/transforms/data_transforms/src')

import pandas as pd
import sys
import shutil
import transform_functions as tf
import itertools

from os import listdir
from os.path import isfile, join
import time

#importPath = 'C:/Users/rushn/Documents/Projects/transforms/Holeboard/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/Holeboard/loaded/'

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    fileGroups = [list(v) for k,v in itertools.groupby(fileList,key=tf.get_field_sub)]
    for i in fileGroups:
        ComprehensiveDataOutput = [x for x in i if "Comprehensive" in x][0]
        HolePokeStandard = [x for x in i if "HolePoke" in x][0]

        #Grabbing the 3 fields from the header rows
#        dataHead = pd.read_csv(str(importPath + ComprehensiveDataOutput), skiprows=42, nrows=2)
        dataHead = tf.readcsv(importPath = str(importPath + ComprehensiveDataOutput), skiprows=49, nrow=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])
        
        data_original = tf.readcsv(importPath = importPath + ComprehensiveDataOutput, skiprows=55)
        
        colNameOld = [ 
                      'SUBJECT ID',
                      'HOLE POKE TOTAL COUNT']
        
        #Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Collected Date'] = date
        data['Collected By'] = user
        
        to_rename = {
		             'HOLE POKE TOTAL COUNT':'Total Hole Pokes',
                     'SUBJECT ID':'Key'}
        
        data = data.rename(columns=to_rename)

        #####################################################################
        data_CDO = data        
        
        dataHead = tf.readcsv(importPath = str(importPath + HolePokeStandard), skiprows=26, nrow=2)

        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        #comment = str(dataHead['COMMENT'].iloc[0])
        
#        data_original = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=130)
        data_original = tf.readcsv(importPath = importPath + HolePokeStandard, skiprows=32)

        
        colNameOld = ['SUBJECT ID',
                      'HOLE NAME']
        
        #Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)
        
        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Collected Date'] = date
        data['Collected By'] = user
        data['Animal'] = ''
        data['Sample'] = ''
        
        to_rename = {'SUBJECT ID':'Key'}
        
        data = data.rename(columns=to_rename)
        data = data.dropna(subset=['Key'])
        data = data.astype({"Key": int, "HOLE NAME": int})
        
        ####################################################################
        #Calculations to create required columns for LIMS
        data_HPS = tf.calculations_HB(data)
        #####################################################################
        data_all = pd.merge(data_CDO, data_HPS, on = ['Key', 'Collected Date',
                                                      'Collected By'])  # Comments removed
        #data_all['Arena ID'] = data_all['Arena ID'].str.extract('(\d+)')
        data_all = data_all.drop_duplicates()
        data_all = data_all.round(2)
        data_all.to_csv(loadedPath+'LOADED_'+ HolePokeStandard.split('_')[0] + '.csv', index=False,
                        float_format='%.0f')
        
        #moving processed file to archive folder
        shutil.move(importPath+ComprehensiveDataOutput, importPath+'\\Archive')
        shutil.move(importPath+HolePokeStandard, importPath+'\\Archive')


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
            print('Previous:')
            print(previousFileList)
        
        time.sleep(pollTime)
		
        newFileList = fileInDirectory(src_dir)
        print('New:')
        print(newFileList)
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