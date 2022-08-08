# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 14:52:32 2020

@author: rushn
"""

import pandas as pd
import sys
import shutil
import transform_functions as tf
from os import listdir
from os.path import isfile, join
import time

#importPath = 'C:/Users/rushn/Documents/Projects/transforms/Hematology/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/Hematology/loaded/'


def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    for i in fileList:
        data_original = tf.readxlsx(importPath = str(importPath+i), skiprows=1)
		# CLIMB requires these two columns but we do not use them.
        data_original['Animal'] = ''
        data_original['Sample'] = ''
		
        colNameOld = [ 'Sample No.',
                      'WBC(10^3/uL)',
                      'RBC(10^6/uL)',
                      'HGB(g/dL)',
                      'HCT(%)',
                      'MCV(fL)',
                      'MCH(pg)',
                      'MCHC(g/dL)',
                      'PLT(10^3/uL)',
                      'RDW-CV(%)',
                      'MPV(fL)',
                      'NRBC#(10^3/uL)',
                      'NRBC%(%)',
                      'NEUT#(10^3/uL)',
                      'LYMPH#(10^3/uL)',
                      'MONO#(10^3/uL)',
                      'EO#(10^3/uL)',
                      'BASO#(10^3/uL)',
                      'NEUT%(%)',
                      'LYMPH%(%)',
                      'MONO%(%)',
                      'EO%(%)',
                      'BASO%(%)',
                      'RET%(%)',
                      'RET#(10^9/L)',
                      'IRF(%)',
                      'LFR(%)',
                      'MFR(%)',
                      'HFR(%)',
                      'RET-He(pg)',
                      'PLT-O(10^3/uL)',
		              'Experimenter',
                      'Date-Time Sacrifice/Collection',
					  'Animal',
					  'Sample']
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        
		
        data_final = tf.final_data_Hem(data)
        data_final = data_final.round(2)
        data_final.to_csv(loadedPath+'LOADED_'+i.split('.')[0] + '.csv.', index=False)
        
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