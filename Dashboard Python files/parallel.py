# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 13:48:47 2020

@author: tmazaherikou
"""

files = ['UC1A_Attach.py', 'UC1A_Detach.py', 'UC1B_Attach.py','UC1B_Detach.py','UC2A.py','UC2B.py','UC3.py']

def execute_files(file):
    output()


if __name__ == '__main__':
    from multiprocessing import Pool
    from pathlib import Path
    import sys
    import os
    sys.path.append(r"C:\Users\tmazaherikou\Documents\Python-3 UCs-abc")
    from UC1A_Attach import *
    from UC1B_Attach import *
    from UC1A_Detach import *
    from UC1A_Detach import *
    from UC2A import *
    from UC2B import *
    from UC3 import *
    
    
    
    import concurrent.futures
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(execute_files, files)
        print(results)