'''Cleanup files & directories under /tmp'''
import os
import shutil
import glob

dir_root = '/tmp/'

def cleanup_work_dir(dir_root=dir_root):
    '''Cleanup directories & files under the root directory
    Defaults to /tmp/ which is default working directory'''
    listing = glob.glob(dir_root+'*')
    for l in listing:
        if os.path.isdir(l):
            try:
                shutil.rmtree(l)
            except:
                pass
        else:
            try:
                os.remove(l)
            except:
                pass
