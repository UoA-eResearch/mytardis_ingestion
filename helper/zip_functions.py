from zipfile import ZipFile
import zipfile
import zlib
import os
from pathlib import Path

def get_all_file_paths(directory): 
  
    # initializing empty file paths list 
    file_paths = [] 

    root_dir = Path(directory)
    
    # crawling through directory and subdirectories 
    for root, directories, files in os.walk(directory): 
        for filename in files: 
            # join the two strings in order to form the full filepath. 
            abs_filepath = Path(os.path.join(root, filename))
            filepath = abs_filepath.relative_to(root_dir)
            file_paths.append(filepath) 
  
    # returning all file paths 
    return file_paths

def zip_directory(root_dir,
                  zip_file_name):
    file_paths = get_all_file_paths(root_dir)
    print(root_dir)
    cwd = os.getcwd()
    os.chdir(root_dir)
    print(file_paths)
    with ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
        for file_name in file_paths:
            if str(file_name) == str(zip_file_name):
                print('match')
                continue
            print(os.getcwd())
            zip_file.write(file_name)
    os.chdir(cwd)
