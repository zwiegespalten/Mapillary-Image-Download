import math
import os
import time
from asyncio.futures import _asyncio
import numpy as np
import pandas as pd
import geopandas as gpd
from metadata_processing import process_one_sequence,async_process_one_sequence, get_response, async_get_response
import cProfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
import aiohttp


#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################
# These functions divide a given bbox into smaller bboxes. "divide_bbox" into 2. "segmented_bboxes" for more
"""------------------------------Division of bboxes-------------------------------------------------------"""
def segmented_bboxes(boundary_box, n):
    """
    Divide a boundary box into smaller boxes.
    Parameters:
    - boundary_box (list): Bounding box represented by [west, south, east, north].
    - n (int): Number of divisions along both the x-axis and y-axis.
    Returns:
    - list of lists: List containing smaller boxes represented by [west, south, east, north].
    """
    west, south, east, north = boundary_box
    
    # Divide into ceil(sqrt(n)) times ceil(sqrt(n)) smaller boundary boxes
    num_rows_cols = math.ceil(math.sqrt(n))
    
    boxes = []
    for i in range(num_rows_cols):
        for j in range(num_rows_cols):
            sub_box_west = west + i * (east - west) / num_rows_cols
            sub_box_east = west + (i + 1) * (east - west) / num_rows_cols
            sub_box_south = south + j * (north - south) / num_rows_cols
            sub_box_north = south + (j + 1) * (north - south) / num_rows_cols
            
            sub_box = [sub_box_west, sub_box_south, sub_box_east, sub_box_north]
            boxes.append(sub_box)
    return boxes

def divide_bbox(bbox):
    """
    Divide a bounding box into two equal-sized sub-bounding boxes along the latitude axis.
    Parameters:
    - bbox (list): Bounding box defined as [west, south, east, north].
    Returns:
    - List containing two lists representing the sub-bounding boxes.
    """
    west, south, east, north = bbox
    latitude_midpoint = (south + north) / 2  # Calculate the midpoint latitude

    # Create two sub-bounding boxes
    bbox1 = [west, south, east, latitude_midpoint]
    bbox2 = [west, latitude_midpoint, east, north]

    return [bbox1, bbox2]


#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################
# These functions query a given bbox and return the sequenceIDs therein.They keep dividing the bbox
# calling themselves till less than 2000 elements are returned

# "async_get_bboxes_and_sequences" does this using an asyncchronous event loop and is the most efficient sofar
# "get_bboxes_and_sequences" does this using for loops
# "get_bboxes_and_sequences_parallel" does this using an executor either a ProcessPoolExecutor or ThreadPoolExecutor

# "async_get_bboxes_and_sequences_higher_level" is meant to be used with "async_get_bboxes_and_sequences" and
# provides this with a "Session". You do not need this, if you provide a "session" in your code

# "async_get_bboxes_and_sequences_wrapping" as the name suggests, wraps "async_get_bboxes_and_sequences_higher_level"
# around. It runs the "async_get_bboxes_and_sequences_higher_level" asyncchronously and is meant to be used
# if "async_get_bboxes_and_sequences_higher_level" is used within a ProcessPoolExecutor or ThreadPoolExecutor,
# as "coroutines" cannot be passed to these. 
"""------------------------------Getting needed bboxes and unique sequences-------------------------------------------------------"""

async def async_get_bboxes_and_sequences(bbox, n, mly_key, bbox_set, sequence_set, call_limit=5, session=None,base_limit=1):
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    checker = 0 #checker for empty Data
    checker_ = 0 #checker for no Data in JSON
    checker__ = 0 #checker for None responses
    checker___ = 0 #checker for responses different than 200
    data = None
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&fields=sequence&bbox={bbox_str}"

    while True:
        #if checker__ == call_limit or checker_ == call_limit or checker == call_limit:
        if checker == call_limit:
            break
        
        response =  await async_get_response(url,session)
        if response is None:
            await asyncio.sleep(min(0.1*2**checker__,base_limit))
            checker__ += 1
            continue
        
        if response.status != 200:
            await asyncio.sleep(min(0.1*2**checker___,base_limit))
            continue
        
        json = await response.json()
        if 'data' not in json:
            await asyncio.sleep(min(0.1*2**checker_,base_limit))
            checker_ += 1
            continue
        
        if not len(json['data']):
            await asyncio.sleep(min(0.1*2**checker,base_limit))
            checker += 1
            continue
            
        else:
            data = json['data']
            break

    if data is None:
        bbox_set.add(tuple(bbox))
        return bbox_set, sequence_set
    
    if len(data) == 2000:
        sub_bboxes = segmented_bboxes(bbox, n)
        #sub_bboxes = divide_bbox(bbox)
        tasks = [async_get_bboxes_and_sequences(sub_bbox, n, mly_key, bbox_set, sequence_set, call_limit, session) for sub_bbox in sub_bboxes]
        tasks = await asyncio.gather(*tasks)
        tasks = [task for task in tasks if task is not None]
        for task in tasks:
            sequence_set = sequence_set.union(task[1])
            bbox_set = bbox_set.union(task[0])

    else:
        data = pd.DataFrame(data)
        if 'sequence' in data.columns:
            sequence_set = sequence_set.union(set(data['sequence'].unique()))
        del data

    return bbox_set, sequence_set

def get_bboxes_and_sequences(bbox,n,mly_key,bbox_list,sequence_list,call_limit=5,base_limit=1):
    """
    Retrieve bounding boxes and sequences from Mapillary API recursively 
    Args:
        bbox (list): Bounding box coordinates in the format [west, south, east, north].
        n (int): divides the bbox into n x n sub_bboxes in the case of getting 2000 (limit) API response
        mly_key (str): Mapillary access token.
        bbox_list (list): List to store bounding boxes.
        sequence_list (list): List to store sequences.
        call_limit (int, optional): Maximum number of API call attempts. Defaults to 5.
    Returns:
        tuple: A tuple containing updated lists for bounding boxes, sequences, and set of unique sequences.
    """
    bbox_str = str(f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}')
    checker = 0 #checker for empty Data
    checker_ = 0 #checker for no Data in JSON
    checker__ = 0 #checker for None responses
    checker___ = 0 #checker for responses different than 200
    data = None
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&fields=sequence&bbox={bbox_str}"
    
    while True:
         #------------------------checking the response is meaningful------------------------------------------------#
        if checker == call_limit:
            break
        
        response = get_response(url)
        
        if response is None:
            time.sleep(min(0.01*2**checker__,base_limit))
            checker__ +=1
            continue
        
        if response.status_code != 200:
            time.sleep(min(0.01*2**checker___,base_limit))
            checker___ +=1
            continue
        
        json = response.json()
        if 'data' not in json:
            time.sleep(min(0.01*2**checker_,base_limit))
            checker_ += 1
            continue
        
        if not len(json['data']):
            time.sleep(min(0.01*2**checker,base_limit))
            checker += 1 
        else:    
            data = json['data']
            break
        
    if data is None:
        return  bbox_list, sequence_list
    # if data contains 2000 (API limit) elements, it divides the box into n x n sub_bboxes and calls itself
    if len(data) == 2000: 
        sub_bboxes = segmented_bboxes(bbox, n)
        #sub_bboxes = divide_bbox(bbox)
        for sub_bbox in sub_bboxes:
            bbox_list,sequence_list = get_bboxes_and_sequences(sub_bbox,n,mly_key,[],[],call_limit,base_limit)
    # if it has found a box, that returns less than 2000 elements, it updates the bbox_list as well the extracts
    # sequences and updates the "set_" with unique sequences
    else:
        #bbox_list = bbox_list.union(set(bbox)) 
        data = pd.DataFrame(data)
        if 'sequence' in data.columns:
            sequence_list = sequence_list.union(set(data['sequence'].unique()))    
        del data
            
    return  bbox_list,sequence_list 

def get_bboxes_and_sequences_parallel(bbox,n,mly_key,bbox_list,sequence_list,executor,call_limit=5,base_limit=1):
    """
    Retrieve bounding boxes and sequences from Mapillary API recursively.

    Args:
        bbox (list): Bounding box coordinates in the format [west, south, east, north].
        n (int): divides the bbox into n x n sub_bboxes in the case of getting 2000 (limit) API response
        mly_key (str): Mapillary access token.
        bbox_list (list): List to store bounding boxes.
        sequence_list (list): List to store sequences.
        call_limit (int, optional): Maximum number of API call attempts. Defaults to 5.
        max_workers (int, optional): Maximum number of worker threads for concurrent execution. Defaults to 16.

    Returns:
        tuple: A tuple containing updated lists for bounding boxes, sequences, and set of unique sequences.
    """
    bbox_str = str(f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}')
    checker = 0 #checker for empty Data
    checker_ = 0 #checker for no Data in JSON
    checker__ = 0 #checker for None responses
    checker___ = 0 #checker for responses different than 200
    data = None
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&fields=sequence&bbox={bbox_str}"
    
    while True:
         #------------------------checking the response is meaningful------------------------------------------------#
        if checker == call_limit:
            break
        
        response = get_response(url)
        
        if response is None:
            time.sleep(min(0.01*2**checker__,base_limit))
            checker__ +=1
            continue
        
        if response.status_code != 200:
            time.sleep(min(0.01*2**checker___,base_limit))
            checker___ +=1
            continue
        
        json = response.json()
        if 'data' not in json:
            time.sleep(min(0.01*2**checker_,base_limit))
            checker_ += 1
            continue
        
        if not len(json['data']):
            time.sleep(min(0.01*2**checker,base_limit))
            checker += 1 
        else:    
            data = json['data']
            break
        
    if data is None:
        return bbox_list, sequence_list
    
    # If data contains 2000 (API limit) elements, divide the box into n x n sub_bboxes and call itself
    sub_bboxes = []
    if len(data) == 2000: 
        #sub_bboxes = segmented_bboxes(bbox, n)
        sub_bboxes = divide_bbox(bbox)
 
        futures = [executor.submit(get_bboxes_and_sequences_parallel,
                                                sub_bbox,
                                                n,
                                                mly_key,
                                                set(),
                                                set(),
                                                executor,
                                                call_limit,
                                                base_limit) for sub_bbox in sub_bboxes]
        
        futures = [future for future in futures if future is not None]
        for future in futures:
            sub_bbox_list, sub_sequence_list = future.result()
            
            #bbox_list.extend(sub_bbox_list)
            sequence_list = sequence_list.union(sub_sequence_list)
    
    # If it has found a box that returns less than 2000 elements, update the bbox_list, extract sequences, 
    # and update the "set_" with unique sequences
    else:
        data = pd.DataFrame(data)
        if 'sequence' in data.columns:
            sequence_list = sequence_list.union(set(data['sequence'].unique()))    
        del data
        
    # Returns the set of bboxes "bbox_list", set of sequences "sequence_list"             
    return bbox_list, sequence_list

####################Wrap-Up Code for get_bboxes_and_sequences_parallel if used in ProcessPoolExecutor ##############################################
def async_get_bboxes_and_sequences_wrapping(bbox,n,mly_key,call_limit=5,base_limit=1):
    return asyncio.run(async_get_bboxes_and_sequences_higher_level(bbox,n,mly_key,call_limit,base_limit))

async def async_get_bboxes_and_sequences_higher_level(bbox,n,mly_key,call_limit=5,base_limit=1):
    
    bbox_set = set()
    sequence_set = set()
    async with aiohttp.ClientSession() as session:
        tasks = [async_get_bboxes_and_sequences(bbox, n, mly_key, bbox_set, sequence_set, call_limit, session,base_limit)]
        tasks = await asyncio.gather(*tasks)
        for task in tasks:
            if task[0] is not None:
                bbox_set = bbox_set.union(task[0])
            if task[1] is not None:
                sequence_set = sequence_set.union(task[1])

    return bbox_set,sequence_set

#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################
# These functions retrieve the metadata of images associated with sequenceIDs. I used to have a function
# "metadata_mapillary", hence the name "metadata_mapillary_parallelized" which used forloops to iterate over
# sequenceIDs but it wasn't efficient. You can implement this but don't do it.

# "metadata_mapillary_parallelized" downloads the metadata of images, provided with a list of sequenceIDs
# using asynchronous download

# "metadata_mapillary_parallelized_wrapping" wraps around the latter to be used by a ProcessPoolExecutor or
# a ThreadPoolExecutor since 'coroutines' cannot be passed to these.
"""------------------------------Getting metadata-------------------------------------------------------"""
def metadata_mapillary_parallelized_wrapping(mly_key, sequences, call_limit_images=10, call_limit=5,base_limit=1):
    return asyncio.run(metadata_mapillary_parallelized(mly_key, sequences, call_limit_images=call_limit_images, call_limit=call_limit,base_limit=base_limit))

async def metadata_mapillary_parallelized(mly_key, sequences, call_limit_images=10, call_limit=5,base_limit=1):
    metadata = []
    sequence_info = []
    async with aiohttp.ClientSession() as session:
        tasks = [async_process_one_sequence(sequence,mly_key,session,call_limit_images,call_limit,base_limit=base_limit)
                    for sequence in sequences]
        results = await asyncio.gather(*tasks)
    
    results = [result for result in results if result is not None]
    for result in results:
        metadata.append(result[0])
        if result[1] is not None:
            sequence_info.append(result[1])
            
    del results
    return metadata,sequence_info


#############################################################################################################
#############################################################################################################
#############################################################################################################
#Well, we need to use these functions somehow. 

# 'missing_sequences_download':  Since life sucks and you cannot download stuff when you want to,
#  such sequenceIDs will be saved into the directory so that you can try to download them later using this function here.

# 'get_sequences': this mf here orchestrates the extraction of sequenceIDs provided a bbox. You can skip
# the asynchronous implementation and use a ThreadPoolExecutor or ProcessPoolExecutor which will then use 
# 'get_bboxes_and_sequence_parallel' in its stead. But don't. Unless you want to suffer, then do.
# I did not implement a strategy which uses 'get_bboxes_and_sequences' using forloops because nobody lives that long

# 'get_metadata' downloads the metadata associated with a list of sequenceIDs. You have the option to choose
# between the usage of only a ThreadPoolExecutor or a ThreadPoolExecutor combined with asynchronous download. Go for this,
# it is the default. Do not replace ThreadPoolExecutor with a ProcessPoolExecutor unless you want to suffer

# 'main' combines 'get_sequences' with 'get_metadata'
"""------------------------------Et Actio-------------------------------------------------------"""
async def missing_sequences_download(sequence_list,mly_key,file_unfiltered_metadata,missing_sequences,call_limit_images=10,call_limit=5,base_limit=1,max_workers=4,async_true=True):
    """
    Download missing sequences metadata and save them to CSV files.

    Args:
        sequence_list (list): List of Mapillary sequence IDs for which the download failed, they could be retrained from the CSV files
                              starting with "missing_sequence_".
        mly_key (str): Mapillary access token.
        file_unfiltered_metadata (str): Path to save unfiltered metadata CSV file.
        missing_sequences (str): CSV file to save still missing sequences
        call_limit_images (int, optional): Maximum number of API call attempts per sequence
                                        when the server returns 2000 (API limit) images in each turn. Defaults to 10.
        call_limit (int, optional): Maximum number of API call attempts per sequence when no data is returned. Defaults to 5.
    """
    task = [get_metadata(sequence_list,mly_key,call_limit_images,call_limit,base_limit,max_workers,async_true)]
    
    results = await asyncio.gather(*task)
    del task
    unfiltered_dfs = results[0][0]
    sequences_info = results[0][1]
    del results
    
    if len(unfiltered_dfs):
        if not isinstance(unfiltered_dfs,pd.DataFrame):
            unfiltered_dfs = pd.concat(unfiltered_dfs)
        if os.path.exists(file_unfiltered_metadata):
            file_to_be_concatenated = pd.read_csv(file_unfiltered_metadata)
            unfiltered_dfs = pd.concat([file_to_be_concatenated,unfiltered_dfs],axis=0,ignore_index=True)

        unfiltered_dfs.reset_index(drop=True,inplace=True)
        unfiltered_dfs.to_csv(file_unfiltered_metadata,index=False)
        unfiltered_dfs = None
    if len(sequences_info):
        sequences_info = np.unique(np.array(sequences_info))
        sequences_info = pd.DataFrame({"sequence":sequences_info})
        sequences_info.to_csv(missing_sequences,index=False)
        
async def get_sequences(bbox_,n,mly_key,async_true=True,max_workers=4,call_limit=5,base_limit=1):
        bboxes = segmented_bboxes(bbox_, max_workers)
        
        bbox_list = set()
        sequence_list = set()
        results = []
        #In case asyncronous query is wished
        if async_true == True:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(async_get_bboxes_and_sequences_wrapping,
                                            bboxes,
                                            [n]*len(bboxes),
                                            [mly_key]*len(bboxes),
                                            [call_limit]*len(bboxes),
                                            [base_limit]*len(bboxes)))
                results = [result for result in results if result is not None]
        #Otherwise ThreadPoolExecutor but asyncronous query combined with ThreadPoolExecutor is significantly faster
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(get_bboxes_and_sequences_parallel,
                                            bboxes,
                                            [n]*len(bboxes),
                                            [mly_key]*len(bboxes),
                                            [set() for _ in bboxes],
                                            [set() for _ in bboxes],
                                            [executor for _ in bboxes],
                                            [call_limit for _ in bboxes],
                                            [base_limit for _ in bboxes]))
            results = [result for result in results if result is not None]
        for result in results:
            bbox_list = bbox_list.union(result[0])
            sequence_list = sequence_list.union(result[1])
        
        del results
        sequence_list = list(sequence_list)
        bbox_list = list(bbox_list)
   
        return bbox_list,sequence_list

async def get_metadata(sequence_list,mly_key,call_limit_images=10,call_limit=5,base_limit=1,max_workers=4,async_true=True):
    unfiltered_dfs = []
    sequences_info = []
        
    results = []
    #In case asyncronous query is wished
    if async_true == True:
        sequence_list_chunks = np.array_split(sequence_list,max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            process_results = list(executor.map(metadata_mapillary_parallelized_wrapping,
                                                [mly_key]*len(sequence_list_chunks),
                                                sequence_list_chunks,
                                                [call_limit_images]*len(sequence_list_chunks),
                                                [call_limit]*len(sequence_list_chunks),
                                                [base_limit]*len(sequence_list_chunks)))
        for process_result in process_results:
            unfiltered_dfs.extend(process_result[0])
            sequences_info.extend(process_result[1])
                    
    #Otherwise ThreadPoolExecutor but asyncronous download combined with ThreadPoolExecutor is significantly faster
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_one_sequence,
                                        sequence_list,
                                        [mly_key]*len(sequence_list),
                                        sequence_list,
                                        [call_limit_images]*len(sequence_list),
                                        [call_limit]*len(sequence_list),
                                        [base_limit]*len(sequence_list)))       
        results = [result for result in results if result is not None]
            
        for result in results:
            if result is not None:
                unfiltered_dfs.append(result[0])
                if result[1] is not None:
                    sequences_info.append(result[1])
                            
    del results
    return unfiltered_dfs,sequences_info

async def main(seq,missing_bboxes,bbox_, mly_key, n, file_unfiltered_metadata, missing_sequences,
        call_limit_images=10, call_limit=5, max_workers=4,async_true=False,download_true=True,base_limit=1):
    """
    Main function to download the image metadata from Mapillary API using bboxes. 'bbox' will be divided into
    "ceil(math.sqrt(number_of_initial_sub_bboxes)) * ceil(math.sqrt(number_of_initial_sub_bboxes)) " bboxes first. 
    The Mapillary API will be called on each of these cells and provided that they return 2000 (server limit) elements,
    they will further be divided into "ceil(nath.square(n)) * ceil(nath.square(n))", till they return less than 2000 elements
    to ensure that no sequences are overseen. During division unique sequence IDs will be saved to call the Mapillary API on them
    later to get the metadata of the images within them. If the metadata of the images of some sequences cannot be extracted, their
    sequence IDs will be saved to "missing_sequences" to try to download them later.
    
    Args:
    bbox (list): Bounding box coordinates in the format [west, south, east, north]. 
    mly_key (str): Mapillary access token.
    number_of_initial_sub_bboxes (int): Number of subdivisions for segmentation.
    n (int): Maximum number of images per bounding box.
    file_unfiltered_metadata (str): Path to save unfiltered metadata CSV file.
    missing_sequences (str): Path to save missing sequences CSV file.
    call_limit_images (int, optional): Maximum number of API call attempts per sequence
                                    when the server returns 2000 (API limit) images in each turn. Defaults to 10.
    call_limit (int, optional): Maximum number of API call attempts per sequence when no data is returned. Defaults to 5.
    """
        
    "---------------------------Getting the Sequences-----------------------------------------------------"
    t1 = time.time()
    sequence_list = []
    bbox_list = []
    bbox_list,sequence_list = await get_sequences(bbox_,n,mly_key,async_true,max_workers,call_limit,base_limit)

    print(f"number of sequences: {len(sequence_list)}")
    if len(sequence_list):
        sequences = pd.DataFrame({'sequences':sequence_list})
        sequences.to_csv(seq,index=False)
    #if len(bbox_list):
    #    bbox_list = pd.DataFrame({'bboxes':bbox_list})
    #    bbox_list.to_csv(missing_bboxes,index=False)
    t2 = time.time()
    print(f'it took {t2-t1} seconds to query')
    
    "---------------------------Getting the Image Metadata-----------------------------------------------------"
    unfiltered_dfs = []
    sequences_info = []
        
    if download_true == True:
        unfiltered_dfs,sequences_info = await get_metadata(sequence_list,mly_key,call_limit_images,call_limit,base_limit,max_workers,async_true)
       
        if len(unfiltered_dfs):
            unfiltered_dfs = pd.concat(unfiltered_dfs)
            unfiltered_dfs.reset_index(drop=True,inplace=True)
            unfiltered_dfs.to_csv(file_unfiltered_metadata,index=False)
            unfiltered_dfs = None
        if len(sequences_info):
            sequences_info = pd.DataFrame({"sequence":sequences_info})
            sequences_info.to_csv(missing_sequences,index=False)
            
        t3 = time.time()
        print(f'it took {t3-t2} seconds to download')
    

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################
########################################################################################################################################
########################################################################################################################################
########################################################################################################################################        
"""----------------------------------------------------------------------------------Parameters--------------------------------------"""
max_cpu_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', os.cpu_count()))
max_workers = 3*2*max_cpu_workers

dir = os.getcwd()
dir = '/mnt/sds-hd/sd17f001/eren/metadata_manipulation_new/europe'

continent = 'EU'
grid_coordinates = 'europe.csv'

mly_key = "MLY|6700072406727486|6534c2cab766ff1eaa4c4defeb21a3f4"
mly_key = 'MLY|6704446686232389|295d77211da2365c42ea656e7ab032c6'
mly_key = 'MLY|7160741587366125|36727dbc605d960f172712bcbcaa3010'
mly_key = 'MLY|24934087266239487|283a94b3c68c0c95f486cf4c11a229c2'
mly_key = 'MLY|25723892110543686|a2cf5b92cf0b1126075958cc00159db6'
mly_key = 'MLY|24901690569476887|16afd3356dd8d13bb2714075d42da6dd'

async_true = True
download_true = True
call_limit_images=10
call_limit=5
base_limit = 1
n = 2

if __name__ == '__main__':
    grid = 4
    grid = int(str(__file__).split('/')[-1].split('.')[0]) #the grid number will be read from the file name
    
    bbox = [49.392653,49.420689,8.657570,8.707523]
    bbox = [-50.402527,-50.210266,12.425848,12.626938]
    bbox = pd.read_csv(grid_coordinates).iloc[grid]
    
    south, north, west, east = bbox
    bbox = [west, south, east, north]

    file_unfiltered_metadata = f'{dir}/metadata_unfiltered_{continent}_{grid}.csv'
    missing_sequences = f'{dir}/missing_sequences_metadata_unfiltered_{continent}_{grid}.csv'
    seq = f'{dir}/sequences_{continent}_{grid}.csv'
    missing_bboxes = f'{dir}/missing_bboxes_{continent}_{grid}.csv'

    #cProfile.run("main(seq,missing_bboxes,bbox,mly_key,n,file_unfiltered_metadata,missing_sequences,call_limit_images,call_limit,max_workers,async_true,download_true,base_limit)")
    thread_numbers = np.arange(1,33)*2
    thread_numbers = [16]
    time_table = []
    std_table = []
    for thread_number in thread_numbers:
        times = []
        for i in range(1):
            t = time.time()
            asyncio.run(main(seq,missing_bboxes,bbox,mly_key,n,file_unfiltered_metadata,missing_sequences,call_limit_images,call_limit,max_workers,
                         async_true,download_true,base_limit))
            tt = time.time()
            times.append(tt - t)
        mean = np.mean(times)
        std = np.std(times)
        time_table.append(mean)
        std_table.append(std)
    
    #data = pd.DataFrame({'thread_number':thread_numbers,'mean_time':time_table,'std':std_table})
    #print(data)
    #print(data[data['mean_time'] == min(data['mean_time'])])
    #data.to_csv("thread_number.csv")
       