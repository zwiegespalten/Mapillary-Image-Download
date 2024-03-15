import os
import pandas as pd
from metadata_download import get_metadata
import asyncio
import numpy as np

continent = 'NA'
#dir = os.getcwd()
dir = '/mnt/sds-hd/sd17f001/eren/metadata_manipulation_new/north_america'

mly_key = "MLY|6700072406727486|6534c2cab766ff1eaa4c4defeb21a3f4" 
times = 5
call_limit_images = 10
call_limit = 5
base_limit = 1
max_cpu_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', os.cpu_count()))
max_workers = 3*2*max_cpu_workers
async_true = True

prototype = "missing_sequences_metadata_unfiltered"
file_unfiltered_metadata = f'{dir}/metadata_unfiltered_{continent}_missing_sequences.csv'
missing_sequences = f'{dir}/missing_sequences_metadata_unfiltered_combined.csv'

async def main(limit=5):
    aggregated_unfiltered_dfs = []
    counter = 0
    sequences_length = 0
    
    while True:
        if counter == limit:
            break
        
        sequences = None
        if os.path.exists(missing_sequences):
            sequences = pd.read_csv(missing_sequences)["sequence"].to_numpy()
        else:
            list_of_files = os.listdir(dir)
            files = [pd.read_csv(f"{dir}/{file}") for file in list_of_files if file.startswith(prototype)]
            files = pd.concat(files,axis=0,ignore_index=True)
            sequences = files["sequence"].to_numpy()
            
        task = [get_metadata(sequences,mly_key,call_limit_images,call_limit,base_limit,max_workers,async_true)]
        task = await asyncio.gather(*task)
        task = task[0]
        
        unfiltered_dfs, sequence_info = task
        aggregated_unfiltered_dfs.extend(unfiltered_dfs)
        
        if len(sequence_info):
            sequence_info = np.unique(np.array(sequence_info))
            sequence_info = pd.DataFrame({"sequence":sequence_info})
            sequence_info.to_csv(missing_sequences,index=False)
        
        if len(sequence_info) == sequences_length:
            counter += 1
        else:
            counter = 0
            sequences_length = len(sequence_info)
            
    if len(aggregated_unfiltered_dfs):
        
        if not isinstance(aggregated_unfiltered_dfs,pd.DataFrame):
            aggregated_unfiltered_dfs = pd.concat(aggregated_unfiltered_dfs)
            
        if os.path.exists(file_unfiltered_metadata):
            file_to_be_concatenated = pd.read_csv(file_unfiltered_metadata)
            aggregated_unfiltered_dfs = pd.concat([file_to_be_concatenated,aggregated_unfiltered_dfs],axis=0,ignore_index=True)

        aggregated_unfiltered_dfs.reset_index(drop=True,inplace=True)
        aggregated_unfiltered_dfs.to_csv(file_unfiltered_metadata,index=False)
        del aggregated_unfiltered_dfs
        
        
    
asyncio.run(main(times))

