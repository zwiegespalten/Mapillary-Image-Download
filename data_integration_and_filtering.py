

import os,glob,shutil
import numpy as np
import pandas as pd
import json
import geopandas as gpd
from shapely.geometry import Point,LineString
from shapely import wkt
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from spacing_calculation import calculate_spacing_vectorized
from metadata_processing import process_one_sequence


##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
#This block serves the purporse of reading the image metadata and writing them to smaller files
# 'read_file' is for reading the image metadata with pre-given data types
# 'write_file' is just to write a file to .csv
# 'writing_to_new_files' divides a DataFrame into 'number_of_files' and writes them into files
# 'unification_parallel' reads csv files using 'read_file' and ProcessPoolExecutor and concatenates them to create
# a DataFrame containing metadata from all files. However, it should be used with caution since it uses a great deal of
# Memory and can result in memory error if used with unfiltered raw metadata
"""--------------------------------------Reading and Writing Files---------------------------------------------------------"""
def read_file(file):
    dtype_dict = {
    'sequence': str,
    'id': str,
    'url': str,
    'long' :float,
    'lat' : float,
    'height' : int,
    'width' : int,
    'altitude' :float,
    'make' : str,
    'model' : str,
    'creator': str,
    'is_pano': bool,
    'timestamp':int
    }
    try:
        df = pd.read_csv(file,low_memory=False,dtype=dtype_dict)
        return df
    except Exception as err:
        return None
    
def write_file(df,file):
    try:
        df.to_csv(file,index=False)
    except:
        pass 

def writing_to_new_files(df, number_of_files, prototype, dir,max_workers=64):
    """
    Write a DataFrame to multiple CSV files concurrently.

    Args:
        df (pandas.DataFrame): DataFrame to be split and written to multiple CSV files.
        number_of_files (int): Number of CSV files to be created.
        prototype (str): Prototype for the filenames of the output CSV files.
        dir (str): Directory where the output CSV files will be saved.

    This function splits the input DataFrame into chunks and writes each chunk to a separate CSV file.
    It uses ThreadPoolExecutor to write chunks to files concurrently.
    """
    chunks = np.array_split(df, number_of_files)
    file_names = [f'{dir}/{prototype}_{i}.csv' for i in range(number_of_files)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk, file_name in zip(chunks, file_names):
            future = executor.submit(write_file, chunk, file_name)
            futures.append(future)
        
        for future, chunk in zip(futures, chunks):
            future.result()  # Ensure writing is completed
            del chunk  # Delete the chunk to release memory
            
    
def unification_parallel(dir, condition, col1="sequence",col2="id",max_workers=64):
    """
    Read and unify data from multiple CSV files in parallel, removing duplicates based on specified columns.

    Args:
        dir (str): Directory containing CSV files.
        condition (str): Filename condition for selecting files.
        col1 (str): Name of the first column for duplicate removal. Default should be the column with sequences.
        col2 (str): Name of the second column for duplicate removal. Default should be the column with image IDs

    Returns:
        pandas.DataFrame: DataFrame containing the unified data from all selected CSV files.

    This function searches for CSV files in the specified directory that match the given condition.
    It reads each file in parallel using ThreadPoolExecutor and read_file function.
    The resulting DataFrames are concatenated into a single DataFrame.
    Duplicate rows based on specified columns are removed, and the resulting DataFrame is returned.
    """
    files = glob.glob(f'{dir}/{condition}')
    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        dfs = [result for result in executor.map(read_file, files) if result is not None]

    if len(dfs):
        dfs = pd.concat(dfs, ignore_index=True)
        #dfs = dfs.drop_duplicates(subset=[col1,col2], keep='first',ignore_index=True).reset_index(drop=True)
        dfs = dfs.drop_duplicates(subset=[col2], keep='first',ignore_index=True).reset_index(drop=True)
    else:
        dfs = pd.DataFrame()
    return dfs
    
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
# This block has two functions and serves the purpose of intersecting a DataFrame with a Polygon

# 'intersect_chunk' is the actual function intersecting a DataFrame
# 'parallel_intersect' coordinates the 'intersect_chunk' by using a PoolProcessExecutor and dividing a given DataFrame
# into smaller chunks to speed up the process. Caution advised if the DataFrame is very large, then a memory error 
# could be thrown
"""--------------------------------------Intersection ---------------------------------------------------------"""
def intersect_chunk(chunk, intersecting):
    """
    This function performs a spatial join between 'chunk' and 'intersecting' GeoDataFrames using the 'within' operation.
    It splits the records of 'chunk' into two GeoDataFrames based on whether they intersect with 'intersecting' or not.

    Args:
        chunk (geopandas.GeoDataFrame): Chunk of GeoDataFrame for spatial intersection.
        intersecting (geopandas.GeoDataFrame): GeoDataFrame with which the intersection is performed.

    Returns:
        tuple: A tuple containing two GeoDataFrames:
            - The first GeoDataFrame contains the records of 'chunk' that intersect with 'intersecting'.
            - The second GeoDataFrame contains the records of 'chunk' that do not intersect with 'intersecting'.
    """
    within_intersection = gpd.sjoin(chunk, intersecting, how='inner', predicate='within')
    within_intersection = within_intersection[chunk.columns]
    outside_intersection = chunk[~chunk.index.isin(within_intersection.index)]
    outside_intersection = outside_intersection[chunk.columns]
    return within_intersection, outside_intersection

def parallel_intersect(to_be_intersected, intersecting, chunk_size=500000,max_workers=64):
    """
    This function performs parallel spatial intersection between 'to_be_intersected' and 'intersecting' GeoDataFrames.
    It splits 'to_be_intersected' into smaller chunks and processes them concurrently using a ProcessPoolExecutor.
    Each chunk is intersected with 'intersecting' GeoDataFrame using the 'intersect_chunk' function.
    The results are aggregated into two GeoDataFrames representing intersecting and non-intersecting records.

    Args:
        to_be_intersected (geopandas.GeoDataFrame): GeoDataFrame to be intersected.
        intersecting (geopandas.GeoDataFrame): GeoDataFrame with which the intersection is performed.
        chunk_size (int, optional): Size of chunks to be processed in parallel. Defaults to 500000.

    Returns:
        tuple: A tuple containing two GeoDataFrames:
            - The first GeoDataFrame contains the records of 'to_be_intersected' that intersect with 'intersecting'.
            - The second GeoDataFrame contains the records of 'to_be_intersected' that do not intersect with 'intersecting'.

    """
    results_within = []
    results_outside = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        chunks = np.array_split(to_be_intersected, np.ceil(len(to_be_intersected) / chunk_size))
        for chunk in chunks:
            futures.append(executor.submit(intersect_chunk, chunk, intersecting))
            
        for future in futures:
            within_intersection, outside_intersection = future.result()
            results_within.append(within_intersection)
            results_outside.append(outside_intersection)
        del futures
    
    if len(results_within):
        results_within = gpd.GeoDataFrame(pd.concat(results_within, ignore_index=True), geometry='geometry', crs=to_be_intersected.crs)
    else:
        results_within = pd.DataFrame()
    if len(results_outside):
        results_outside = gpd.GeoDataFrame(pd.concat(results_outside, ignore_index=True), geometry='geometry', crs=to_be_intersected.crs)
    else:
        results_outside = pd.DataFrame()
    return results_within, results_outside
    
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
#This block here applies the spacing to raw metadata
# 'chunk_spacing' is the function that groups and filters the metadata based on 'interval_length' by passing
# them to 'calculate_spacing_vectorized'. If 'line_string_true' is True, it will also create LineStrings from
# the filtered metadata

# 'parallel_spacing' coordinates the application of 'chunk_spacing' by dividing the metadata into chunks and running the
# code concurrently using ProcessPoolExecutor
"""--------------------------------------Spatial Filtering---------------------------------------------------------"""
def chunk_spacing(gdf, interval_length, cols=["sequence","id","url"], line_string_true=True):
    """
    Perform spacing calculation on chunks of a GeoDataFrame.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame to be processed.
        interval_length (float): Interval length for filtering points in kms.
        cols (list): List of column names used for grouping. Defaults to cols=["sequence","id","url"]
        line_string_true (bool, optional): Whether to create LineString geometries. Defaults to True.

    Returns:
        tuple: A tuple containing two lists:
            - The first list contains filtered GeoDataFrames after spacing calculation.
            - The second list contains LineString geometries if line_string_true is True; otherwise, an empty list.

    This function splits the GeoDataFrame into chunks based on the specified column names.
    It then performs spacing calculation using the calculate_spacing_vectorized function on each chunk.
    If line_string_true is True, it also creates LineString geometries using create_LineString function.
    """
    grouped = gdf.groupby(cols[0])
    filtered_gdfs = []
    list_of_linestrings = []
    for _, group in grouped:
        filtered_df, road_length = calculate_spacing_vectorized(gdf=group,
            interval_length=interval_length)
        filtered_gdfs.append(filtered_df)
        
        if line_string_true:
          linestring_subgdf = create_LineString(group,cols)
          if linestring_subgdf is not None:
            list_of_linestrings.append(linestring_subgdf)
    
    #Returns the filtered metadata of the chunk as a list of GeoDataFrames in case it is non-empty, otherwise it returns an empty list
    #Returns the LineStrings of the chunk as list of GeoDataFrames if "line_string_true" = True, otherwise an empty list
    return filtered_gdfs,list_of_linestrings

def parallel_spacing(gdf, interval_length,cols=["sequence","id","url"],line_string_true=True,chunk_size=500000,max_workers=64):
    """
    This function splits the input GeoDataFrame into smaller chunks and processes them in parallel.
    Each chunk is processed using the "chunk_spacing" function.
    The results are aggregated into two GeoDataFrames representing filtered metadata and LineString geometries.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame to be processed.
        interval_length (float): Interval length for filtering points in kms.
        cols (list): List of column names used for LineStrings. Defaults to ["sequence","id","url"]
        line_string_true (bool, optional): Whether to create LineString geometries. Defaults to True.
        chunk_size (int, optional): Size of chunks to be processed in parallel. Defaults to 200000.

    Returns:
        tuple: A tuple containing two GeoDataFrames:
            - The first GeoDataFrame contains filtered metadata after spacing calculation.
            - The second GeoDataFrame contains LineString geometries if line_string_true is True; otherwise, an empty DataFrame.

    """
    results = []
    line_strings = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        chunks = np.array_split(gdf, np.ceil(len(gdf) / chunk_size))
        for chunk in chunks:
            futures.append(executor.submit(chunk_spacing, chunk,interval_length=interval_length,
                                           cols=cols,line_string_true=line_string_true))
        
        for future in futures:
            result_chunk = future.result()
            results.extend(result_chunk[0])
            line_strings.extend(result_chunk[1])
        del futures
        
    filtered_metadata = pd.DataFrame()
    if len(results):
        filtered_metadata = gpd.GeoDataFrame(pd.concat(results,ignore_index=True))
        del results
        filtered_metadata.reset_index(drop=True,inplace=True)
        
    filtered_linestrings = pd.DataFrame()
    if len(line_strings):
        filtered_linestrings = pd.concat(line_strings,axis=0,ignore_index=True)
        del line_strings
        filtered_linestrings.reset_index(drop=True,inplace=True)
    
    #Returns filtered metadata from all chunks as a GeoDataFrame in case it is non-empty, otherwise it returns an empty DataFrame
    #Returns LineStrings from all chunks as a GeoDataFrame if "line_string_true" = True, otherwise an empty DataFrame
    if line_string_true:
        return filtered_metadata,gpd.GeoDataFrame(filtered_linestrings)
    else:
        return filtered_metadata,filtered_linestrings 
    
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
# This block creates LineStrings from filtered metadata and concatenate them

# 'create_LineString' does the creation of LineStrings. However, the gdf should only contain one sequence
# 'combine_LineStrings' concatenates LineStrings that have the same sequenceID. More than one LineSTrings with the same
# sequenceID could be created, if a sequence is divided between chunks. This function here corrects such cases.    
    
"""--------------------------------------LineStrings for Postprocessing---------------------------------------------------------"""
def create_LineString(gdf, cols=['sequence','id','url','timestamp']):
    """
    Create a GeoDataFrame with LineString geometry from a GeoDataFrame containing Point geometries.

    Parameters:
    gdf (geopandas.GeoDataFrame): Input GeoDataFrame containing Point geometries.
    cols (list): A list of column names. Defaults are "cols=['sequence','id','url']"

    Returns:
    geopandas.GeoDataFrame: A new GeoDataFrame with LineString geometry and with an info column that includes the image IDs 
                            and the URLs of the these images as a dictionary

    """
    linestring = []
    list_coords = [(point.x, point.y) for point in gdf['geometry'] if 'geometry' in gdf]
    dictionary = {row[cols[1]]: row[cols[2]] for _, row in gdf.iterrows()}
    

    if len(list_coords) >= 2:
        linestring.append(LineString(list_coords))
        row = gdf.iloc[0]
        gdf = gpd.GeoDataFrame({cols[0]: row[cols[0]], 'info': [dictionary], 'timestamp':row['timestamp']}, geometry=linestring,crs="EPSG:4326")
        return gdf
    else:
        return None
    
def combine_LineStrings(gdf,col='sequence'):
    """
    Combine LineString geometries in a GeoDataFrame based on duplicate values in a specified column.

    Parameters:
    gdf (geopandas.GeoDataFrame): Input GeoDataFrame containing LineString geometries.
    col (str): Name of the column used to identify duplicate sequences. Defaults to 'sequence'.

    Returns:
    geopandas.GeoDataFrame: A new GeoDataFrame with combined LineString geometries if there are duplicates
                            else it returns the original GeoDataFrame

    """
    duplicates = gdf[gdf.duplicated([col])]
    uniques = gdf[~gdf.duplicated([col])]
    if len(duplicates): #if there are duplicates
        grouped = duplicates.groupby(col) #group them by their "col"
        combined_linestrings = [] #list to store combined LineStrings
    
        for sequence, group in grouped:
          combined_line = ''
          combined_dict = {}
          for i in range(len(group)):
            if i == 0:
              combined_line = group.iloc[i]['geometry']
              combined_dict = group.iloc[i]['info']
            else:
              combined_line = combined_line.union(group.iloc[i]['geometry'])
 
              combined_dict.update(group.iloc[i]['info'])
          combined_linestrings.append({col:sequence,'info':[combined_dict],'geometry':combined_line,'timestamp':group.iloc[0]['timestamp']})
        
        #combined_linestrings = gpd.GeoDataFrame(pd.DataFrame(combined_linestrings),crs="EPSG:4326")
        gdf = gpd.GeoDataFrame(pd.concat([uniques, pd.DataFrame(combined_linestrings)], axis=0, ignore_index=True))
        del duplicates
        del uniques
    return gdf
      
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
# This block here is to fill the missing data in the older versions of the Database
# 'removing_duplicates' compare a given (filtered) DataFrame with another DataFrame (the older version) and removes duplicates
# if there are any
 
# 'process_one_pic' downloads the metadata associated with an image. It is meant to be used with 'adding_timestamps' to 
# extract the timestamp info of a given image

# 'adding_timestamps' adds the timestamp column to the older version of the Databas. However, it takes too much time so
# it is not efficient and the code must be improved. A possible approach would be to use asynchronous download in 'process_one_pic'
# by changing 'process_one_sequence' to 'async_process_one_sequence' and restructuring both functions to for asynchronous case 
"""--------------------------------------Comparing with older Files and updating them---------------------------------------------------"""
def removing_duplicates(gdf1, gdf2, col1, col11, col2, col22):
    """
    Remove duplicate entries between two GeoDataFrames based on specified columns.

    Parameters:
    gdf1 (pandas.DataFrame): First DataFrame containing entries to be filtered. 
    gdf2 (pandas.DataFrame): Second DataFrame used for filtering duplicate entries.
    col1 (str): Column name in gdf1 used for filtering. It should be the sequence columnn of gdf1
    col11 (str): Column name in gdf1 used for additional filtering. It should be the image ID columnn of gdf1
    col2 (str): Column name in gdf2 used for filtering.  It should be the sequence columnn of gdf2
    col22 (str): Column name in gdf2 used for additional filtering. It should be the image ID columnn of gdf2

    Returns:
    pandas.DataFrame: A DataFrame with removed duplicate entries.
    """
    columns = gdf1.columns
    # Create indexes on columns used for filtering
    gdf2_index = gdf2.set_index(col2)
    gdf2_col22_index = gdf2.set_index(col22)

    # Find rows in gdf1 that don't have a corresponding col2 value in gdf2
    unique_col2_values = set(gdf2_index.index.unique())
    filtered_gdf1 = gdf1[~gdf1[col1].isin(unique_col2_values)]

    # Find rows from gdf1 that have col1 values in gdf2 and col11 values not in gdf2[col22]
    duplicates_gdf1 = gdf1[gdf1[col1].isin(unique_col2_values)]
    unique_col22_values = set(gdf2_col22_index.index.unique())
    unique_gdf = duplicates_gdf1[~duplicates_gdf1[col11].isin(unique_col22_values)]

    # Concatenate the filtered and unique rows
    unique_gdf = pd.concat([filtered_gdf1[columns], unique_gdf[columns]],axis=0, ignore_index=True)
    unique_gdf.reset_index(inplace=True)

    return unique_gdf

def process_one_pic(sequence,mly_key,id_,call_limit_images=10,call_limit=5):
    """
    Process a single image and extract data for a specific ID.

    Args:
        sequence (object): The image sequence to be processed.
        mly_key (str): The Machine Learning Key required for processing.
        id_ (int): The ID for which data is to be extracted.
        call_limit_images (int) : the number of steps to terminate the loop if there is no new ids added (default is 10).
        call_limit (int) : number of step to terminate if the server doesn't respond in a meaningful way (default is 5).

    Returns:
        DataFrame: A DataFrame containing the data for the specified ID.
    """
    df_data, sequence_info = process_one_sequence(sequence, mly_key, call_limit_images=call_limit_images, call_limit=call_limit)
    return df_data[df_data['id'] == id_]

def adding_timestamps(gdf1,col1,col2,col3,mly_key,call_limit_images=10,call_limit=5):
    """ 
    Add timestamps to a GeoDataFrame based on image sequences.

    Args:
        gdf1 (GeoDataFrame): The GeoDataFrame to which timestamps are to be added.
        col1 (str): The name of the column containing  sequences.
        col2 (str): The name of the column containing image IDs.
        col3 (str): The name of the column to store timestamps.
        mly_key (str): The Machine Learning Key required for processing.
        call_limit_images (int) : the number of steps to terminate the loop if there is no new ids added (default is 10).
        call_limit (int) : number of step to terminate if the server doesn't respond in a meaningful way (default is 5).

    Returns:
        GeoDataFrame: The modified GeoDataFrame with timestamps added.
    """
    sequences = gdf1[col1].tolist()
    ids = gdf1[col2].tolist()
    timestamps = []
    with ThreadPoolExecutor() as executor:
        futures = []
        for sequence, id_ in zip(sequences, ids):
            futures.append(executor.submit(process_one_pic, sequence=sequence, mly_key=mly_key, id_=id_,
                                           call_limit_images=call_limit_images, call_limit=call_limit))
          
        for future in futures:
            row = future.result() 
            timestamps.append(row[col3].tolist()[0] if row is not None and col3 in row else np.nan)
    
    gdf1[col3] = timestamps
    return gdf1

##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
# This is where all the magic happens. There are different functions to do basically the same thing. The reason why there
# are so many of them is due to many trial and errors sprinkled with existential angst, frustration and Memory errors.

# 'get_all_unfiltered_metadata_at_once' reads the .csv files in a directory that have the same structure based on a condition
# concatenates them using 'unification_parallel', intersects the concatenated raw metadata with a continent polygon,
# writes this raw metadata to some number of files, in case somebody wants to download unfiltered images. It then goes on to
# intersecting this raw metadata with urban polygons of a continent and returns both the data within the polygons as well as
# the rest for further processsing. This could work for a continent with not many roads such as Africa but otherwise the 
# memory usage is very high and the process could terminate with a Memory Error. Use it with caution

# 'get_metadata' is the lowest level function in this case which reads a .csv file, intersects it first with a continent POLYGON
# and then with an urban POLYGON and returns the raw metadata for urban and non-urban areas so that they can be processed further
# where spacing will be applied. This function is meant to be used within another function. The resulting unfiltered raw files
# for both urban and non urban areas will be written to the output directory as .csv files

# 'get_metadata_in_chunks': same functionality as 'get_all_unfiltered_metadata_at_once' but without concatenating the raw
# metadata from all grids and then intersecting with the continent and then later with the urban polygons. This function 
# instead uses a ProcessPoolExecutor and the function 'get_metadata' to read files from the input directory and intersect with
# continent and urban POLYGONs

#'process_areas' uses either the raw results either from 'get_metadata_in_chunks' or from 'get_all_unfiltered_metadata_at_once'
# removes duplicates if there are any vis-a-vis the old database and then goes on to applying the spacing to both the urban and
# non-urbana areas. Then, it writes the duplicated-removed unfiltered raw metadata as well as the filtered metadata for both urban
# non-urban areas to .GPKG files as well as to .csv files around 500ish so that the images could be downloaded. This function 
# can only work however, if there has been no memory errors in former steps in 'get_all_unfiltered_metadata_at_once' or in
# 'get_metadata_in_chunks', so it is highly unlikely. Hence the commenting out the relevant steps in the script

# 'process_in_chunks' is a higher level function for 'get_metadata'. It passes a file directory as well as relevant POLYGONs
# to 'get_metadata' and then applies the spacing to both the urban and non-urban areas separately. It will create LineStrings
# as well if this is wanted. The filtered files for both urban and non-urban areas will be written to directories as .gkpg files.
# This is the best approach so far. 

# 'get_metadata_and_process_them_in_chunks' is a higher level function for 'process_in_chunks'. It calls this concurrently and passes
# each time a file in the input directory. The resulting filtered urban and non-urban DataFrames will be concatenated including the 
# LineStrings. Duplicates will be removed in both cases. The resulting files will be written as .GPKG files onto the output
# directory and GeoDataFrames for urban and non-urban areas will be divided into a number of .csv files so that the
# images associated with the metadata could be downloaded parallely. Despite the noble idea, this approach drains the memory
# most of the time, so I commented out its usage. A more sound approach seems to be run as many as scripts as there are grids,
# write the filtered metadata form each script to files and then reading the filtered .gpkg files from a different script and doing
# all this.

# 'concatenation_of_results' concatenates as the name suggests .GPKG files from the input directory which have earlier been created by
# 'process_in_chunks' in parallel, removes any duplicates vis-a-vis the old database the concatenated file to a GKPG and divides it into
# some number of files for parallel download.

# 'main' defines parameter names, spacing, number of workers etc and orchestrates the code for its implementation for both urban and non-urban
# cases. if concatenation is True, it will use 'concatenation_of_results' otherwise it will read the the grid number from the script name and
# implement 'process_in_chunks'. In the latter case, the script must be run again after the finalisation of 'process_in_chunks' to concatenate
# the results

"""--------------------------------------Orchestrating the Code---------------------------------------------------"""
def get_all_unfiltered_metadata_at_once(dir1,dir4,condition_unfiltered_metadadata,col1,col11,
                                        number_of_files,prototype_corrected_unfiltered,file_name_corrected_unfiltered_json,
                                        polygon_continent,intersecting_file,max_workers):
    """
    Retrieve, process, and save unfiltered metadata from a directory, performing parallel operations.

    Args:
        dir1 (str): Directory path containing unfiltered metadata files.
        dir4 (str): Directory path where the output files will be saved.
        condition_unfiltered_metadadata (str): Condition to filter unfiltered metadata.
        col1 (str): Column 1 to consider in the unification process.
        col11 (str): Column 2 to consider in the unification process.
        number_of_files (int): Number of files to split the data into for writing.
        prototype_corrected_unfiltered (str): Prototype file name for the output files.
        file_name_corrected_unfiltered_json (str): File name for the output GeoPackage file.
        max_workers (int): Maximum number of worker threads for parallel processing.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame containing the processed unfiltered metadata
                                 after spatial intersection with a continent polygon dataset.

    Note:
        This function performs the following operations:
        1. Retrieves unfiltered metadata from dir1.
        2. Drops rows with missing values in specific columns.
        3. Converts the 'geometry' column to GeoPandas-compatible format.
        4. Performs a spatial intersection with a continent polygon dataset.
        5. Writes the resulting GeoDataFrame to multiple files and a GeoPackage file.

    """
    df_unfiltered = unification_parallel(dir1, condition_unfiltered_metadadata,col1,col11,max_workers=max_workers)
    df_unfiltered = df_unfiltered.dropna(subset=['geometry','sequence','id','timestamp','url'])
    df_unfiltered['geometry'] = df_unfiltered['geometry'].apply(wkt.loads)
    gdf_unfiltered = gpd.GeoDataFrame(df_unfiltered, geometry='geometry', crs="EPSG:4326")
    del df_unfiltered
    
    #-----------------------------------------intersecting with continent polygons----------------------------#
    polygon_continent = gpd.read_file(polygon_continent)
    gdf_unfiltered_intersected,_ = parallel_intersect(gdf_unfiltered,polygon_continent,max_workers=max_workers)
    del _
    del polygon_continent
    del gdf_unfiltered
    
    writing_to_new_files(gdf_unfiltered_intersected,number_of_files,prototype_corrected_unfiltered,dir4,max_workers)
    gdf_unfiltered_intersected.to_file(file_name_corrected_unfiltered_json,driver='GPKG',index=False)
    
    #-----------------------------------------intersecting with urban polygons----------------------------#
    intersecting = gpd.read_file(intersecting_file)
    results_within,results_outside = parallel_intersect(gdf_unfiltered_intersected, intersecting,max_workers=max_workers)
    del intersecting_file
    del gdf_unfiltered_intersected
    
    results_within.reset_index(drop=True,inplace=True)
    results_outside.reset_index(drop=True,inplace=True)
    return results_within,results_outside 

def get_metadata_in_chunks(dir1,dir4,col11,intersecting_file_continent,intersecting_file_area,
                           condition_unfiltered_metadadata,prototype_corrected_unfiltered,max_workers=4):
    
    files = glob.glob(f'{dir1}/{condition_unfiltered_metadadata}')
    results_within = []
    results_outside = []
    intersecting_continent = gpd.read_file(intersecting_file_continent)
    intersecting_area = gpd.read_file(intersecting_file_area)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for _ in range(len(files)):
            file = files.pop()
            file_to_be_read = file
            file_index = file.split(".")[0].split("_")[-1]
            file_to_be_written = os.path.join(dir4,f'{prototype_corrected_unfiltered}_{file_index}.csv')
            future = executor.submit(get_metadata,file_to_be_read,file_to_be_written,intersecting_continent,intersecting_area)
            if future is not None:
                result = future.result()
                results_within.append(result[0])
                results_outside.append(result[1])
                del result
            del future
            
    intersecting_continent = None
    intersecting_area = None
    
    if len(results_within):
        results_within = pd.concat(results_within,ignore_index=True)
        results_within = results_within.drop_duplicates(subset=[col11], keep='first',ignore_index=True).reset_index(drop=True)
    if len(results_outside):
        results_outside = pd.concat(results_outside,ignore_index=True).reset_index(drop=True)
        results_outside = results_outside.drop_duplicates(subset=[col11], keep='first',ignore_index=True).reset_index(drop=True)
        
    return results_within,results_outside

def process_areas(results_df, check_against_gdf, col1, col11, col12, col2, col22, 
                line_string_true, interval_length,dir2, dir3, dir4, continent, area,number_of_files, 
                prototype_unfiltered, prototype_filtered, file_filtered_uncontained, 
                max_workers):
    """
    Process spatial data for a specific type of area.It removes duplicates by checking the code against
    the 'results_df' using the columns, 'col1', 'col11' of the 'results_df' against the columns,
   'col2', 'col22' of 'check_against_gdf'. It then writes the unique metadata to 'dir4' using 
    in form given by 'prototype_unfiltered', it further creates a GPKG for the whole unfiltered metadata
    in the folder 'dir3'. Then it applies the spacing given by 'interval_length' and creates LineStrings
    if this is True. Finally,the filtered metadata is written to 'number_of_files' in the directory 'dir2'
    and a GPKG file is created for the filtered metadata with filename given by 'file_filtered_uncontained'

    Parameters:
        results_df (GeoDataFrame): The spatial data for the area to be processed.
        check_against_gdf (GeoDataFrame): The reference dataset to check duplicates against.
        col1, col11, col12, col2, col22 (str): Column names for data processing.
        line_string_true (bool): Indicates whether to process LineStrings.
        interval_length (float): The interval length for parallel spacing.
        dir2, dir3, dir4 (str): Directories for file operations.
        continent (str): The continent name for file naming.
        area (str): The type of area being processed (e.g., 'urban', 'other').
        number_of_files (int): Number of files for writing data.
        prototype_unfiltered (str): Prototype for unfiltered file names.
        prototype_filtered (str): Prototype for filtered file names.
        file_filtered_uncontained (str): File name for filtered uncontained areas.
        max_workers (int): Maximum number of workers for parallel processing.
     
    Returns:
        None
    """
    unfiltered_uncontained = removing_duplicates(results_df, check_against_gdf, col1, col11, col2, col22)
    del results_df
    writing_to_new_files(unfiltered_uncontained, number_of_files, prototype_unfiltered, dir4, max_workers)
    unfiltered_uncontained.to_file(f'{dir3}/unfiltered_{area}_areas_{continent}.gpkg', driver='GPKG', index=False)
    filtered_uncontained, linestrings = parallel_spacing(unfiltered_uncontained,
                                                            interval_length, [col1, col11, col12],
                                                            line_string_true, max_workers=max_workers)
    del unfiltered_uncontained
    if line_string_true and isinstance(linestrings, gpd.GeoDataFrame):
        linestrings = combine_LineStrings(linestrings, col1)
        linestrings.to_file(f'{dir3}/linestrings_{area}_areas_{int(interval_length)}m_filtered.gpkg', driver='GPKG', index=False)
    linestrings = None
    writing_to_new_files(filtered_uncontained, number_of_files, prototype_filtered, dir2, max_workers)
    filtered_uncontained.to_file(file_filtered_uncontained, driver='GPKG', index=False)
    del filtered_uncontained
    
def get_metadata(file_to_be_read,continent_file,within_file,outside_file,continent_df,intersecting):
    df = read_file(file_to_be_read)
    result_within = result_outside = None
    if df is not None:
        df = df.dropna(subset=['geometry','sequence','id','timestamp','url'])
        df['geometry'] = df['geometry'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        del df
        #-----------------------------------------intersecting with continent----------------------------#
        gdf,_ = intersect_chunk(gdf,continent_df)
        del _
        write_file(gdf,continent_file)
        #-----------------------------------------intersecting with urban polygons----------------------------#
        result_within,result_outside = intersect_chunk(gdf,intersecting)
        del gdf
        write_file(result_within,within_file)
        write_file(result_outside,outside_file)
        
    return result_within,result_outside

def process_in_chunks(file_to_be_read,continent_file,within_file,outside_file,intersecting_continent,intersecting_area,
                      interval_urban,interval_other,file_filtered_urban,file_filtered_other,line_string_true,col1,max_workers):

    filtered_within = filtered_outside = linestring_within = linestring_outside = None
    result_within,result_outside = get_metadata(file_to_be_read,continent_file,within_file,outside_file,intersecting_continent,intersecting_area)
    
    def process(unfiltered_uncontained,interval_length,line_string_true,col1,file_filtered,chunk_size,max_workers):
        
        filtered_uncontained, linestrings = parallel_spacing(gdf=unfiltered_uncontained,interval_length=interval_length,
                                                             line_string_true=line_string_true,chunk_size=chunk_size,max_workers=max_workers)
        del unfiltered_uncontained
        if line_string_true and isinstance(linestrings, gpd.GeoDataFrame):
            linestrings = combine_LineStrings(linestrings, col1)
        filtered_uncontained.to_file(file_filtered, driver='GPKG', index=False)
        return filtered_uncontained,linestrings
    
    if result_within is not None:
        chunk_size_urban = int(len(result_within)/max_workers)
        if chunk_size_urban != 0:
            filtered_within,linestring_within = process(result_within,interval_urban,line_string_true,col1,file_filtered_urban,chunk_size_urban,max_workers)
        del result_within
    
    if result_outside is not None:
        chunk_size_other = int(len(result_outside)/max_workers)
        if chunk_size_other != 0:
            filtered_outside,linestring_outside = process(result_outside,interval_other,line_string_true,col1,file_filtered_other,chunk_size_other,max_workers)
        del result_outside
        
    return filtered_within,filtered_outside,linestring_within,linestring_outside

def get_metadata_and_process_them_in_chunks(dir1,dir2,dir4,col1,col11,col2,col22,intersecting_file_continent,intersecting_file_area,check_against_gdf,
                                           condition_unfiltered_metadadata,prototype_corrected_unfiltered,prototype_unfiltered_urban, prototype_unfiltered_other,
                                           prototype_filtered_urban, prototype_filtered_other,file_filtered_uncontained_urban,file_filtered_uncontained_other,
                                           linestrings_urban_file,linestrings_other_file,interval_urban,interval_other,line_string_true,
                                           number_of_files,max_workers=4):
    
    files = glob.glob(f'{dir1}/{condition_unfiltered_metadadata}')
    intersecting_continent = gpd.read_file(intersecting_file_continent)
    intersecting_area = gpd.read_file(intersecting_file_area)
    
    results_within = []
    results_outside = []
    linestrings_within = []
    linestrings_outside = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for _ in range(len(files)):
            file_to_be_read = files.pop()
            file_index = file_to_be_read.split(".")[0].split("_")[-1]
            continent_file = os.path.join(dir4,f'{prototype_corrected_unfiltered}_{file_index}.csv')
            within_file = os.path.join(dir4,f'{prototype_unfiltered_urban}_{file_index}.csv')
            outside_file = os.path.join(dir4,f'{prototype_unfiltered_other}_{file_index}.csv')
            filtered_within_file = os.path.join(dir2,f'{prototype_filtered_urban}_{file_index}.gpkg')
            filtered_outside_file = os.path.join(dir2,f'{prototype_filtered_other}_{file_index}.gpkg')
            
            future = executor.submit(process_in_chunks,file_to_be_read,
                                     continent_file,within_file,outside_file,intersecting_continent,intersecting_area,
                                     interval_urban,interval_other,filtered_within_file,filtered_outside_file,
                                     line_string_true,col1,max_workers=15)
            
            
            if future is not None:
                filtered_within,filtered_outside,linestring_within,linestring_outside = future.result()
                
                if filtered_within is not None:
                    results_within.append(filtered_within)
                if filtered_outside is not None:    
                    results_outside.append(filtered_outside)
                    
                if line_string_true:
                    if linestring_within is not None:
                        linestrings_within.append(linestring_within)
                    if linestring_outside is not None:
                        linestrings_outside.append(linestring_outside)
                
        results_within = pd.concat(results_within)
        results_within = removing_duplicates(results_within, check_against_gdf, col1, col11, col2, col22)
        results_within.drop_duplicates(col11,keep='first',inplace=True,ignore_index=True)
        results_within = gpd.GeoDataFrame(results_within)
        results_within.to_file(file_filtered_uncontained_urban,driver='GPKG',index=False)
        writing_to_new_files(results_within, number_of_files, prototype_filtered_urban, dir2, max_workers)
        del results_within

        results_outside = pd.concat(results_outside)
        results_outside = removing_duplicates(results_outside, check_against_gdf, col1, col11, col2, col22)
        results_outside.drop_duplicates(col11,keep='first',inplace=True,ignore_index=True)
        results_outside = gpd.GeoDataFrame(results_outside)
        results_outside.to_file(file_filtered_uncontained_other,driver='GPKG',index=False)
        writing_to_new_files(results_outside, number_of_files, prototype_filtered_other, dir2, max_workers)
        del results_outside
        
        if line_string_true:
            linestrings_within = pd.concat(linestrings_within)
            linestrings_within = combine_LineStrings(linestrings_within)
            linestrings_within['info'] = linestrings_within['info'].apply(json.dumps)
            linestrings_within.to_file(linestrings_urban_file,driver='GPKG',index=False)
            del linestrings_within
            
            linestrings_outside = pd.concat(linestrings_outside)
            linestrings_outside = combine_LineStrings(linestrings_outside)
            linestrings_outside['info'] = linestrings_outside['info'].apply(json.dumps)
            linestrings_outside.to_file(linestrings_other_file,driver='GPKG',index=False)
            del linestrings_outside
            
def concatenation_of_results(dir2,prototype_filtered,file_filtered_uncontained,check_against_gdf, col1, col11, col2, col22,number_of_files,max_workers):
    filtered_within_file = f'{prototype_filtered}_*.gpkg'
    results_within = gpd.GeoDataFrame(unification_parallel(dir2, filtered_within_file, col1="sequence",col2="id",max_workers=64))
    results_within = removing_duplicates(results_within, check_against_gdf, col1, col11, col2, col22)
    results_within.drop_duplicates(col11,keep='first',inplace=True,ignore_index=True)
    results_within = gpd.GeoDataFrame(results_within)
    results_within.to_file(file_filtered_uncontained,driver='GPKG',index=False)
    writing_to_new_files(results_within, number_of_files, prototype_filtered, dir2, max_workers)
    del results_within
    
def main(concatenation):
    """--------------------------------------Definition of Parameters---------------------------------------------------------"""
    mly_key = "MLY|6700072406727486|6534c2cab766ff1eaa4c4defeb21a3f4"
    continent = 'AFR'
    continent_long = 'africa'
    number_of_files = 500
    interval_length_urban = 0.1
    interval_length_other = 1
    max_workers = int(os.cpu_count())
    col1 = 'sequence' #columns of the metadata
    col11 = 'id' #columns of the metadata
    col12 = 'url' #columns of the metadata
    col2= 'sequence_id' #columns of the older file, created metadata will be checked against this file
    col22 = 'image_id' # columns of the older file, created metadata will be checked against this file
    col3 = 'timestamp'
    line_string_urban_true = False
    line_string_other_true = False

    dir_older_pics = f'/mnt/sds-hd/sd17f001/eren/continents_new/{continent_long}_1000m'
    old_points = 'predictions_africa_new_wo_duplicates.gpkg'

    dir = f'/mnt/sds-hd/sd17f001/eren/metadata_manipulation_new/{continent_long}'
    dir1 = f'{dir}/raw_metadata'
    dir2 = f'{dir}/processed_metadata/filtered'
    dir3 = f'{dir}/geo_processing'
    dir4 = f'{dir}/processed_metadata/unfiltered'

    polygon_continent = f'{dir3}/{continent_long}_outline.geojson'
    intersecting_file = f'{dir3}/AFRICAPOLIS2020.shp'

    condition_filtered_metadadata = f'metadata_{continent}_*.csv'
    condition_unfiltered_metadadata = f'metadata_unfiltered_{continent}_*.csv'

    prototype_corrected_filtered = f'metadata_filtered_{continent}'
    prototype_corrected_unfiltered = f'metadata_unfiltered_{continent}'

    file_name_corrected_filtered_json =  f'{dir3}/metadata_filtered_{continent}.gpkg'
    file_name_corrected_unfiltered_json = f'{dir3}/metadata_unfiltered_{continent}.gpkg'

    prototype_unfiltered_urban = f'metadata_unfiltered_urban_{continent}'
    prototype_unfiltered_other = f'metadata_unfiltered_other_{continent}'

    file_filtered_uncontained_urban = f'{dir3}/{continent}_metadata_urban_areas_{int(interval_length_urban*1000)}_m.gpkg'
    file_filtered_uncontained_other = f'{dir3}/{continent}_metadata_other_{int(interval_length_other*1000)}_m.gpkg'

    prototype_filtered_urban = f'metadata_urban_areas_filtered_{int(interval_length_urban*1000)}_{continent}'
    prototype_filtered_other = f'metadata_other_areas_filtered_{int(interval_length_other*1000)}_{continent}'
    
    linestrings_urban_file = f'{dir3}/linestrings_urban_areas_{int(interval_length_urban*1000)}m_filtered.gpkg'
    linestrings_other_file = f'{dir3}/linestrings_other_areas_{int(interval_length_other*1000)}m_filtered.gpkg'

    if not os.path.exists(dir2):
        os.makedirs(dir2,exist_ok=True)
    if not os.path.exists(dir3):
        os.makedirs(dir3,exist_ok=True)
    if not os.path.exists(dir4):
        os.makedirs(dir4,exist_ok=True)
    _ = [shutil.copy2(f'{dir}/{x}', f'{dir1}/{x}') for x in os.listdir(dir) if x.endswith('.csv')]
    
    """--------------------------------------Et Actio---------------------------------------------------------"""
    #-----------------------unfiltered data set------------------------------------------------------#
    #results_within,results_outside = get_all_unfiltered_metadata_at_once(dir1,dir4,condition_unfiltered_metadadata,
     #                                                                    col1,col11,number_of_files,prototype_corrected_unfiltered,
      #                                                                   file_name_corrected_unfiltered_json,polygon_continent,
       #                                                                  intersecting_file,max_workers)
    
    #results_within,results_outside = get_metadata_in_chunks(dir1,dir4,col11,polygon_continent,
    #                                                        intersecting_file,condition_unfiltered_metadadata,
    #                                                        prototype_corrected_unfiltered,4)
    #
    check_against_gdf = gpd.read_file(os.path.join(dir_older_pics,old_points))
    #get_metadata_and_process_them_in_chunks(dir1,dir2,dir4,col1,col11,col2,col22,polygon_continent,intersecting_file,check_against_gdf,
    #                                       condition_unfiltered_metadadata,prototype_corrected_unfiltered,prototype_unfiltered_urban, prototype_unfiltered_other,
    #                                       prototype_filtered_urban, prototype_filtered_other,file_filtered_uncontained_urban,file_filtered_uncontained_other,
    #                                       linestrings_urban_file,linestrings_other_file,interval_length_urban,interval_length_other,line_string_urban_true,
    #                                       number_of_files,max_workers=4)
    #----------------------------urban areas-----------------------------------------------------------#
    #process_areas(results_within, check_against_gdf, col1, col11, col12, col2, col22,
    #            line_string_urban_true, interval_length_urban, dir2, dir3, dir4,continent,
    #            'urban', number_of_files, prototype_unfiltered_urban, prototype_filtered_urban,
    #            file_filtered_uncontained_urban, max_workers)
    #--------------------------------------other areas--------------------------------------------------#
    #process_areas(results_outside, check_against_gdf, col1, col11, col12, col2, col22,
    #              line_string_other_true, interval_length_other, dir2, dir3, dir4,continent,
    #              'other', number_of_files, prototype_unfiltered_other, prototype_filtered_other,
    #              file_filtered_uncontained_other, max_workers)
    #---------------------------------------adding timestamps-----------------------------------------------#
    #predictions_gdf_timestamps_added = adding_timestamps(check_against_gdf,col2,col22,col3,mly_key)
    #predictions_gdf_timestamps_added.to_file(f'{dir_older_pics}/{old_points[0:-8]}_timestamps_added.gpkg',driver='GPKG',index=False)
    #check_against_gdf = None
    
    if concatenation == False:
        file_index = __file__.split('/')[-1].split('.')[0]
        file_to_be_read = os.path.join(dir,f'metadata_unfiltered_{continent}_{file_index}.csv')
        intersecting_continent = gpd.read_file(polygon_continent)
        intersecting_area = gpd.read_file(intersecting_file)
    
        continent_file = os.path.join(dir4,f'{prototype_corrected_unfiltered}_{file_index}.csv')
        within_file = os.path.join(dir4,f'{prototype_unfiltered_urban}_{file_index}.csv')
        outside_file = os.path.join(dir4,f'{prototype_unfiltered_other}_{file_index}.csv')
        filtered_within_file = os.path.join(dir2,f'{prototype_filtered_urban}_{file_index}.gpkg')
        filtered_outside_file = os.path.join(dir2,f'{prototype_filtered_other}_{file_index}.gpkg')
            
        result_within,result_outside,linestrings_within, linestrings_outside = process_in_chunks(file_to_be_read,
                                    continent_file,within_file,outside_file,intersecting_continent,intersecting_area,
                                    interval_length_urban,interval_length_other,filtered_within_file,filtered_outside_file,
                                    line_string_other_true,col1,max_workers=15)
    
        del result_within
        del result_outside
        del linestrings_within
        del linestrings_outside
    
    else:
        concatenation_of_results(dir2,prototype_filtered_urban,file_filtered_uncontained_urban,check_against_gdf, col1, col11, col2, col22,number_of_files,max_workers)
        concatenation_of_results(dir2,prototype_filtered_other,file_filtered_uncontained_other,check_against_gdf, col1, col11, col2, col22,number_of_files,max_workers)
    
if __name__ == "__main__":
    concatenation = True
    main(concatenation)


