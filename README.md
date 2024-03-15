# Mapillary-Image-Download
Mapillary Image Download

This repository is for the download of sequences of images from the Mapillary API.
The images can then further be used in ML or DL algorithms

## metadata_processing.py

This script is the lowest level of the module. It deals with getting meaningful responses from the Mapillary API both
for getting the $sequenceIDs$ which are unique IDs associated with a sequence of images taken after one another by the same user and ordered by their timestamp

There are actually three functions here with each function having two differnt implementations. One is for normal implementations which will be used by functions in "metadata_download.py"
which implement synchronous dealing with the Mapillary API. The other is the asychronous implementation with the same functionality to be used default. 

"async_get_response" and "get_response" extract the respond from the Mapillary API and and returns this unless the server has responded  "call_limit" times with None, then they return None

'async_adding_images_to_each_other' and 'adding_images_to_each_other' add image metadata from different calls together and terminates the loop when no more new image data is added after 'call_limit_images' times.

 'process_one_sequence' and 'async_process_one_sequence' orchestrate the download of the image metadata for a single sequenceID

## metadata_download.py

This is the main script for the download of metadata. Like 'metadata_processing.py', it implements both synchronous and asynchronous downloads as well Multiprocessing and Multithreading.
I have added the synchronous as well, even though asynchronous implementations combined with Multithreading seem to work the most efficiently. Asynchronous implementations came later
after hell of a trial and errors sprinkled with frustration and many existential crisis. 

The script is implemented in blocks: 

### Division of bboxes
   Functions here divide a given bbox into smaller bboxes. Duh. "divide_bbox" into 2 smaller bbox along some line. "segmented_bboxes" does this, but into more bboxes

### Getting needed bboxes and unique sequences

   These functions query a given bbox and return the sequenceIDs therein.They keep dividing the bbox calling themselves till less than 2000 elements are returned
   
   "async_get_bboxes_and_sequences" does this using an asyncchronous event loop and is the most efficient sofar
   "get_bboxes_and_sequences" does this using for loops
   "get_bboxes_and_sequences_parallel" does this using an executor either a ProcessPoolExecutor or ThreadPoolExecutor
   
   "async_get_bboxes_and_sequences_higher_level" is meant to be used with "async_get_bboxes_and_sequences" and provides this with a "Session". You do not need this, if you provide a "session" in your code
   
   "async_get_bboxes_and_sequences_wrapping" as the name suggests, wraps "async_get_bboxes_and_sequences_higher_level" around. It runs the "async_get_bboxes_and_sequences_higher_level" asyncchronously and is meant to be used  if "async_get_bboxes_and_sequences_higher_level" is used within a ProcessPoolExecutor or ThreadPoolExecutor, as "coroutines" cannot be passed to these.

### Getting the metadata
 These functions retrieve the metadata of images associated with sequenceIDs. I used to have a function "metadata_mapillary", hence the name "metadata_mapillary_parallelized" which used forloops to 
 iterate over sequenceIDs but it wasn't efficient. You can implement this but don't.
    
 "metadata_mapillary_parallelized" downloads the metadata of images, provided with a list of sequenceIDs using asynchronous download
    
 "metadata_mapillary_parallelized_wrapping" wraps around the latter to be used by a ProcessPoolExecutor or ThreadPoolExecutor since 'coroutines' cannot be passed to these.

### Et Actio
   Well well well, we need to use these functions somehow. Otherwise they were for naught

   'missing_sequences_download':  Since life sucks and you cannot download stuff when you want to such sequenceIDs will be saved into the directory so that you can try to download them later using this function here.
   
   'get_sequences': this mf here orchestrates the extraction of sequenceIDs provided a bbox. You can skip the asynchronous implementation and use a ThreadPoolExecutor or ProcessPoolExecutor which will then use 'get_bboxes_and_sequence_parallel' in its stead. But don't. Unless you want to suffer, then do. I did not implement a strategy which uses 'get_bboxes_and_sequences' using forloops because nobody ain't gonne live that long
   
   'get_metadata' downloads the metadata associated with a list of sequenceIDs. You have the option to choose between the usage of only a ThreadPoolExecutor or a ThreadPoolExecutor combined with asynchronous download. Go for this, it is the default. Do not replace ThreadPoolExecutor with a ProcessPoolExecutor unless you want to suffer, then don't.
   
   'main' combines 'get_sequences' with 'get_metadata'

## missing_sequences.py

This is a small script with only one functionality, that is the download of missing sequences. There happens to be some sequences that could not be downloaded by 'metadata_download.py'. The code will skip these sequences after awhile and continue with the download of the rest of the sequences while writing the undownloaded sequences to a .csv file. The main function of this script will read these files from the multitude of grids, put them together and try to download them. That is why this script should be run AFTER all grid downloads have been completed. 
## spacing_calculation.py

This is small, lower level script to calculate distances and filter points based on them. Its functions will be called from 'data_integration_and_filtering.py'

'distance_on_sphere_vectorized' is vectorized funtion to calculate the distance between two points. It is basically the haversine function and can also be used with points or two point arrays

'filter_points' will iterate over points and their distances to one another within a sequence , aggregate them filter out those that are less than the wanted distance

'calculate_spacing_vectorized' is a higher level function which will order the points based on their timestamp first and then apply 'filter_points'
## data_integration_and_filtering.py

This script is one hell of a beast. It deals with the downloaded metadata. Like other scripts, it is divided into blocks with separate functionalities. The main aim of the script is:

1) to read raw metadata from grids files
2) to intersect the grids with a continent POLYGON to remove images outside of this continent. It could create a GKPG file for the whole coverage of a continent with raw metadata. However, this does not work very well due to file being very large and memory errors. If this should be possible, it will also divide this file into some number of .csv files so that the images can be downloaded in the next step in parallel
3) to intersect the raw metadata of a continent with urban polygons. Depending on the function being used, it can either do this by getting the whole raw metadata first and then intersecting it urban polygons but this drains memory or by intersecting the metadata of a grid with urban polygon. The second approach is the default. Caution is advised for the first one. Some parts of the code must be de-commented out if you want to use the first approach. The intersected raw metadata for urban and non-urban areas will be written to files
4) to apply the spacing condition to urban and non-urban areas separately. The script as it is will handle at each it is run, only one grid, this script is should be run as many times as there are grids and the name of the script must be changed to the number of grid. The filtered DataFrames will be written to .GPKG files. It can also create LineStrings from the sequences if this is wanted too
5) It will concatenate the results from different grids together, writes it to a finalised .GPKG file and divide its content to a number of .csv files for the download of images in parallel. However, for the concatenation of the results, the script must be run once again and the condition in the main function must be changed.

Now the blocks:
### Reading and Writing Files: 
This block serves the purporse of reading the image metadata and writing them to smaller files
'read_file' is for reading the image metadata with pre-given data types
'write_file' is just to write a file to .csv
'writing_to_new_files' divides a DataFrame into 'number_of_files' and writes them into files
'unification_parallel' reads csv files using 'read_file' and ProcessPoolExecutor and concatenates them to create
a DataFrame containing metadata from all files. However, it should be used with caution since it uses a great deal of
Memory and can result in memory error if used with unfiltered raw metadata
   
### Intersection
This block has two functions and serves the purpose of intersecting a DataFrame with a Polygon

'intersect_chunk' is the actual function intersecting a DataFrame
'parallel_intersect' coordinates the 'intersect_chunk' by using a PoolProcessExecutor and dividing a given DataFrame into smaller chunks to speed up the process. Caution advised if the DataFrame is very large, then a memory error could be thrown

### Spatial Filtering

This block here applies the spacing to raw metadata
'chunk_spacing' is the function that groups and filters the metadata based on 'interval_length' by passing them to 'calculate_spacing_vectorized'. If 'line_string_true' is True, it will also create LineStrings from  the filtered metadata

'parallel_spacing' coordinates the application of 'chunk_spacing' by dividing the metadata into chunks and running thecode concurrently using ProcessPoolExecutor

### LineStrings for Postprocessing
This block creates LineStrings from filtered metadata and concatenate them

'create_LineString' does the creation of LineStrings. However, the gdf should only contain one sequence
'combine_LineStrings' concatenates LineStrings that have the same sequenceID. More than one LineSTrings with the same sequenceID could be created, if a sequence is divided between chunks. This function here corrects such cases.  

### Comparing with older Files and updating them
This block here is to fill the missing data in the older versions of the Database 'removing_duplicates' compare a given (filtered) DataFrame with another DataFrame (the older version) and removes duplicates if there are any
 
'process_one_pic' downloads the metadata associated with an image. It is meant to be used with 'adding_timestamps' to extract the timestamp info of a given image

'adding_timestamps' adds the timestamp column to the older version of the Databas. However, it takes too much time so it is not efficient and the code must be improved. A possible approach would be to use asynchronous download in 'process_one_pic' by changing 'process_one_sequence' to 'async_process_one_sequence' and restructuring both functions to for asynchronous case 

### Orchestrating the Code

This is where all the magic happens. There are different functions to do basically the same thing. The reason why there are so many of them is due to many trial and errors sprinkled with existential angst, frustration and Memory errors.

'get_all_unfiltered_metadata_at_once' reads the .csv files in a directory that have the same structure based on a condition concatenates them using 'unification_parallel', intersects the concatenated raw metadata with a continent polygon, writes this raw metadata to some number of files, in case somebody wants to download unfiltered images. It then goes on to intersecting this raw metadata with urban polygons of a continent and returns both the data within the polygons as well as the rest for further processsing. This could work for a continent with not many roads such as Africa but otherwise the memory usage is very high and the process could terminate with a Memory Error. Use it with caution

'get_metadata' is the lowest level function in this case which reads a .csv file, intersects it first with a continent POLYGON and then with an urban POLYGON and returns the raw metadata for urban and non-urban areas so that they can be processed further where spacing will be applied. This function is meant to be used within another function. The resulting unfiltered raw files for both urban and non urban areas will be written to the output directory as .csv files

'get_metadata_in_chunks': same functionality as 'get_all_unfiltered_metadata_at_once' but without concatenating the raw metadata from all grids and then intersecting with the continent and then later with the urban polygons. This function instead uses a ProcessPoolExecutor and the function 'get_metadata' to read files from the input directory and intersect with continent and urban POLYGONs

'process_areas' uses either the raw results either from 'get_metadata_in_chunks' or from 'get_all_unfiltered_metadata_at_once' removes duplicates if there are any vis-a-vis the old database and then goes on to applying the spacing to both the urban and non-urban areas. Then, it writes the duplicated-removed unfiltered raw metadata as well as the filtered metadata for both urban and non-urban areas to .GPKG files as well as to .csv files around 500ish so that the images could be downloaded. This function can only work however, if there has been no memory errors in former steps in 'get_all_unfiltered_metadata_at_once' or in 'get_metadata_in_chunks', so it is highly unlikely. Hence the commenting out the relevant steps in the script

'process_in_chunks' is a higher level function for 'get_metadata'. It passes a file directory as well as relevant POLYGONs to 'get_metadata' and then applies the spacing to both the urban and non-urban areas separately. It will create LineStrings as well if this is wanted. The filtered files for both urban and non-urban areas will be written to directories as .gkpg files. This is the best approach so far. 

'get_metadata_and_process_them_in_chunks' is a higher level function for 'process_in_chunks'. It calls this concurrently and passes each time a file in the input directory. The resulting filtered urban and non-urban DataFrames will be concatenated including the  LineStrings. Duplicates will be removed in both cases. The resulting files will be written as .GPKG files onto the output directory and GeoDataFrames for urban and non-urban areas will be divided into a number of .csv files so that the images associated with the metadata could be downloaded parallely. Despite the noble idea, this approach drains the memory most of the time, so I commented out its usage. A more sound approach seems to be run as many as scripts as there are grids, write the filtered metadata form each script to files and then reading the filtered .gpkg files from a different script and doing all this.

'concatenation_of_results' concatenates as the name suggests .GPKG files from the input directory which have earlier been created by 'process_in_chunks' in parallel, removes any duplicates vis-a-vis the old database the concatenated file to a GKPG and divides it into some number of files for parallel download.

'main' defines parameter names, spacing, number of workers etc and orchestrates the code for its implementation for both urban and non-urban cases. if concatenation is True, it will use 'concatenation_of_results' otherwise it will read the the grid number from the script name and implement 'process_in_chunks'. In the latter case, the script must be run again after the finalisation of 'process_in_chunks' to concatenate the results


