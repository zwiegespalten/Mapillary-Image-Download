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
   
