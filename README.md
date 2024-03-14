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

1) Functions in block '-Division of bboxes' divide a given bbox into smaller bboxes. Duh. "divide_bbox" into 2. "segmented_bboxes" for more

2) 'Getting needed bboxes and unique sequences':

   These functions query a given bbox and return the sequenceIDs therein.They keep dividing the bbox calling themselves till less than 2000 elements are returned
   
   "async_get_bboxes_and_sequences" does this using an asyncchronous event loop and is the most efficient sofar
   "get_bboxes_and_sequences" does this using for loops
   "get_bboxes_and_sequences_parallel" does this using an executor either a ProcessPoolExecutor or ThreadPoolExecutor
   
   "async_get_bboxes_and_sequences_higher_level" is meant to be used with "async_get_bboxes_and_sequences" and provides this with a "Session". You do not need this, if you provide a "session" in your code
   
   "async_get_bboxes_and_sequences_wrapping" as the name suggests, wraps "async_get_bboxes_and_sequences_higher_level" around. It runs the "async_get_bboxes_and_sequences_higher_level" asyncchronously and is meant to be used  if "async_get_bboxes_and_sequences_higher_level" is used within a ProcessPoolExecutor or ThreadPoolExecutor, as "coroutines" cannot be passed to these. 

