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
