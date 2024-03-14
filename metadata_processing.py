import pandas as pd
import numpy as np
import json
import time
import requests
from shapely import Point
import asyncio
import aiohttp

#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
# There are actually three functions here with each function having two differnt implementations
# One is for normal implementations which will be used by functions in "metadata_download.py"
# which implement synchronous dealing with the Mapillary API. The other is the asychronous
# implementation with the same functionality to be used default. 

# "async_get_response" and "get_response" extract the respond from the Mapillary API
# and and returns this unless the server has responded  "call_limit" times with None, then
# they return None

# 'async_adding_images_to_each_other' and 'adding_images_to_each_other' add image metadata from
# different calls together and terminates the loop when no more new image data is added after 
# 'call_limit_images' times.

# 'process_one_sequence' and 'async_process_one_sequence' orchestrate the download of the image
# metadata for a single sequenceID

"""---------------------------Calling the API--------------------------------------"""
async def async_get_response(url, session, call_limit=100, base_sleep=0.01, rate_limit_sleep=0.05,waiting_time=1):
    attempt = 0
    exception_attempt = 0
    rate_limit_attempt = 0
    while True:
        try:
            async with session.get(url) as response:
                if response is not None:
                    if response.status == 200:
                        json = await response.json()
                        return response
                    else:
                        await asyncio.sleep(min(rate_limit_sleep * 2**rate_limit_attempt,waiting_time)) # Exponential backoff for rate limit errors
                        rate_limit_attempt += 1
                else:
                    await asyncio.sleep(min(base_sleep * 2**attempt,waiting_time))  # Exponential backoff
                    attempt += 1
        except Exception:
            if exception_attempt == call_limit:
                break
            else:
                await asyncio.sleep(min(base_sleep * 2**exception_attempt,waiting_time))  # Exponential backoff for other exceptions
                exception_attempt += 1
    return None

def get_response(url, call_limit=5, base_sleep=0.05, rate_limit_sleep=0.1):
    """
    Attempts to make a GET request to the specified URL with retry logic.
    
    Parameters:
        url (str): The URL to which the GET request will be made.
        call_limit (int): The maximum number of attempts to make the request (default is 5).
        base_sleep (float): The initial sleep time in seconds between retry attempts for other errors (default is 0.05).
        rate_limit_sleep (float): The initial sleep time in seconds between retry attempts for rate limit errors (default is 0.1).
        
    Returns:
        response (requests.Response) or None: 
            If a successful response (status code 200) is received, returns the response object.
            If all attempts fail, returns None.
    """
    attempt = 0
    rate_limit_attempt = 0
    while attempt < call_limit:
        try:
            response = requests.get(url)
            if response is not None and response.status_code == 200:
                return response
            elif response.status_code == 429:  # Check for rate limit error
                error_message = response.json().get('error', {}).get('message', '')
                if "Application request limit reached" in error_message:
                    time.sleep(rate_limit_sleep * 2**rate_limit_attempt)# Exponential backoff for rate limit errors
                    rate_limit_attempt += 1  # Skip incrementing attempt
            # Increment attempt for other cases
            time.sleep(base_sleep * 2**attempt)  # Exponential backoff
            attempt += 1
        except Exception:
            time.sleep(base_sleep * 2**attempt)  # Exponential backoff for other exceptions
            attempt += 1
    return None

"""---------------------------Adding image info from different Calls together--------------------------------------"""
async def async_adding_images_to_each_other(sequence,url,session,call_limit_images=10,call_limit=5,base_limit=1):
    '''
    This function aims at downloading the metadata of a sequence. To ensure all points from a sequence are downloaded due 
    to the API limit of maximum 2000 elements for each call,it calls the Mapillary API consecutively and extracts the ids
    of the images and adds the unique ones from different calls to the list "metadata_images_global". If no new ids are 
    added after "call_limit_images" if one the calls have had 2000 elements or after "round(call_limit_images/2)" if none 
    have had 2000 elements, the function terminates. There are then other conditions given by "call_limit" to terminate
    the loop, when the server doesn't respond or it does but the response isn't meaningful or when there is no data in
    the response contrary to what is expected or when the returned data doesn't include any information
   
    Parameters:
    
        sequence (str): a Mapillary road sequence
        url (str) : url to the sequence info
        call_limit_images (int) : the number of steps to terminate the loop if there is no new ids added (default is 10).
        call_limit (int) : number of step to terminate if the server doesn't respond in a meaningful way (default is 5).
        
    Returns:
    
        metadata_images_global (pd.DataFrame) : metadata of the images of the respective sequence as a pandas DataFrame if the download
                                 was successfull else an empty DataFrame
        sequence_info (str) : None if the download was succesfull else sequence itself else for further processing
    '''
                    
    metadata_images_global = []
    set_ = set() #to check if there are new ids added from each call
    checker = 0 #to break the loop after "call_limit" times if the server is response is  None or invalid
    checker_ = 0 #to break the loop after "call_limit" times if the response is positive but there is  no data in the response
    checker__ = 0 #to break the loop after "call_limit" times if the data in the returned json contains no element 
    checker___ = 0 #to break the loop after "call_limit" times if the returned data contains 2000 (limit) elements and no new images
    checker____ = 0
    number_of_elements_from_each_call = set()
    while True:
        #------------------------checking the response is meaningful------------------------------------------------#
        if checker___ == call_limit:
            break
        
        response = await async_get_response(url,session)
        if response is None:
            await asyncio.sleep(min(0.01*2**checker,base_limit))
            checker+=1
            continue
        
        if response.status != 200:
            await asyncio.sleep(min(0.01*2**checker_,base_limit))
            checker_+=1
            continue
        
        json = await response.json()
        if 'data' not in json:
            await asyncio.sleep(min(0.01*2**checker__,base_limit))
            checker__+=1
            continue
        
        data = json['data']
        if not len(data):
            await asyncio.sleep(min(0.01*2**checker___,base_limit))
            checker___+=1
            continue
        #---------------------ensuring the metadata contains the columns------------------------------------------------#
        data = pd.DataFrame(data)
        number_of_elements_from_each_call.add(len(data))
        #Extracting meaningful columns and getting the unique ids
        df_filtered = data[data.apply(lambda x: all(k in x for k in ["id", "thumb_original_url", "computed_geometry", "captured_at"]), axis=1)]
        df_filtered = df_filtered[~df_filtered['id'].isin(set_)]
        
        if len(df_filtered): #checking whether there is new ids
            metadata_images_global.append(df_filtered) #uploading the global list
            set_.update(df_filtered['id']) #adding unique ids to the set
            checker____ = 0 ##resetting the call_limit_images
            
        else:
           if 2000 in number_of_elements_from_each_call: #if there has been 2000 elements in a call, this will be assumed for the rest of the calls
               if checker____ == call_limit_images:
                   break
           else:
               if checker____ == round(call_limit_images/2):#if not, half of the call_limit_images will suffice to terminate
                   break
           checker____ += 1
           
    if len(metadata_images_global):
        return pd.concat(metadata_images_global), None
    else:
        return pd.DataFrame(metadata_images_global), sequence
    
def adding_images_to_each_other(sequence,url,call_limit_images=10,call_limit=5,base_limit=1):
    '''
    This function aims at downloading the metadata of a sequence. To ensure all points from a sequence are downloaded due 
    to the API limit of maximum 2000 elements for each call,it calls the Mapillary API consecutively and extracts the ids
    of the images and adds the unique ones from different calls to the list "metadata_images_global". If no new ids are 
    added after "call_limit_images" if one the calls have had 2000 elements or after "round(call_limit_images/2)" if none 
    have had 2000 elements, the function terminates. There are then other conditions given by "call_limit" to terminate
    the loop, when the server doesn't respond or it does but the response isn't meaningful or when there is no data in
    the response contrary to what is expected or when the returned data doesn't include any information
    Parameters:
        sequence (str): a Mapillary road sequence
        url (str) : url to the sequence info
        call_limit_images (int) : the number of steps to terminate the loop if there is no new ids added (default is 10).
        call_limit (int) : number of step to terminate if the server doesn't respond in a meaningful way (default is 5).
    Returns:
    
        metadata_images_global (pd.DataFrame) : metadata of the images of the respective sequence as a pandas DataFrame if the download
                                 was successfull else an empty DataFrame
        sequence_info (str) : None if the download was succesfull else sequence itself else for further processing
    '''
                    
    metadata_images_global = []
    set_ = set() #to check if there are new ids added from each call
    checker = 0 #to break the loop after "call_limit" times if the server is response is  None or invalid
    checker_ = 0 #to break the loop after "call_limit" times if the response is positive but there is  no data in the response
    checker__ = 0 #to break the loop after "call_limit" times if the data in the returned json contains no element 
    checker___ = 0 #to break the loop after "call_limit" times if the returned data contains 2000 (limit) elements and no new images
    checker____ = 0
    number_of_elements_from_each_call = set()
    while True:
        #------------------------checking the response is meaningful------------------------------------------------#
        if checker___ == call_limit:
            break
        
        response = get_response(url)
        if response is None:
            time.leep(min(0.01*2**checker,base_limit))
            checker+=1
            continue
        
        if response.status != 200:
            time.sleep(min(0.01*2**checker_,base_limit))
            checker_+=1
            continue
        
        json = response.json()
        if 'data' not in json:
            time.sleep(min(0.01*2**checker__,base_limit))
            checker__+=1
            continue
        
        data = json['data']
        if not len(data):
            time.sleep(min(0.01*2**checker___,base_limit))
            checker___+=1
            continue
        #---------------------ensuring the metadata contains the columns------------------------------------------------#
        data = pd.DataFrame(data)
        number_of_elements_from_each_call.add(len(data))
        #Extracting meaningful columns and getting the unique ids
        df_filtered = data[data.apply(lambda x: all(k in x for k in ["id", "thumb_original_url", "computed_geometry", "captured_at"]), axis=1)]
        df_filtered = df_filtered[~df_filtered['id'].isin(set_)]
        
        if len(df_filtered): #checking whether there is new ids
            metadata_images_global.append(df_filtered) #uploading the global list
            set_.update(df_filtered['id']) #adding unique ids to the set
            checker____ = 0 ##resetting the call_limit_images
        else:
           if 2000 in number_of_elements_from_each_call: #if there has been 2000 elements in a call, this will be assumed for the rest of the calls
               if checker____ == call_limit_images:
                   break
           else:
               if checker____ == round(call_limit_images/2):#if not, half of the call_limit_images will suffice to terminate
                   break
           checker____ += 1
           
    if len(metadata_images_global):
        return pd.concat(metadata_images_global), None
    else:
        return pd.DataFrame(metadata_images_global), sequence


def process_one_sequence(sequence, mly_key, call_limit_images=10, call_limit=5,base_limit=1):
    '''
    This function processes one sequence of data. It initializes a DataFrame
    calls the "adding_images_to_each_other()" with the respective sequence, returns 
    the metadata of the images within that sequence as well as "sequence_info" telling whether
    the download of the metadata successful(None) or there were problems(sequence itself) for further processing.
    The function then updates the initialized DataFrame with the metadata, creates "long" and "lat" columns,
    populates missing columns with NaN values, renames and returns them with the "sequence_info". If no data is contained
    in the metadata, the initialized DataFrame will be renamed and returned together with the "sequence_info" 

    Parameters:
    
        sequence (str): a Mapillary road sequence
        mly_key (str) : Mapillary Key
        call_limit (int): call limit for  "adding_images_to_each_other()" after which the function terminates (default is 5).
        call_limit_images (int): the number of steps to terminate the loop if there is no new ids added (default is 10).
        
    Returns:
    
        df_data : metadata of the images of the respective sequence as a pandas DataFrame if the download was successfull
                    else an empty DataFrame
        sequence_info : None if the adding_images_to_each_other() downloads the metadata successfully
                        sequence itself else for further processing
    '''

    df_data = pd.DataFrame(columns=['sequence', 'id', 'thumb_original_url', 'long', 'lat', 'computed_geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'captured_at'])
    url = f'https://graph.mapillary.com/images?access_token={mly_key}&sequence_ids={[sequence]}&fields=height,width,computed_geometry,altitude,id,thumb_original_url,make,model,creator,is_pano,captured_at'
    meta_data_images, sequence_info = adding_images_to_each_other(sequence, url,call_limit_images=call_limit_images, call_limit=call_limit,base_limit=base_limit)
    if len(meta_data_images) > 0:
        df_data = pd.DataFrame(meta_data_images)
        
        df_data['sequence'] = sequence
        df_data['geometry'] = df_data['computed_geometry'].apply(lambda x: Point(x.get('coordinates')) if isinstance(x, dict) else np.nan)
        df_data['long'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[0] if isinstance(x, dict) else np.nan)
        df_data['lat'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[1] if isinstance(x, dict) else np.nan)
        
        missing_columns = ['sequence', 'id', 'thumb_original_url', 'long', 'lat', 'computed_geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'captured_at']
        for col in missing_columns:
            if col not in df_data.columns:
                if col == "is_pano":
                     df_data[col] = False
                else:
                    df_data[col] = np.nan
                    
        df_data.drop(['computed_geometry'], inplace=True, axis=1)
        
    else:
        df_data.rename({'computed_geometry':'geometry'}, inplace=True, axis=1)
        
    df_data.rename({'thumb_original_url': 'url', 'captured_at': 'timestamp'}, axis=1, inplace=True)
    df_data = df_data[['sequence', 'id', 'url', 'long', 'lat', 'geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'timestamp']]
    return df_data, sequence_info

async def async_process_one_sequence(sequence, mly_key,session, call_limit_images=10, call_limit=5,base_limit=1):
    '''
    This function processes one sequence of data. It initializes a DataFrame
    calls the "adding_images_to_each_other()" with the respective sequence, returns 
    the metadata of the images within that sequence as well as "sequence_info" telling whether
    the download of the metadata successful(None) or there were problems(sequence itself) for further processing.
    The function then updates the initialized DataFrame with the metadata, creates "long" and "lat" columns,
    populates missing columns with NaN values, renames and returns them with the "sequence_info". If no data is contained
    in the metadata, the initialized DataFrame will be renamed and returned together with the "sequence_info" 
   
    Parameters:
    
        sequence (str): a Mapillary road sequence
        mly_key (str) : Mapillary Key
        call_limit (int): call limit for  "adding_images_to_each_other()" after which the function terminates (default is 5).
        call_limit_images (int): the number of steps to terminate the loop if there is no new ids added (default is 10).
        
    Returns:
    
        df_data : metadata of the images of the respective sequence as a pandas DataFrame if the download was successfull
                    else an empty DataFrame
        sequence_info : None if the adding_images_to_each_other() downloads the metadata successfully
                        sequence itself else for further processing
    '''

    df_data = pd.DataFrame(columns=['sequence', 'id', 'thumb_original_url', 'long', 'lat', 'computed_geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'captured_at'])
    url = f'https://graph.mapillary.com/images?access_token={mly_key}&sequence_ids={[sequence]}&fields=height,width,computed_geometry,altitude,id,thumb_original_url,make,model,creator,is_pano,captured_at'
    meta_data_images, sequence_info = await async_adding_images_to_each_other(sequence, url,session,call_limit_images=call_limit_images, call_limit=call_limit,base_limit=base_limit)
    if len(meta_data_images) > 0:
        df_data = pd.DataFrame(meta_data_images)
        
        df_data['sequence'] = sequence
        df_data['geometry'] = df_data['computed_geometry'].apply(lambda x: Point(x.get('coordinates')) if isinstance(x, dict) else np.nan)
        df_data['long'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[0] if isinstance(x, dict) else np.nan)
        df_data['lat'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[1] if isinstance(x, dict) else np.nan)
        
        missing_columns = ['sequence', 'id', 'thumb_original_url', 'long', 'lat', 'computed_geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'captured_at']
        for col in missing_columns:
            if col not in df_data.columns:
                if col == "is_pano":
                     df_data[col] = False
                else:
                    df_data[col] = np.nan
                    
        df_data.drop(['computed_geometry'], inplace=True, axis=1)
        
    else:
        df_data.rename({'computed_geometry':'geometry'}, inplace=True, axis=1)
        
    df_data.rename({'thumb_original_url': 'url', 'captured_at': 'timestamp'}, axis=1, inplace=True)
    df_data = df_data[['sequence', 'id', 'url', 'long', 'lat', 'geometry', 'height', 'width', 'altitude', 
                       'make', 'model', 'creator', 'is_pano', 'timestamp']]
    return df_data, sequence_info


