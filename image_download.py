#!/usr/bin/env python
# coding: utf-8

import os
import datetime
import numpy as np
import pandas as pd
from fractions import Fraction

from concurrent.futures import ThreadPoolExecutor
import asyncio

import imageio.v3 as imageio
from PIL import Image
from PIL.ExifTags import TAGS
import piexif
import imgaug.augmenters as iaa

#########################################################################################################
#########################################################################################################
#########################################################################################################
#########################################################################################################
# This block implement some math functions relevant to the reading and writing of the metadata to and from
#image files

# 'to_deg' converts a decimal coordinate into degrees, minutes and seconds tuple
# 'to_coords' reverts this process
# 'change_to_rational' converts a number to a tuple of nominator and denominator
# 'rational_real' reverts this
"""----------------Some Math Functions for Metadata------------------------------------------"""
def to_deg(value, loc):
    """convert decimal coordinates into degrees, minutes and seconds tuple
    Keyword arguments: value is float gps-value, loc is direction list ["S", "N"] or ["W", "E"]
    return: tuple like (25, 13, 48.343 ,'N')
    """
    if value < 0:
        loc_value = loc[0]
    elif value > 0:
        loc_value = loc[1]
    else:
        loc_value = ""
    abs_value = abs(value)
    deg =  int(abs_value)
    t1 = (abs_value-deg)*60
    min = int(t1)
    sec = round((t1 - min)* 60, 5)
    return (deg, min, sec, loc_value)


def to_coords(deg, min, sec, loc_value):
    sec = sec/3600
    min = min/60
    abs_value = deg + min + sec
    
    if loc_value.decode("utf-8") ==  "S" or loc_value.decode("utf-8") == "W":
        return -abs_value
    if loc_value.decode("utf-8") ==  "N" or loc_value.decode("utf-8") == "E":
        return abs_value
    
def change_to_rational(number):
    """convert a number to rantional
    Keyword arguments: number
    return: tuple like (1, 2), (numerator, denominator)
    """
    f = Fraction(str(number))
    return (f.numerator, f.denominator)

def rational_real(x,y):
    return int(x)/int(y)

#########################################################################################################
#########################################################################################################
#########################################################################################################
#########################################################################################################
# This blocks aim is to implement two functions to write metadata to an image using PIEXIF TAGS and
# given such an image, to read the metadata from it

# 'write_to_metadata': no need to elaborate on this further
# 'read_from_metadata': as above
"""-------------------------------------Writing to and Reading From Image Metadata----------------------"""
def write_to_metadata(file_name, image_info):
    """Adds GPS position and other metadata as EXIF metadata
    Keyword arguments:
    file_name -- image file
    image_info -- a tuple containing the following information:
                  sequence -- Sequence ID
                  id -- Image ID
                  coordinates -- Tuple containing (latitude, longitude, altitude)
                  url -- URL
                  is_pano -- Is panoramic (boolean)
                  timestamp -- Timestamp (integer)
    Returns:
    True if successful, False otherwise
    """
    try:
        sequence, id_, coordinates, url, is_pano, timestamp = image_info
        lat, lng, alt = coordinates

        lat_deg = to_deg(lat, ["S", "N"])
        lng_deg = to_deg(lng, ["W", "E"])

        exiv_lat = (change_to_rational(lat_deg[0]), change_to_rational(lat_deg[1]), change_to_rational(lat_deg[2]))
        exiv_lng = (change_to_rational(lng_deg[0]), change_to_rational(lng_deg[1]), change_to_rational(lng_deg[2]))

        # Convert integer timestamp to datetime object
        timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp/1000)

        zeroth_ifd = {
            piexif.ImageIFD.ImageDescription: sequence,
            piexif.ImageIFD.Model: str(id_),
            piexif.ImageIFD.Software: url,
            piexif.ImageIFD.Make: str(is_pano),  # Using Make tag for is_pano
            piexif.ImageIFD.DateTime: timestamp_dt.strftime("%Y:%m:%d %H:%M:%S")
        }

        gps_ifd = {
            piexif.GPSIFD.GPSVersionID: (2, 0, 0, 0),
            piexif.GPSIFD.GPSLatitudeRef: lat_deg[3],
            piexif.GPSIFD.GPSLatitude: exiv_lat,
            piexif.GPSIFD.GPSLongitudeRef: lng_deg[3],
            piexif.GPSIFD.GPSLongitude: exiv_lng,
            piexif.GPSIFD.GPSAltitude: (int(alt), 1),
            piexif.GPSIFD.GPSAltitudeRef: (0 if alt >= 0 else 1)  # Setting altitude reference
        }

        exif_dict = {"0th": zeroth_ifd, "GPS": gps_ifd}

        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, file_name)
        return True
    except Exception as err:
        print(err)
        return False

def read_from_metadata(file_name):
    """Reads GPS position and other metadata from EXIF metadata.
    Keyword arguments:
    file_name -- image file
    Returns:
    A list containing the following information in the same order as 'write_to_metadata':
    - sequence_id
    - img_id
    - coordinates: Tuple containing (latitude, longitude, altitude)
    - url
    - is_pano (boolean)
    - timestamp (integer)
    """
    try:
        exif_data = piexif.load(file_name)
        zero_th = exif_data["0th"]
        GPS = exif_data["GPS"]

        sequence = zero_th.get(piexif.ImageIFD.ImageDescription, None)
        id_ = zero_th.get(piexif.ImageIFD.Model, None)
        url = zero_th.get(piexif.ImageIFD.Software, None)
        is_pano = zero_th.get(piexif.ImageIFD.Make, None)  # Reading is_pano from Make tag
        timestamp_str = zero_th.get(piexif.ImageIFD.DateTime, None)

        lat_deg_hemisphere, lat, lng_deg_hemisphere, lng, alt, alt_ref = [GPS.get(key, None) for key in [
            piexif.GPSIFD.GPSLatitudeRef, piexif.GPSIFD.GPSLatitude,
            piexif.GPSIFD.GPSLongitudeRef, piexif.GPSIFD.GPSLongitude,
            piexif.GPSIFD.GPSAltitude, piexif.GPSIFD.GPSAltitudeRef
        ]]

        if alt_ref is not None:
            alt_ref = alt_ref[0]  # Extracting the altitude reference

        if lat is not None:
            deg_lat, min_lat, sec_lat = lat
            lat = to_coords(rational_real(deg_lat[0], deg_lat[1]), rational_real(min_lat[0], min_lat[1]),
                            rational_real(sec_lat[0], sec_lat[1]), lat_deg_hemisphere)

        if lng is not None:
            deg_lng, min_lng, sec_lng = lng
            lng = to_coords(rational_real(deg_lng[0], deg_lng[1]), rational_real(min_lng[0], min_lng[1]),
                            rational_real(sec_lng[0], sec_lng[1]), lng_deg_hemisphere)

        if alt is not None:
            e, f = alt
            alt = e / f

        if timestamp_str:
            timestamp = datetime.datetime.strptime(timestamp_str, "%Y:%m:%d %H:%M:%S")
            timestamp_int = int(timestamp.timestamp())  # Convert datetime to integer timestamp
        else:
            timestamp_int = None

        data = [sequence, id_, [lng, lat, alt], url, is_pano, timestamp_int]
        decoded_data = [item.decode("utf-8") if isinstance(item, bytes) else item for item in data]

        return decoded_data

    except Exception:
        return None
    
#########################################################################################################
#########################################################################################################
#########################################################################################################
#########################################################################################################
# This block here is all about downloading the image, resizing and saving it
# 'get_image' as the name suggests, this mf here downloads the image and resize it
# 'save_image' this here writes the metadata to the images, resized and original and saves them
# 'process_image' this the higher level function coordinating both
"""-------------------------------------Image Download and Processsing----------------------"""
def get_image(url,size,call_limit=5):
    """
    Fetches an image from a given URL and resizes it.

    Args:
    - url (str): The URL from which to fetch the image.
    - size (tuple): A tuple specifying the desired height and width of the image after resizing.
    - call_limit (int): The maximum number of retries allowed for fetching and resizing the image.

    Returns:
    - tuple: A tuple containing the original image and the resized image. If fetching or resizing fails,
             returns (None, None).
    """
    
    image_aug = None
    image = None
    checker = 0
    checker_ = 0 
    while checker < call_limit and checker_ < call_limit:
        try:
            image = imageio.imread(url)
        except:
            checker += 1
            continue
    
        try:
            resize = iaa.Resize({"height": size[0], "width": size[1]})
            image_aug = resize(image=image)
        except:
            checker_ += 1
            continue
        break
    if isinstance(image, np.ndarray) and isinstance(image_aug, np.ndarray):
        return image, image_aug
    else:
        return None, None

def save_image(name_org,name_resized,image_org,image_resized,image_info_org,image_info_resized):
    """
    Saves the original and resized images to disk and writes metadata to them.

    Args:
    - name_org (str): The filename (without extension) for the original image.
    - name_resized (str): The filename (without extension) for the resized image.
    - image_org (numpy.ndarray): The original image data.
    - image_resized (numpy.ndarray): The resized image data
    - image_info_org: Information or metadata associated with the original image.
    - image_info_resized: Information or metadata associated with the resized image.

    Returns:
    - bool: True if both images are saved successfully and metadata is written successfully, 
            False otherwise.
    """
    if isinstance(image_org,np.ndarray) and isinstance(image_resized, np.ndarray):
       try: 
            image_org = Image.fromarray(image_org)
            image_resized = Image.fromarray(image_resized)
                
            image_org.save(f'{name_org}.jpg')
            image_resized.save(f'{name_resized}.jpg')
        
            cond1 = write_to_metadata(f'{name_org}.jpg',image_info_org)
            cond2 =  write_to_metadata(f'{name_resized}.jpg',image_info_resized)
    
            if cond1 and cond2:
                return True
            else:
                os.remove(f'{name_org}.jpg')
                os.remove(f'{name_resized}.jpg')
                return False
       except Exception as err:
            return False
    else:
        return False
def process_image(row,original_dir,resized_dir,image_size,all_images):
    """
    Process images from a DataFrame, resize them, and save the original and resized versions.

    Args:
    - df (pd.DataFrame): The DataFrame containing image information.
    - original_dir (str): The directory path where original images will be saved.
    - resized_dir (str): The directory path where resized images will be saved.
    - image_size (tuple): A tuple specifying the desired height and width of the resized image.

    Returns:
    - list: A list of boolean values indicating whether each image was processed and saved successfully.
    """
    sequence = row['sequence']
    id_ = row['id']
    url = row['url']
    long = row['long']
    lat = row['lat']
    alt = row['altitude']
    is_pano = row['is_pano']
    timestamp = row['timestamp']
    image_info = [sequence, id_, [long, lat, alt], url, is_pano,timestamp]
    name = str(id_)

    if f"{name}.jpg" in all_images:
        return True
        
    returned = get_image(url,image_size,True)
    condition = False
    if returned[0] is not None and returned[1] is not None:
        img_original, img_resized = returned
        image_filename_original = f'{original_dir}/missing/{name}'
        image_filename_resized = f'{resized_dir}/missing/{name}'
            
        condition = save_image(image_filename_original,image_filename_resized,img_original,
                                        img_resized,image_info,image_info)
    return condition
            
#########################################################################################################
#########################################################################################################
#########################################################################################################
#########################################################################################################
# Et Actio: is the block where the main function is located. Given a .csv file, it downloads, resizes, saves
# and adds the metadata to an image featuring a ThreadPoolExecutor to speed up the process. Info of the images
# that could not be saved will be written to the folder where the input files are located.   

# 'main' does this using a ThreadPoolExecutor synchronously
# 'async_main' does the same job using an eventloop. I haven't tried this yet but it can be an improvement
# However, no improvement is actually needed, since the script is already efficient as it is.  

"""-------------------------------------Et Actio----------------------"""
def main(dir, image_size, path_to_metadata, metadata_file, missing_images,max_workers=4):
    df = pd.read_csv(metadata_file)
    
    original_dir = f'{dir}/originals'
    if not os.path.exists(original_dir):
        os.makedirs(original_dir, exist_ok=True)
        
    resized_dir = f'{dir}/resized'
    if not os.path.exists(resized_dir):
        os.makedirs(resized_dir, exist_ok=True)

    all_images = set(os.listdir(original_dir))
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, row in df.iterrows():
            future = executor.submit(process_image, row, original_dir, resized_dir, image_size,all_images)
            futures.append(future)
        
    results = [future.result() if future.result() is not None else True for future in futures]
    if len(results) == len(df):
        not_downloaded_images = df[~np.array(results)]
        not_downloaded_images.to_csv(f"{path_to_metadata}/{missing_images}",index=False)
        
async def async_main(dir, image_size, path_to_metadata, metadata_file, missing_images, max_workers=4):
    df = pd.read_csv(metadata_file)
    
    original_dir = f'{dir}/originals'
    if not os.path.exists(original_dir):
        os.makedirs(original_dir, exist_ok=True)
        
    resized_dir = f'{dir}/resized'
    if not os.path.exists(resized_dir):
        os.makedirs(resized_dir, exist_ok=True)

    all_images = set(os.listdir(original_dir))
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = [loop.run_in_executor(executor, process_image, row, original_dir, resized_dir, image_size, all_images) for index, row in df.iterrows()]
        results = await asyncio.gather(*tasks)
    
    if len(results) == len(df):
        not_downloaded_images = df[~np.array(results)]
        not_downloaded_images.to_csv(f"{path_to_metadata}/{missing_images}", index=False)
        
"""---------------------------------------------------------------------------------"""
if __name__ == '__main__':
    
    status = None
    dir = None
    urban_other = None

    continent = 'asia'
    continent_code = "ASIA"
    
    max_cpu_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', os.cpu_count()))
    max_workers = 3*2*max_cpu_workers
    
    interval_length = 100
    filtered = "filtered"
    #filtered = 'unfiltered'
    urban_other = 'urban'
    #urban_other = 'other'
    
    if filtered == 'filtered':
        status = f'metadata_{urban_other}_areas_{filtered}_{interval_length}_{continent_code}'
        dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{urban_other}/{filtered}/{interval_length}m'
    else:
        if urban_other is not None:
            status = f'metadata_{filtered}_{urban_other}_{continent_code}'
            dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{urban_other}/{filtered}/'
        else:
            status = f'metadata_{filtered}_{continent_code}'
            dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{filtered}/'
    
    #dir = os.getcwd()
    image_size = [256,427]
    file = int(str(__file__).split('/')[-1].split('.')[0])
    missing_images = f'missing_images_{urban_other}.csv'
    path_to_metadata = f'/mnt/sds-hd/sd17f001/eren/metadata_manipulation_new/{continent}/processed_metadata/{filtered}'
    #path_to_metadata = os.getcwd()
    metadata_file = f"{path_to_metadata}/{status}_{file}.csv"
    #main(dir,image_size,path_to_metadata,metadata_file,missing_images,max_workers)
    asyncio.run(async_main(dir, image_size, path_to_metadata, metadata_file, missing_images, max_workers))
    
    interval_length = 1000
    filtered = "filtered"
    #filtered = 'unfiltered'
    #urban_other = 'urban'
    urban_other = 'other'

    if filtered == 'filtered':
        status = f'metadata_{urban_other}_areas_{filtered}_{interval_length}_{continent_code}'
        dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{urban_other}/{filtered}/{interval_length}m'
    else:
        if urban_other is not None:
            status = f'metadata_{filtered}_{urban_other}_{continent_code}'
            dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{urban_other}/{filtered}/'
        else:
            status = f'metadata_{filtered}_{continent_code}'
            dir = f'/mnt/sds-hd/sd17f001/eren/final_continents/{continent}/{filtered}/'
            
    #dir = os.getcwd()
    image_size = [256,427]
    file = int(str(__file__).split('/')[-1].split('.')[0])
    missing_images = f'missing_images_{urban_other}.csv'
    path_to_metadata = f'/mnt/sds-hd/sd17f001/eren/metadata_manipulation_new/{continent}/processed_metadata/{filtered}'
    #path_to_metadata = os.getcwd()
    metadata_file = f"{path_to_metadata}/{status}_{file}.csv"
    #main(dir,image_size,path_to_metadata,metadata_file,missing_images,max_workers)
    asyncio.run(async_main(dir, image_size, path_to_metadata, metadata_file, missing_images, max_workers))
   