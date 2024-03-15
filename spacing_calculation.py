#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
# This is small script whose aim is to calculate distances between points and filter them based on that.

# 'distance_on_sphere_vectorized' is vectorized funtion to calculate the distance between two points. It
# is basically the haversine function and can also be used with points or two point arrays

# 'filter_points' will iterate over points and their distances to one another within a sequence , aggregate them
# filter out those that are less than the wanted distance

# 'calculate_spacing_vectorized' is a higher level function which will order the points based on their timestamp 
# first and then apply 'filter_points'
"""-----------------------------------Distance Functions------------------------------------------------""" 
def distance_on_sphere_vectorized(p1, p2):
    """
    p1 and p2 are two lists that have two elements. They are numpy arrays of the long and lat
    coordinates of the points in set1 and set2
        
    Calculate the distance between two points on the Earth's surface using the haversine formula.

    Args:
        p1 (list): Array containing the longitude and latitude coordinates of points FROM which the distance to be calculated in degree
        p2 (list): Array containing the longitude and latitude coordinates of points TO which the distance to be calculated in degree

    Returns:
        numpy.ndarray: Array containing the distances between the two points on the sphere in kilometers.

    This function computes the distance between two points on the Earth's surface using the haversine formula,
    which takes into account the spherical shape of the Earth. The input arrays `p1` and `p2` should contain
    longitude and latitude coordinates in degrees. The function returns an array containing the distances
    between corresponding pairs of points.
    """
    earth_radius = 6371  # km

    # Convert latitude and longitude to radians
    p1 = np.radians(np.array(p1))
    p2 = np.radians(np.array(p2))

    delta_lat = p2[1] - p1[1]
    delta_long = p2[0] - p1[0]

    a = np.sin(delta_lat / 2) ** 2 + np.cos(p1[1]) * np.cos(p2[1]) * np.sin(delta_long / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    distances = earth_radius * c
    return distances

"""-----------------------------------Filtering Points------------------------------------------------""" 
def filter_points(df, threshold_distance):
    """
    Filter points from a DataFrame based on a threshold distance.

    Args:
        df (pandas.DataFrame): DataFrame containing latitude and longitude columns.
        threshold_distance (float): Threshold distance for filtering points in kms.

    Returns:
        pandas.DataFrame: Filtered DataFrame containing selected points.
        float: Total road length calculated from the selected points.

    This function filters points from a DataFrame based on the given threshold distance. It calculates
    distances between consecutive points and accumulates them until the accumulated distance surpasses
    the threshold distance. It then selects those points and constructs a new DataFrame. Additionally,
    it manually checks the last point to include it if it satisfies the length condition. The function
    returns the filtered DataFrame along with the calculated road length.
    """
    road_length = 0
    mask = np.zeros(len(df), dtype=bool)
    mask[0] = True

    # Calculate distances between consecutive points
    lat = df['lat'].to_numpy()
    long = df['long'].to_numpy()

    distances = distance_on_sphere_vectorized([long[1:],lat[1:]],
                                              [long[:-1],lat[:-1]])
    road_length = np.sum(distances)

    
    #save the last point if the road segment is relavitely small (< 2*road_length)
    if threshold_distance <= road_length < 2 * threshold_distance:
        mask[-1] = True
    
    accumulated_distance = 0
    for i, distance in enumerate(distances):
        accumulated_distance += distance
        if accumulated_distance >= threshold_distance:
            mask[i+1] = True
            accumulated_distance = 0  # Reset accumulated distance
    

    to_be_returned_df = df[mask]
    # since the last point has to be omitted in the vectorized distance calculation, it is being checked manually
    p2 = to_be_returned_df.iloc[0]
    distance = distance_on_sphere_vectorized([float(p2["long"]),float(p2["lat"])],[long[-1],lat[-1]])
    
    #last point will be added if it suffices the length condition
    #last point will be added in case there is only one point returned
    if distance >= threshold_distance or len(to_be_returned_df) ==1: 
        to_be_returned_df = pd.concat([to_be_returned_df,pd.DataFrame(df.iloc[-1],columns=to_be_returned_df.columns)],axis=0)
    return to_be_returned_df, road_length


"""-----------------------------------Distance Calculations------------------------------------------------""" 
def calculate_spacing_vectorized(gdf, interval_length):
    """
    Calculate spacing between points in a GeoDataFrame.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame containing points with timestamps.
        interval_length (float): Interval length for filtering points in kms.

    Returns:
        geopandas.GeoDataFrame: Filtered GeoDataFrame containing selected points.
        float: Total road length calculated from the selected points.

    This function calculates the spacing between points in a GeoDataFrame by filtering points
    based on the provided interval length. It first sorts the GeoDataFrame by timestamp and
    then filters points using the filter_points function. The function returns the filtered
    GeoDataFrame along with the total road length.
    """
    road_length = 0 
    if len(gdf) == 1:
        return gdf, road_length
    sorted_sub_df = gdf.sort_values(by=['timestamp'])
    filtered_sorted_sub_df, road_length = filter_points(sorted_sub_df,interval_length,ignore_index=True)
    return filtered_sorted_sub_df, np.round(road_length, 3)
