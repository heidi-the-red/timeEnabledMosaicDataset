# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# ALClasses.py
# Created on: 2016-04-15
# Description: Utility classes for handling mosaic datasets
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#
# THIS CODE CANNOT RUN IN MULTITHREADED MODE AS ANY TEMPORARY TABLES OR
# FEATURES CLASSES WILL CONFLICT.
#
# ---------------------------------------------------------------------------

# Import arcpy module
import arcpy
import os
import logging
import datetime
import time
import pandas as pd
import subprocess
import tempfile


# Global defaults
log = False
std_out = True
WMAS_ProjCS = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]];-20037700 -30241100 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"



def timer(in_process):
    """This function is used as a decorator in order to get the execution time of a function

    Example usage:

    @timer                
    def do_the_work(self):
        lg = logging.getLogger(self.mosaic_name)
        self.create()
        self.delete()

    Parameters
    ----------
    in_process : function
        function to get the execution time of
    """
    def wrapper_timer(*args, **kwargs):
        """
        Parameters
        ----------
        *args : tuple
            positional arguments from in_process function
        *kwargs : dict
            key word arguments from in_process function
        """

        with Timer(f"{in_process.__name__!r}") as timer:
            value = in_process(*args, **kwargs)
        return value
    return wrapper_timer


def output_msg(msg):
    """Sends msg to the log and also prints the geoprocessing messages
    from the most recently called tool (which includes msg)

    Parameters
    ----------
    msg : str
        The message to be outputted
    """
    if log:
        logging.info('%s', msg)
    if std_out:
        arcpy.AddMessage(msg)
        print(arcpy.GetMessages())
        
def set_logging(log_path):
    """Sets up the logger

    Parameters
    ----------
    log_path : str
        The path to create (if it doesn't exist
        already) where the log file will reside
    """
    if log:
        os.makedirs(log_path, exist_ok=True)
        file_name = f'{str(datetime.date.today())}.log'
        log_file = os.path.join(log_path, file_name)
        logging.basicConfig(filename=log_file, format='%(levelname)s : %(message)s', level=logging.INFO)
        return log_file



def decode_names (geodatabase_name, table_name=None):
    """returns the geodatabase path where the table (or it 
    might be a mosaic dataset or something else) is,
    the table name, and the full path (geodatabase path + table name)

    Parameters
    ----------
    table_name : str
        the table name (or it might be mosaic dataset name, 
        etc. depending on where this function was called)
    """
    if table_name == None:
        local_geodatabase_name = os.path.dirname(geodatabase_name)
        local_table_name = os.path.basename(geodatabase_name)
    else:
        local_geodatabase_name = geodatabase_name
        local_table_name = table_name
    if local_geodatabase_name == "":
        local_full_name = local_table_name
    else:
        local_full_name = os.path.join(local_geodatabase_name,local_table_name)

    return (local_geodatabase_name, local_table_name, local_full_name)



def parse_table_name(table_name, replacechars=None):
    """replaces characters in table_name with what they
    are mapped to in the replacechars dictionary

    Parameters
    ----------
    table_name : str
        the table name (or the geodatabase name, etc. depending
        on where initialize_names() was called from)

    replacechars : dict
        dictionary that holds character mappings
    """
    if replacechars == None:
        return table_name
    else:
        return "".join([replacechars.get(ch, ch) for ch in table_name])


def initialize_names(geodatabase_name, table_name=None, temptable=False, scratch=None, replacechars=None):
    """returns the geodatabase path where the table (or it 
    might be a mosaic dataset or something else) is,
    the table name, and the full path (geodatabase path + table name)

    Parameters
    ----------
    geodatabase_name : str

    table_name : str
        the table name (or the geodatabase name, etc. depending
        on where initialize_names() was called from)

    temptable : bool
        Whether we are working with a temporary table or not

    scratch : str
        The default geodatabase if no geodatabase is specified

    replacechars : dict
        dictionary that holds character mappings
    """
    # Was this just a table name or a GDB and table name
    if temptable == True: # Q: Why are you checking if temptable is true?
        local_geodatabase_name, local_table_name, local_full_name = decode_names(geodatabase_name, table_name)

        if local_geodatabase_name == "":
            local_geodatabase_name = arcpy.env.scratchGDB if scratch==None else scratch

        local_table_name = parse_table_name(local_table_name,replacechars)

        return decode_names(arcpy.CreateUniqueName(local_table_name, local_geodatabase_name))
    else:
        local_table_name = parse_table_name(table_name,replacechars) if table_name else None
        return decode_names(geodatabase_name, local_table_name)






class Timer (object):
    """
        Context manager as a python timer

        example usage:

        with Timer("Test") as timer: 
            for i in range(1000000):
                x = 0
                pass
    """

    def __init__(self, output_string = None): 
        self.start = None
        self.output_string = output_string
           
    def __enter__(self):
        """
            Notes the time at the start of the iteration
        """
        self.start = time.time()
        return self
       
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
            Prints the time taken at the end of the iteration
        """
        end_time = time.time()
        if self.output_string:
            print(f"{self.output_string}: ", end="")
        print("Time taken to finish:", self.report_elapsed_time(end_time-self.start))

    @staticmethod
    def report_elapsed_time(elapsed_time):
        """Return a human readable string for time difference provided.
        
        Parameters
        ----------
        elapsed_time : int
            time (in seconds) that you want to convert to a human readable format
        """
        seconds_time = int(elapsed_time % 60)
        seconds_string = "1 second" if seconds_time == 1 else "{} seconds".format(seconds_time)
        if elapsed_time < 60:
            return "{}".format(seconds_string)

        minutes_time = int((elapsed_time % 3600) / 60)
        minutes_string = "1 minute" if minutes_time == 1 else "{} minutes".format(minutes_time)
        if elapsed_time < 3600:
            return "{} {}".format(minutes_string, seconds_string)

        hours_time = int(elapsed_time / 3600)
        hours_string = "1 hour" if hours_time == 1 else "{} hours".format(hours_time)
        return "{} {} {}".format(hours_string, minutes_string, seconds_string)



class Table (object):
    """
    Attributes
    ----------
    geodatabase_name : str
        geodatabase path where the table is stored
    full_name : str
        geodatabase_name + table_name
    temp_table : bool
        Whether or not the table is a temporary table
    table_name : str
    """
    def __init__(self, geodatabase_name, table_name=None, temp_table=False):
        replace_list = {
            " ": "_",
            ".": ""
            }
        self.geodatabase_name = None
        self.table_name = None
        self.full_name = None
        self.temp_table = temp_table

        # Was this just a table name or a GDB and table name
        (self.geodatabase_name, self.table_name, self.full_name) = initialize_names (geodatabase_name, table_name, temp_table, arcpy.env.scratchGDB, replace_list)
        if self.temp_table == True:
            if self.delete() == False:
                raise arcpy.ExecuteError


    def table_to_dataframe(self, field_names, **kwargs):
        numpy_array = arcpy.da.TableToNumPyArray(self.full_name, field_names, **kwargs)
        ret_val = pd.DataFrame(numpy_array)
        output_msg('Converted table {0} to dataframe'.format(self.full_name))
        return ret_val

    def parse_table_name(self, table_name):
        return table_name.replace(" ", "_").replace(".","")

    def createTable(self,template=None, **kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/create-table.htm"""
        ret_val = arcpy.CreateTable_management(self.geodatabase_name, self.table_name, template, **kwargs)
        output_msg('Created table {0} in {1}'.format(self.table_name, self.geodatabase_name))
        return ret_val

    def describe(self):
        """Returns an object with properties detailing the data element described. Some
        of the returned object's properties will contain literal values or objects."""
        ret_val = arcpy.Describe(self.full_name)
        output_msg('Described {0}'.format(self.full_name))
        return ret_val

    # Get size of selection
    def get_count(self):
        """Get number of rows in table"""
        return int(arcpy.GetCount_management(self.full_name).getOutput(0))

    def get_fields(self, **kwargs):
        """Get list of fields
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/arcpy/functions/listfields.htm"""
        ret_val = arcpy.ListFields(self.full_name, **kwargs)
        output_msg('Listed fields of {0}'.format(self.full_name))
        return ret_val

    def test_field_exists (self, field_name):
        field_list = self.get_fields()
        for field in field_list:
            if field.name == field_name:
                return True
        return False

    # Set the value using the field calculator
    def calculate_field (self, field, expression, expression_type="PYTHON", code_block="", where_clause=None, **kwargs):
        """Calculates the values of a field for a feature class or feature layer
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-field.htm"""
        if where_clause:
            self.selectbyattribute("NEW_SELECTION", where_clause)
        arcpy.CalculateField_management(self.full_name, field, expression, expression_type, code_block, **kwargs)
        output_msg('Calculated field for: [{0}]'.format(self.full_name))

#arcpy.management.CalculateFields(in_table, expression_type, fields, {code_block}, {enforce_domains})


    # Set the value using the field calculator
    def calculate_fields (self, expression_type="PYTHON", fields=None, code_block=None, where_clause=None, **kwargs):
        """Calculates the values of two or more fields
        for a feature class or feature layer.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-fields.htm"""
        if where_clause:
            self.select_by_attribute("NEW_SELECTION", where_clause)
        arcpy.CalculateFields_management(self.full_name, expression_type, fields, code_block, **kwargs)
        output_msg('Calculated field(s) for: [{0}]'.format(self.full_name))


    # Create a field
    def create_field (self, field_name, field_type, **kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/add-field.htm"""
        arcpy.AddField_management(self.full_name, field_name, field_type, **kwargs)
        output_msg('Added field {0} to {1}'.format(field_name, self.full_name))
        
    # Create a field
    def create_fields (self, field_description):
        arcpy.AddFields_management(self.full_name, field_description)
        output_msg('Added field(s) to [{0}] '.format(self.full_name))


    # Create a field and then set the value using the field calculator
    def create_and_calculate_field (self, field_name, field_type, expression, expression_type="PYTHON", field_length="", code_block=""):
        self.create_field(field_name, field_type, field_length=field_length)
        self.calculate_field(field_name, expression, expression_type, code_block)

    # Select something from the table and return the number selected
    def select_by_attribute(self, selection_type="NEW_SELECTION", where_clause="", **kwargs):
        """Adds, updates, or removes a selection based on an attribute query.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/select-layer-by-attribute.htm"""
        arcpy.SelectLayerByAttribute_management(self.full_name, selection_type, where_clause, **kwargs)
        output_msg('SelectLayerByAttribute called on {0}'.format(self.full_name))
        return self.get_count()

    def join_table (self,in_field,join_table,join_field=None,fields=None):
        """Join another table"""
        if join_field == None:
            join_field = in_field
        arcpy.JoinField_management(self.full_name, in_field, join_table.full_name, join_field, fields)
        output_msg('Joined {0} to {1}'.format(join_table.full_name, self.full_name))


    def copy_features (self, out_feature_class, **kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/copy-features.htm"""
        arcpy.CopyFeatures_management (self.full_name, out_feature_class.full_name, **kwargs)
        output_msg('Copied features from {0} to {1}'.format(self.full_name, out_feature_class.full_name))


    # Sort features
    def sort(self,out_dataset,sort_field,direction="ASCENDING",**kwargs):
        """Reorders records in a feature class or table, in ascending or descending order,
        based on one or multiple fields. The reordered result is written to a new dataset.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/sort.htm"""
        arcpy.Sort_management(self.full_name, out_dataset.full_name, [[sort_field,direction]], **kwargs)
        output_msg('Sorted {0}'.format(self.full_name))


    def delete_identical(self, fields, **kwargs):
        """Deletes records in a feature class or table which have identical values in a
        list of fields. If the geometry field is selected, feature geometries are compared.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/delete-identical.htm"""
        arcpy.DeleteIdentical_management(self.full_name, fields, **kwargs)
        output_msg('Deleted identical records in {0}'.format(self.full_name))


    # Add index to table
    def add_index(self, fields, index_name="NEW_INDEX", unique="NON_UNIQUE", ascending="NON_ASCENDING"):
        """Adds an attribute index to an existing table, feature
        class, shapefile, or attributed relationship class."""
        arcpy.AddIndex_management(self.full_name, fields, index_name, unique, ascending)
        output_msg('Added an attribute index to {0}'.format(self.full_name))

    # Delete a field
    def delete_field(self,drop_field):
        arcpy.DeleteField_management(self.full_name, drop_field)
        output_msg('Deleted field(s) from {0}'.format(self.full_name))


    def insert_cursor(self,field_names=None):
        """InsertCursor establishes a write cursor on a feature class or
        table. InsertCursor can be used to add new rows."""
        return arcpy.da.InsertCursor(self.full_name, field_names)

    def search_cursor(self,field_names,where_clause=None,spatial_reference=None,explode_to_points=False,sql_clause=(None,None),**kwargs):
        """SearchCursor establishes read-only access
        to the records returned from a feature class or table.

        It returns an iterator of tuples. The
        order of values in the tuple matches the order
        of fields specified by the field_names argument.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/arcpy/data-access/searchcursor-class.htm"""
        return arcpy.da.SearchCursor(self.full_name, field_names, where_clause, spatial_reference, explode_to_points, sql_clause, **kwargs)

    # Get UpdateCursor
    def update_cursor(self,ufields,where_clause=None,spatial_reference=None,explode_to_points=False,sql_clause=(None,None), **kwargs):
        """UpdateCursor establishes read-write access
        to records returned from a feature class or table.

        Returns an iterator of lists. The order of values
        in the list matches the order of fields specified
        by the field_names argument.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/arcpy/data-access/updatecursor-class.htm"""
        return arcpy.da.UpdateCursor(self.full_name, field_names, where_clause, spatial_reference, explode_to_points, sql_clause, **kwargs)

    def exists(self):
        return arcpy.Exists(self.full_name)

    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.full_name)
            if self.exists():
                arcpy.AddError("{0} cannot be deleted".format(self.full_name))
                return False
            output_msg('Deleted: [{0}] '.format(self.full_name))
        return True

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temp_table == True:
            self.delete()

    def __del__(self):
        if self.temp_table == True:
            self.delete()



# Class for temporary tables
class TempTable(Table):
    """
    Temporary table (gets deleted when it goes out of scope)
    """
    def __init__(self, table_name=None):
        super(TempTable,self).__init__("TempTable" if table_name == None else table_name, temp_table=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempTable,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempTable,self).__del__()



class FeatureClass(Table):
    """
    Attributes
    ----------
    geodatabase_name : str
        geodatabase path where the feature class is stored
    table_name : str
        the feature class name
    full_name : str
    temp_table : bool
    """
    def __init__(self, geodatabase_name, table_name=None, temp_table=False):
        super(FeatureClass,self).__init__(geodatabase_name,table_name,temp_table)

    # Select something from the footprints and return the number selected
    def select_by_location(self, **kwargs):
        """Selects features based on a spatial relationship to features in another dataset.

        Each feature in the Input Features parameter is evaluated against the features in
        the Selecting Features parameter. If the specified Relationship parameter value 
        is met, the input feature is selected.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/select-layer-by-location.htm
        """
        arcpy.SelectLayerByLocation_management(self.full_name, **kwargs)
        output_msg('SelectLayerByLocation called on: [{0}] '.format(self.full_name))
        return self.get_count()

    def extent(self):
        return arcpy.Describe(self.full_name).extent

    def create(self, template=None, spatial_reference="#", **kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/create-feature-class.htm"""
        ret_val = arcpy.CreateFeatureclass_management(self.geodatabase_name, self.table_name, template=template, spatial_reference=spatial_reference, **kwargs)
        output_msg('Created feature class: [{0}] '.format(self.table_name))
        return ret_val

    def spatial_reference(self):
        return self.describe().spatialReference

    def spatial_reference_as_string(self):
        return self.spatial_reference.exporttostring()

    def buffer(self,out_feature_class,buffer_distance_or_field,**kwargs):
        """Creates buffer polygons around input features to a specified distance.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/analysis/buffer.htm"""
        arcpy.Buffer_analysis (self.full_name, out_feature_class.full_name, buffer_distance_or_field, **kwargs)
        output_msg('Buffered: [{0}] '.format(self.full_name))


    def project(self, out_dataset, out_coor_system, transform_method=None, in_coor_system=None, preserve_shape=None, max_deviation=None):
        """Projects spatial data from one coordinate system to another.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/project.htm"""
        arcpy.Project_management (self.full_name, out_dataset, out_coor_system, transform_method, in_coor_system, preserve_shape, max_deviation)
        output_msg('Project_management called on {0}'.format(self.full_name))


    def clip(self,clip_features,out_feature_class):
        """Extracts input features that overlay the clip features.

        Use this tool to cut out a piece of one dataset using one
        or more of the features in another dataset as a cookie cutter.
        This is particularly useful for creating a new dataset—also
        referred to as study area or area of interest (AOI)—that 
        contains a geographic subset of the features in another, larger dataset."""
        arcpy.Clip_analysis(self.full_name, clip_features.full_name, out_feature_class.full_name)
        output_msg('Clipped {0}'.format(self.full_name))


    def erase(self,erase_features,out_feature_class):
        """Creates a feature class by overlaying the input features
        with the erase features. Only those portions of the input
        features falling outside the erase features are copied to the output feature class."""
        arcpy.Erase_analysis (self.full_name, erase_features.full_name, out_feature_class.full_name)
        output_msg('Called Erase on {0}'.format(self.full_name))

    def simplify(self,out_feature_class,tolerance,**kwargs):
        """Simplifies polygon features by removing relatively
        extraneous vertices while preserving essential shape.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/cartography/simplify-polygon.htm
        """
        arcpy.SimplifyPolygon_cartography(self.full_name, out_feature_class.full_name, "POINT_REMOVE", tolerance, **kwargs)
        output_msg('Simplified {0}'.format(self.full_name))

    # Get geometries
    def read_geometries (self, **kwargs):
        """Copies features from the input feature class or layer to a new feature class.
        
        Please see: 
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/copy-features.htm"""
        geometries = arcpy.CopyFeatures_management(self.full_name, arcpy.Geometry(), **kwargs)
        output_msg('Copied features from {0} to {1}'.format(self.full_name, arcpy.Geometry()))
        return geometries

    def write_geometries (self,in_features,**kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/copy-features.htm"""
        arcpy.CopyFeatures_management(in_features, self.full_name, **kwargs)
        output_msg('Copied features from {0} to {1}'.format(in_features, self.full_name))


    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temp_table == True:
            self.delete()

    def __del__(self):
        if self.temp_table == True:
            self.delete()



# Class for temporary feature classes
class TempFeatureClass(FeatureClass):
    """
    Temporary feature class (gets deleted when it goes out of scope)
    """
    def __init__(self, table_name=None):
        super(TempFeatureClass,self).__init__("TempFeatureClass" if table_name == None else table_name, temp_table=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempFeatureClass,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempFeatureClass,self).__del__()



class Raster(object):
    """
    Attributes
    ----------
    geodatabase_name : str
        geodatabase path where the raster is stored
    full_name : str
        geodatabase_name + raster_name
    temp_raster : bool
        Whether or not the raster is a temporary raster
    raster_name : str
    """
    def __init__(self, geodatabase_name, raster_name=None,temp_raster=False):
        self.temp_raster = temp_raster
        # Was this just a table name or a GDB and table name
        self.geodatabase_name, self.raster_name, self.full_name = initialize_names (geodatabase_name, raster_name, temp_raster, arcpy.env.scratchFolder)
        if self.temp_raster == True:
            self.delete()

    def describe(self):
        """The Describe function returns a Describe object with multiple
        properties, such as data type, fields, indexes, and many others."""
        return arcpy.Describe(self.full_name)

    def calculate_statistics(self, **kwargs):
        """Calculates statistics for a raster dataset or a mosaic dataset.

        Statistics are required for your raster and mosaic datasets to perform certain
        tasks, such as applying a contrast stretch or classifying your data.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-statistics.htm"""
        arcpy.CalculateStatistics_management(self.full_name, **kwargs)
        output_msg('Calculated statistics for: [{0}]'.format(self.full_name))


    def copy_raster(self, out_rasterdataset, **kwargs):
        """Saves a copy of a raster dataset or converts
        a mosaic dataset into a single raster dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/copy-raster.htm"""
        arcpy.CopyRaster_management(self.full_name, out_rasterdataset, **kwargs)
        output_msg('Copied raster [{0}] to [{1}]'.format(self.full_name, out_rasterdataset))


    def spatial_reference(self):
        return self.describe().spatialReference

    def exists(self):
        return arcpy.Exists(self.full_name)

    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.full_name)
            if self.exists():
                arcpy.AddError("Raster {0} cannot be deleted".format(self.full_name))
                return False
            output_msg('Deleted: [{0}] '.format(self.full_name))
        return True

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temp_raster == True:
            self.delete()

    def __del__(self):
        if self.temp_raster == True:
            self.delete()



class TempRaster(Raster):
    """
    Temporary raster (gets deleted when it goes out of scope)
    """
    def __init__(self, raster_name=None):
        super(TempRaster,self).__init__("TempRaster" if raster_name == None else raster_name,temp_raster=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempRaster,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempRaster,self).__del__()



# Class for feature layers
class FeatureLayer(FeatureClass):
    """
    Attributes
    ----------
    layer_name : str
        full path to new layer name
    layer : feature layer
    """
    def __init__(self,layer_name,in_features="",where_clause=""):
        self.layer_name = layer_name
        super(FeatureLayer,self).__init__(self.layer_name, temp_table=False)
        if in_features == "":
            if not self.exists(): # might not be necessary?
                arcpy.AddError("Layer {0} does not exist".format(self.layer_name))
                raise arcpy.ExecuteError
        else:
            # Assign the layer for persistence
            self.layer = arcpy.MakeFeatureLayer_management(in_features, self.layer_name, where_clause, "", "")

    def __del__(self):
        super(FeatureLayer,self).__del__()

    def get_count(self):
        return int(arcpy.GetCount_management(self.layer_name).getOutput(0))

    def buffer(self,out_feature_class,buffer_distance_or_field):
        """Creates buffer polygons around input features to a specified distance."""
        arcpy.Buffer_analysis (self.layer_name, out_feature_class, buffer_distance_or_field, "FULL", "ROUND", "NONE")
        output_msg('Buffered: [{0}] '.format(self.layer_name))
        



class MosaicLayer(object):
    """
    Attributes
    ----------
    layer_name : str
        new layer name
    layer : mosaic layer
    feature_layer : FeatureLayer
    """
    def __init__(self,layer_name,mosaic_name):
        self.layer_name = layer_name
        # Assign the layer for persistence
        self.layer = arcpy.MakeMosaicLayer_management(mosaic_name, self.layer_name)
        self.feature_layer = FeatureLayer(os.path.join(self.layer_name, 'Footprint'))

    def __del__(self):
        if arcpy.Exists(self.layer_name):
            arcpy.Delete_management(self.layer_name)
            

    # Select something from the footprints and return the number selected
    def select_by_location(self, **kwargs):
        return int(self.feature_layer.select_by_location(**kwargs))

    def calculate_statistics(self, **kwargs):
        """Calculates statistics for a raster dataset or a mosaic dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-statistics.htm"""
        arcpy.CalculateStatistics_management(self.layer_name, **kwargs)
        output_msg('Calculated statistics for: [{0}]'.format(self.layer_name))


    # Select something from the footprints and return the number selected
    def select_by_attribute(self, selection_type="NEW_SELECTION", where_clause="", **kwargs):
        return int(self.feature_layer.select_by_attribute(selection_type, where_clause, **kwargs))

    # Create a field and then set the value using the field calculator
    def calculate_field (self, field, expression, expression_type="PYTHON", code_block="", where_clause=None, **kwargs):
        self.feature_layer.calculate_field(field, expression, expression_type, code_block, where_clause, **kwargs)

    def calculate_fields (self, expression_type="PYTHON", fields=None, code_block=None, **kwargs):
        self.feature_layer.calculate_fields(expression_type, fields, code_block, **kwargs)

    # Create a field and then set the value using the field calculator
    def create_and_calculate_field (self, field_name, field_type, expression, expression_type="PYTHON", field_length="", code_block=""):
        self.feature_layer.create_and_calculate_field(field_name, field_type, expression, expression_type, field_length, code_block)

    # Create a field
    def create_field (self, field_name, field_type, **kwargs):
        self.feature_layer.create_field(field_name, field_type, **kwargs)
        
    def create_fields (self, field_description):
        self.feature_layer.create_fields(field_description)

    # Delete a field
    def delete_field(self,drop_field):
        self.feature_layer.delete_field(drop_field)

    # Copy footprints
    def copy_footprints (self, out_feature_class, **kwargs):
        self.feature_layer.copy_features(out_feature_class, **kwargs)

    # Find a given field in the footprint layer
    def test_field_exists(self, field_name):
        return self.feature_layer.test_field_exists(field_name)

    # Join another table
    def join_table (self,in_field,join_table,join_field=None,fields=None):
        self.feature_layer.join_table(in_field, join_table, join_field, fields)


    def set_properties(self, **kwargs):
        """Defines the defaults for displaying a mosaic
        dataset and serving it as an image service.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/set-mosaic-dataset-properties.htm"""
        arcpy.SetMosaicDatasetProperties_management(self.layer_name, **kwargs)
        output_msg('Set properties for [{0}] '.format(self.layer_name))



    def set_raster_properties(self, **kwargs):
        """Sets the data type, statistics, and NoData
        values on a raster or mosaic dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/set-raster-properties.htm"""
        arcpy.SetRasterProperties_management(self.layer_name, **kwargs)
        output_msg('Set properties for [{0}] '.format(self.layer_name))

    def get_raster_properties(self, **kwargs): #not sure if this will work
        """Retrieves information from the metadata
        and descriptive statistics about a raster dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/get-raster-properties.htm"""
        ret_val = arcpy.GetRasterProperties_management(self.layer_name, **kwargs)
        output_msg('Got properties for [{0}]'.format(self.layer_name))
        return ret_val


    def repair_paths(self, paths_list, **kwargs):
        """Resets paths to source imagery if
        you have moved or copied a mosaic dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/repair-mosaic-dataset-paths.htm"""
        arcpy.RepairMosaicDatasetPaths_management(self.layer_name, paths_list, **kwargs)
        output_msg('Repaired paths for {0}'.format(self.layer_name))

    def create_image_SD_draft(self, out_sddraft, service_name, **kwargs):
        """The CreateImageSDDraft function is the first step to automating the publishing of a mosaic dataset or raster dataset as an Image
        Service using ArcPy. The output created from the CreateImageSDDraft is a Service Definition Draft
        (.sddraft) file, which is a combination of a mosaic dataset in the geodatabase or a raster dataset, information about the server,
        and a set of service properties. This service definition draft
        can be staged as service definition then uploaded to a specified ArcGIS server as an image service.
        
        Please see:

        https://pro.arcgis.com/en/pro-app/2.8/arcpy/functions/createimagesddraft.htm
        """
        arcpy.CreateImageSDDraft(self.layer_name, out_sddraft, service_name, **kwargs)
        output_msg('CreateImageSDDraft called on {0}'.format(self.layer_name))



# Mosaic dataset class
class MosaicDataset(object):
    """
    Attributes
    ----------
    temp_mosaic : bool
        Whether or not this is a temporary mosaic dataset
    geodatabase_name : str
        geodatabase path where the mosaic dataset is located
    mosaic_name : str
        name of the mosaic dataset
    full_name : str
        geodatabase_name + mosaic_name
    mosaic_layer : MosaicLayer
    """
    def __init__(self,geodatabase_name,mosaic_name=None,temp_mosaic=False):
        self.temp_mosaic = temp_mosaic
        self.mosaic_layer     = None

        # Was this just an image name or a workspace and table name
        self.geodatabase_name, self.mosaic_name, self.full_name = initialize_names (geodatabase_name, mosaic_name, temp_mosaic, arcpy.env.scratchGDB)

        # Needs to exist for mosaic layer functions to work so make sure to create it
        if self.temp_mosaic == True:
            if self.delete() == False:
                raise arcpy.ExecuteError

    # Delete mosaic
    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.full_name)
            if self.exists():
                arcpy.AddError("Mosaic {0} cannot be deleted".format(self.full_name))
                return False
            output_msg('Deleted mosaic dataset: [{0}] '.format(self.full_name))

        return True

    def copy_raster(self, out_rasterdataset, **kwargs):
        """Saves a copy of a raster dataset or converts
        a mosaic dataset into a single raster dataset.
        
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/copy-raster.htm"""
        arcpy.CopyRaster_management(self.full_name, out_rasterdataset, **kwargs)
        output_msg('Copied mosaic dataset [{0}] to [{1}]'.format(self.full_name, out_rasterdataset))



    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temp_mosaic == True: # so then if it's not a temp mosaic, it will never be deleted?
            self.delete()

    def __del__(self):
        if self.temp_mosaic == True:
            self.delete()

    def exists(self):
        return arcpy.Exists(self.full_name)

    def create_layer(self):
        if not self.mosaic_layer:
            self.mosaic_layer = MosaicLayer("{}_Layer".format(self.mosaic_name),self.full_name)

    def create_mosaic(self,coordinate_system=WMAS_ProjCS, **kwargs):
        """Please see: 
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/create-mosaic-dataset.htm"""
        if not self.exists():
            arcpy.CreateMosaicDataset_management(self.geodatabase_name, self.mosaic_name, coordinate_system, **kwargs)
            output_msg('Created mosaic dataset for {0} '.format(self.full_name))

    def calculate_statistics(self, **kwargs):
        """Calculates statistics for a raster dataset or a mosaic dataset.
        Please see: 
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-statistics.htm"""
        arcpy.CalculateStatistics_management(self.full_name, **kwargs)
        output_msg('Calculated statistics for: [{0}]'.format(self.full_name))


    def create_image_SD_draft(self, out_sddraft, service_name, **kwargs):
        self.create_layer()
        self.mosaic_layer.create_image_SD_draft(out_sddraft, service_name, **kwargs)



    def create_reference(self,out_mosaic_dataset, **kwargs):
        """Creates a separate mosaic dataset from items in an existing mosaic dataset.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/create-referenced-mosaic-dataset.htm
        """
        arcpy.CreateReferencedMosaicDataset_management(self.full_name, out_mosaic_dataset.full_name, **kwargs)
        output_msg('Created referenced mosaic dataset: [{0}]'.format(out_mosaic_dataset.full_name))


    def create_temp_reference(self,mosaic_name=None):
        omosaic = MosaicDataset("TempMosaic" if mosaic_name == None else mosaic_name, temp_mosaic=True)
        omosaic.createreference(self)
        return omosaic

    def describe(self):
        return arcpy.Describe(self.full_name)

    def spatial_reference(self):
        return self.describe().spatialReference

    # Find a given field in the footprint layer
    def test_field_exists(self,field_name):
        # Make sure that mlayer exists
        self.create_layer()
        return self.mosaic_layer.test_field_exists(field_name)

    # Join another table
    def join_table (self,in_field,join_table,join_field=None,fields=None):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.join_table(in_field,join_table,join_field,fields)

    def add_mosaic(self,stable,exclude_overviews=False, **kwargs): # addtable changed to addmosaic
        """Adds raster datasets to a mosaic dataset from many
        sources, including a file, folder, table, or web service.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/add-rasters-to-mosaic-dataset.htm
        """
        table_name = stable.full_name
        if exclude_overviews == True:
            stable.select_by_attribute("NEW_SELECTION", "Category = 1")
            table_name = stable.mosaic_layer.layer_name
        arcpy.AddRastersToMosaicDataset_management(self.full_name, "Table", table_name, **kwargs)
        output_msg('Added raster files in [{0}] to mosaic dataset: [{1}]'.format(table_name, self.full_name))


    def add_tables(self,input_path,**kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/add-rasters-to-mosaic-dataset.htm"""
        if type(input_path[0]) is Table:
            input_path = [t.full_name for t in input_path]
        arcpy.AddRastersToMosaicDataset_management(self.full_name, "Table", input_path, **kwargs)
        output_msg('Added raster files in [{0}] to mosaic dataset: [{1}]'.format(input_path, self.full_name))


    def add_rasters(self, input_path, **kwargs):
        """Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/add-rasters-to-mosaic-dataset.htm"""
        arcpy.AddRastersToMosaicDataset_management(self.full_name, "Raster Dataset", input_path, **kwargs)
        output_msg('Added raster files in [{0}] to mosaic dataset: [{1}]'.format(input_path, self.full_name))

    def remove_rasters(self, **kwargs):
        """Removes selected raster datasets from a mosaic dataset.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/remove-rasters-from-mosaic-dataset.htm
        """
        arcpy.RemoveRastersFromMosaicDataset_management(self.full_name, **kwargs)
        output_msg('Removed rasters from mosaic dataset {0}'.format(self.full_name))

    # def addrastersRuben(self, rasterType, inputPath, **kwargs):
    #     arcpy.AddRastersToMosaicDataset_management(self.full_name, rasterType, inputPath, **kwargs)
    #     log_msg('Added raster files in [{0}] to mosaic dataset: [{1}]'.format(inputPath, self.full_name))
    #     display_messages()

    def set_properties(self, **kwargs):
        self.create_layer()
        self.mosaic_layer.set_properties(**kwargs)

    def set_raster_properties(self, **kwargs):
        self.create_layer()
        self.mosaic_layer.set_raster_properties(**kwargs)

    def get_raster_properties(self, **kwargs):
        self.create_layer()
        return self.mosaic_layer.get_raster_properties(**kwargs)

    def repair_paths(self, paths_list, **kwargs):
        self.create_layer()
        self.mosaic_layer.repair_paths(paths_list, **kwargs)

    def build_multidimensional_info(self, **kwargs):
        """Generates multidimensional metadata in the mosaic dataset,
        including information regarding variables and dimensions.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/multidimension/build-multidimensional-info.htm"""
        arcpy.md.BuildMultidimensionalInfo(self.full_name, **kwargs)
        output_msg('Built multidimensional information for mosaic dataset: [{0}]'.format(self.full_name))



    def build_footprints(self, **kwargs):
        """Computes the extent of every raster in a mosaic dataset.
        This tool is used when you have added or removed raster datasets
        from a mosaic dataset and want to recompute the footprints.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/build-footprints.htm"""
        arcpy.BuildFootprints_management(self.full_name,**kwargs)
        output_msg('Built footprints for mosaic dataset: [{0}]'.format(self.full_name))


    def build_boundary(self, **kwargs):
        """Updates the extent of the boundary when adding new raster datasets to a
        mosaic dataset that extend beyond its previous coverage.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/build-boundary.htm
        """
        arcpy.BuildBoundary_management(self.full_name, **kwargs)
        output_msg('Built boundary for mosaic dataset: [{0}]'.format(self.full_name))


    def select_by_attribute(self, selection_type="NEW_SELECTION", where_clause="", **kwargs):
        # Make sure that mlayer exists
        self.create_layer()
        return self.mosaic_layer.select_by_attribute(selection_type, where_clause, **kwargs)

    def calculate_field(self, field, expression, expression_type="PYTHON", code_block="", where_clause=None, **kwargs):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.calculate_field(field, expression, expression_type, code_block, where_clause, **kwargs)

    def calculate_fields (self, expression_type="PYTHON", fields=None, code_block=None, **kwargs):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.calculate_fields(expression_type, fields, code_block, **kwargs)

    def create_and_calculate_field (self, field_name, field_type, expression, expression_type="PYTHON", field_length="", code_block=""):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.create_and_calculate_field(field_name, field_type, expression, expression_type, field_length, code_block)

    def create_field (self, field_name, field_type, **kwargs):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.create_field(field_name, field_type, **kwargs)
        
    def create_fields (self, field_description):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.create_fields(field_description)
        

    def delete_field(self,drop_field):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.delete_field(drop_field)

    def export_geometry(self,out_feature_class, **kwargs): #TESTED, WORKS
        """Creates a feature class showing the footprints, boundary,
        seamlines or spatial resolutions of a mosaic dataset.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/export-mosaic-dataset-geometry.htm"""
        arcpy.ExportMosaicDatasetGeometry_management(self.full_name, out_feature_class, **kwargs)
        output_msg('ExportMosaicDatasetGeometry created feature class {0}'.format(out_feature_class))


    def import_geometry(self,input_featureclass,target_join_field="Name",input_join_field=None,geometry_type="FOOTPRINT",set_clip=True, set_boundary=True):
        if input_join_field == None:
            input_join_field = target_join_field
        arcpy.ImportMosaicDatasetGeometry_management(self.full_name, target_featureclass_type, target_join_field, input_featureclass.full_name, input_join_field)
        output_msg('Modified geometries of mosaic dataset {0}'.format(self.full_name))
        if set_clip == True:
            if geometry_type == "FOOTPRINT":
                self.set_properties (clip_to_footprints="CLIP")
                if set_boundary == True:
                    self.build_boundary()
            elif geometry_type == "BOUNDARY":
                self.set_properties (clip_to_boundary="CLIP")

    def calculate_cell_sizes(self, **kwargs):
        """Computes the visibility levels of raster datasets
        in a mosaic dataset based on the spatial resolution.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/calculate-cell-size-ranges.htm"""
        arcpy.CalculateCellSizeRanges_management(self.full_name, **kwargs)
        output_msg('Calculated the visibility levels for {0}'.format(self.full_name))


    def set_minps(self,expression,where_clause="Category = 1"):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.calculate_field("MinPS", expression, where_clause=where_clause) # here

    def set_maxps(self,expression,where_clause="Category = 1"):
        # Make sure that mlayer exists
        self.create_layer()
        self.mosaic_layer.calculate_field("MaxPS", expression, where_clause=where_clause)

    def define_overviews(self,number_of_levels=-1, resampling_method="#", **kwargs):
        """Lets you set how mosaic dataset overviews are generated. The settings
        made with this tool are used by the Build Overviews tool.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/define-overviews.htm"""
        arcpy.DefineOverviews_management(self.full_name, number_of_levels = number_of_levels, resampling_method = resampling_method, **kwargs)
        output_msg('Built overviews on mosaic dataset {0}'.format(self.full_name))



    def export_paths(self,out_table,where_clause="#",export_mode="ALL",types_of_paths="RASTER"):
        """Creates a table of the file path for each item in a mosaic dataset. You can specify
        whether the table contains all the file paths or just the ones that are broken.
        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/export-mosaic-dataset-paths.htm"""
        arcpy.ExportMosaicDatasetPaths_management(self.full_name, out_table.full_name, where_clause, export_mode, types_of_paths)
        output_msg('Exported file paths in mosaic dataset {0}'.format(self.full_name))


    def build_overviews(self):
        # Make sure that mlayer exists
        self.create_layer()

        # Calculate how many service overviews need to be created
        build_footprint_count = self.mosaic_layer.select_by_attribute("NEW_SELECTION", "Category > 2")

        # Build Overviews
        print ("Building {0} overviews for {1}".format(build_footprint_count, self.full_name))
        arcpy.BuildOverviews_management(self.full_name, "", "NO_DEFINE_MISSING_TILES", "GENERATE_OVERVIEWS", "GENERATE_MISSING_IMAGES", "REGENERATE_STALE_IMAGES")
        output_msg('Built overviews on mosaic dataset {0}'.format(self.full_name))

        # Count any incomplete or partial overviews
        build_footprint_count = self.mosaic_layer.select_by_attribute("NEW_SELECTION", "Category > 2")
        if build_footprint_count > 0:
            arcpy.AddWarning ("{0} overviews failed to build correctly".format(build_footprint_count))

        return build_footprint_count
    
    def build_overviews(self, **kwargs):
        """Defines and generates overviews on a mosaic dataset.
        
        Please see: 
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/build-overviews.htm"""
        arcpy.BuildOverviews_management(self.full_name, **kwargs)
        output_msg("Built overviews on mosaic dataset {0}".format(self.full_name))

    def delete_bad_overviews(self):
        with TempFeatureClass ("BadOverviews") as tmp_table:

            # Put list of bad overviews in table
            self.export_paths(tmp_table, "Category > 2", "ALL", "RASTER")
            tmp_table.delete_identical("SourceID")

            with arcpy.da.SearchCursor(tmp_table.full_name, "Path") as bad_overviews:
                for bad_overview in bad_overviews:
                    if arcpy.Exists (bad_overview[0]):
                        arcpy.AddWarning ("Deleting {0}".format(bad_overview[0]))
                        arcpy.Delete_management(bad_overview[0])
                        output_msg("Deleted {0}".format(bad_overview[0]))

                    else:
                        arcpy.AddWarning ("Overview {0} not found".format(bad_overview[0]))

    def build_overviews_robust(self,max_retries=5):
        unbuilt_overview_count = self.build_overviews()

        retry_count = 1
        while unbuilt_overview_count > 0 and retry_count < max_retries:
            self.delete_bad_overviews()
            unbuilt_overview_count = self.build_overviews()
            retry_count += 1

        if unbuilt_overview_count > 0:
            arcpy.AddError ("Overviews failed to build correctly. Exiting...")
            raise arcpy.ExecuteError

    def delete_external_rasters(self,sbound,exc_zone=None,buffer_dist=None):
        temp_boundary = TempFeatureClass("TempBnd")
        temp_exc_zone = TempFeatureClass("TempExclZone")

        # Make sure that mlayer exists
        self.create_layer()

        # Select all rasters in the mosaic
        delete_footprint_count = self.mosaic_layer.select_by_attribute("NEW_SELECTION", "")

        # Remove rasters that intersect the boundary after a buffer
        if buffer_dist:
            sbound.buffer(temp_boundary, buffer_dist)
        else:
            sbound.copyfeatures(temp_boundary)


        delete_footprint_count = self.mosaic_layer.select_by_location(overlap_type="INTERSECT", select_features=temp_boundary, search_distance="", selection_type="REMOVE_FROM_SELECTION")

        # Add rasters that are completely within the exclusion zone to the selection after a buffer
        if exc_zone != None:
            if buffer_dist:
                exc_zone.buffer(temp_exc_zone, buffer_dist)
            else:
                temp_exc_zone = exc_zone
            delete_footprint_count = self.mosaic_layer.select_by_location(overlap_type="COMPLETELY_WITHIN", select_features=temp_exc_zone, search_distance="", selection_type="ADD_TO_SELECTION")

        # Remove Selected Rasters From Mosaic Dataset
        if delete_footprint_count > 0:
            self.remove_rasters()

    def build_crf_overviews(self, overview_location, crf_name, factor = 5, num_pyramids = -1):
        output_msg(f'Creating overview crf file for {self.full_name}')
        s3_crf_key = os.path.join(overview_location, crf_name)

        
        res = arcpy.da.TableToNumPyArray(self.full_name, ['LowPS'])
        minimum_lowPS = 999999999
        for elem in res:
            if elem[0] < minimum_lowPS:
                minimum_lowPS = elem[0]
        minimum_lowPS *= factor
        print(f"minimum_lowPS is {minimum_lowPS}")
        with arcpy.EnvManager(pyramid='PYRAMIDS ' + str(num_pyramids), cellSize=minimum_lowPS):
            arcpy.management.CopyRaster(in_raster=self.full_name, out_rasterdataset=s3_crf_key)

        print("GOT HERE!")

        output_msg(f'Adding overview file to {self.full_name}')
        arcpy.management.AddRastersToMosaicDataset(
            in_mosaic_dataset=self.full_name,
            raster_type='Raster Dataset',
            input_path=s3_crf_key,
            update_cellsize_ranges='UPDATE_CELL_SIZES',
        )
        

        table = self.full_name
        fields = ['MaxPS', 'Category', 'LowPS', 'MinPS']
        with arcpy.da.UpdateCursor(
            table,
            fields,
            f"Name = '{s3_crf_key}'",
        ) as cursor:
            for row in cursor:
                row[0] = 10000
                row[1] = 2
                theLowPS = row[2]
                row[3] = theLowPS
                cursor.updateRow(row)

        fields = ['MaxPS']
        with arcpy.da.UpdateCursor(
            table,
            fields,
            f"Category = 1",
        ) as cursor:
            for row in cursor:
                if row[0] > theLowPS:
                    row[0] = theLowPS
                    cursor.updateRow(row)

    
    def synchronize_mosaic_dataset(self, **kwargs):
        """Keeps your mosaic dataset up to date. In addition to syncing data,
        you can update overviews if the underlying imagery has been changed,
        generate new overviews and cache, and restore the original configuration
        of mosaic dataset items. You can also remove paths to source data with
        this tool. To repair paths, you need to use the Repair Mosaic Dataset Paths tool.

        Synchronization is a one-way operation: changes in the source data
        can be synchronized to the mosaic dataset’s attribute table, thereby
        updating the mosaic dataset's attribute table. Changes in the mosaic
        dataset's attribute table will not affect the source data.

        Please see:
        https://pro.arcgis.com/en/pro-app/2.8/tool-reference/data-management/synchronize-mosaic-dataset.htm
        """
        arcpy.SynchronizeMosaicDataset_management(self.full_name, **kwargs)
        output_msg(f"Synchronized mosaic dataset {self.full_name}")


class TempMosaicDataset(MosaicDataset):
    """
    Temporary mosaic dataset (gets deleted when it goes out of scope)
    """
    def __init__(self,mosaic_name=None):
        super(TempMosaicDataset,self).__init__("TempMosaic" if mosaic_name == None else mosaic_name,temp_mosaic=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempMosaicDataset,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempMosaicDataset,self).__del__()



class Progressor(object):
    """
    Attributes
    ----------
    type : str
        the progressor type
    label : str
        the progressor label
    min_range : int
        the minimum value for the progressor
    max_range : int
        the maximum value for the progressor
    step_value : int
        the progressor step interval for updating the progress bar
    position : int
        the position that the progressor is at
    initialized : bool
        Whether or not the progressor has been initialized
    """
    def __init__(self,type="default",label=None,min_range=0,max_range=100,step_value=1):
        self.type        = type
        self.label       = label
        self.min_range   = min_range
        self.max_range   = max_range
        self.step_value  = step_value
        self.position    = 0
        self.initialized = False

    def initialize(self):
        if not self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)
            self.initialized = True

    def set_default_type(self):
        self.type = "default"
        if self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)

    def set_range(self,min_range=0,max_range=100,step_value=1):
        self.type        = "step"
        self.min_range   = min_range
        self.max_range   = max_range
        self.step_value  = step_value
        if self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)

    def set_label(self,label):
        if self.label != label:
            self.label = label
            if not self.initialized:
                self.initialize()
            else:
                arcpy.SetProgressorLabel(self.label)

    def set_position(self,position):
        if self.position != position:
            self.position = position
            if not self.initialized:
                self.initialize()
            arcpy.SetProgressorPosition(self.position)

    def reset(self):
        if self.initialized:
            arcpy.ResetProgressor()

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        self.reset()

    def __del__(self):
        self.reset()


# try:
#     set_logging("C:\log")
#     md = MosaicDataset("timeEnabled.gdb", "testMosaic")
#     md.createmosaic()
#     md.addrasters(r"C:\Users\rub12531\Downloads\m_4207533_se_18_h_2017_20151109.jp2;C:\Users\rub12531\Downloads\m_4207637_sw_18_h_2015_20151109.jp2;C:\Users\rub12531\Downloads\m_4207636_se_18_h_2016_20151109.jp2", update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="UPDATE_BOUNDARY", update_overviews="NO_OVERVIEWS", maximum_pyramid_levels=None, maximum_cell_size=0, minimum_dimension=1500, spatial_reference=None, filter='', sub_folder="SUBFOLDERS", duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS", operation_description='', force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE", estimate_statistics="NO_STATISTICS", aux_inputs=None, enable_pixel_cache="NO_PIXEL_CACHE", cache_location=r"C:\Users\rub12531\AppData\Local\ESRI\rasterproxies\mosaicDataset")
#     md.createfields([["date", "TEXT", 'date', 255, None, '']])
#     md.calculatefield(full_name="date", cexp="!Name!.split('_')[5]", clang="PYTHON3", cblock='', field_type="TEXT", enforce_domains="NO_ENFORCE_DOMAINS")
#     md.setproperties(rows_maximum_imagesize=4100, columns_maximum_imagesize=15000, allowed_compressions="None;JPEG;LZ77;LERC", default_compression_type="None", JPEG_quality=75, LERC_Tolerance=0.01, resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP", footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="CLIP", color_correction="NOT_APPLY", allowed_mensuration_capabilities="Basic", default_mensuration_capabilities="Basic", allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None", default_mosaic_method="NorthWest", order_field='', order_base='', sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width=10, view_point_x=600, view_point_y=300, max_num_per_mosaic=20, cell_size_tolerance=0.8, cell_size="0 0", metadata_level="Basic", transmission_fields="Name;MinPS;MaxPS;LowPS;HighPS;Tag;GroupName;Productable_name;CenterX;CenterY;ZOrder;Shape_Length;Shape_Area", use_time="ENABLED", start_time_field="date", end_time_field="date", time_format="YYYY", geographic_transform=None, max_num_of_download_items=20, max_num_of_records_returned=1000, data_source_type="GENERIC", minimum_pixel_contribution=1, processing_templates="None", default_processing_template="None", time_interval=None, time_interval_units='', product_definition="NONE", product_band_definitions=None)
# except Exception as exc:
#     logging.exception(exc)
# finally:
#     log = logging.getLogger()
#     lhs = list(log.handlers)
#     for i in lhs:
#         # print(f"Log Handler: {i}")
#         log.removeHandler(i)
#         i.flush()
#         i.close()