import arcpy
import ArcPyWrapper as im
#import log_process as lp

# https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/a-template-for-python-toolboxes.htm

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [timeEnabled]

class timeEnabled(object):
    def __init__(self):
        self.label = "Time-Enabled Mosaic Dataset Load"
        self.description = "Creates a time-enabled mosaic dataset"
        self.canRunInBackground = False

    def getParameterInfo(self):

        param_geodatabase = arcpy.Parameter(
            displayName="Input Geodatabase",
            name="geodatabase",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input"
        )


        param_mosaic_dataset = arcpy.Parameter(
            displayName="Mosaic Dataset Name (e.g. LakeAlgae2022)",
            name="mosaic_dataset",
            datatype="String",
            parameterType="Required",
            direction="Input"
        )

        param_rasters = arcpy.Parameter(
            displayName=r"Folder Location of Rasters to Add to Mosaic Dataset",
            name="rasters",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input"
        )


        params = [param_geodatabase,
                param_mosaic_dataset,
                param_rasters]

        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        # Read in the parameters
        param_geodatabase = parameters[0].valueAsText
        param_mosaic_dataset = parameters[1].valueAsText
        param_rasters = parameters[2].valueAsText
        
        md = im.MosaicDataset(param_geodatabase, param_mosaic_dataset)
        md.create_mosaic()
        md.add_rasters(param_rasters.replace("\\","/"), update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="UPDATE_BOUNDARY", update_overviews="NO_OVERVIEWS", spatial_reference=None, filter='', sub_folder="SUBFOLDERS", duplicate_items_action="EXCLUDE_DUPLICATES", build_pyramids="NO_PYRAMIDS", calculate_statistics="CALCULATE_STATISTICS", build_thumbnails="NO_THUMBNAILS", operation_description='', force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE", estimate_statistics="NO_STATISTICS", aux_inputs=None)
        md.create_fields([["date", "DATE", 'date']])
        md.calculate_field(field="date", expression="'20' + !Name!.split('_')[1][4:6] +'/'+ !Name!.split('_')[1][:2] + '/' + !Name!.split('_')[1][2:4]", expression_type="PYTHON3", code_block='', field_type="TEXT", enforce_domains="NO_ENFORCE_DOMAINS")
        md.set_properties(allowed_compressions="None;JPEG;LZ77;LERC", default_compression_type="None", JPEG_quality=75, LERC_Tolerance=0.01, resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP", footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="CLIP", color_correction="NOT_APPLY", allowed_mensuration_capabilities="Basic", default_mensuration_capabilities="Basic", allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None", default_mosaic_method="NorthWest", order_field='', order_base='', sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width=10, view_point_x=600, view_point_y=300, max_num_per_mosaic=20, cell_size_tolerance=0.8, metadata_level="Basic", transmission_fields="Name;MinPS;MaxPS;LowPS;HighPS;Tag;GroupName;ProductName;CenterX;CenterY;ZOrder;Shape_Length;Shape_Area", use_time="ENABLED", start_time_field="date", end_time_field="date", time_format="YYYY/MM/DD", geographic_transform=None, max_num_of_download_items=20, max_num_of_records_returned=1000, data_source_type="GENERIC", minimum_pixel_contribution=1, processing_templates="None", default_processing_template="None", time_interval=1, time_interval_units='Days', product_definition="NONE", product_band_definitions=None)
        md.calculate_statistics()