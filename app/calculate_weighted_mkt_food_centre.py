# Author: Kok Jim Meng

# Prepare Environment
# -----------------------------------------------------------------------------
import arcpy as ap
import os
import pandas as pd

# Set environments
ap.env.outputCoordinateSystem = ap.SpatialReference(3414)
ap.env.overwriteOutput = True

# Read the files and paths
resale_bldg_2022_tpy_df = pd.read_csv("../Data/processed/unique_tpy_resale_buildings_2022_df.csv")
nea_mkt_food_ct_path = os.path.join("../Data/raw/NEAMarketandFoodCentreKML-point.shp")

# Data Ingestion Functions
def ingest_nea_mkt_food_ct_data(nea_mkt_food_ct_path:str):
    '''Ingest data for NEA market and food centre integration'''
    nea_mkt_food_ct = "in_memory/nea_mkt_food_ct"
    output = ap.CopyFeatures_management(nea_mkt_food_ct_path,
                                        nea_mkt_food_ct)
    return output

# Convert CSV to XY feature class
resale_bldg_2022_tpy_layer = "in_memory/resale_bldg_2022_tpy_layer"
ap.management.MakeXYEventLayer_management(table=resale_bldg_2022_tpy_df,
                                         in_x_field="longitude",
                                         in_y_field="latitude",
                                         out_layer=resale_bldg_2022_tpy_layer,
                                         spatial_reference=("GEOGCS['GCS_WGS_1984',"
                                                            + "DATUM['D_WGS_1984',"
                                                            + "SPHEROID['WGS_1984',6378137.0,298.257223563]],"
                                                            + "PRIMEM['Greenwich',0.0],"
                                                            + "UNIT['Degree',0.0174532925199433]];"
                                                            + "-20037700 -30241100 10000;-100000 10000;-100000 10000;"
                                                            + "0.001;0.001;0.001;IsHighPrecision")
                            )
resale_bldg_2022_tpy_fc = "in_memory/resale_bldg_2022_tpy_fc"
ap.CopyFeatures_management(in_features=resale_bldg_2022_tpy_layer,
                           out_feature_class=resale_bldg_2022_tpy_fc);

def calc_dist_weighted_mkt_food_ctr_to_resale_flat_tpy_catchment(output_gdb_path:str="..Data/processed/sum_weighted_dist_resale_flat_mkt_food_ct.gdb",
                                                                 nea_mkt_food_ct_path:str="",
                                                                 resale_bldgs:str = resale_bldg_2022_tpy_fc,
                                                                 search_distance:int=400,
                                                                 show_messages:bool=False
                                                                ):
    # Declare resale buildings and NEA's market & food centre datasets
    nea_mkt_food_ct = ingest_nea_mkt_food_ct_data(nea_mkt_food_ct_path=nea_mkt_food_ct_path)
    
    # generate near table
    catchment_string = f"{search_distance} meters"
    hdb_mktfc_ntb = 'in_memory/hdb_mktfc_ntb'
    hdb_mktfc_ntb = ap.analysis.GenerateNearTable(resale_bldgs,
                                                  nea_mkt_food_ct,
                                                  hdb_mktfc_ntb,
                                                  catchment_string,
                                                  "NO_LOCATION",
                                                  "NO_ANGLE",
                                                  "ALL",
                                                  0,
                                                  "PLANAR")

    # create new field for weighted distance
    ap.management.CalculateField(hdb_mktfc_ntb,
                                 "Dist_Wt_Mkt_Food_Ctr",
                                 "1/!NEAR_DIST!", "PYTHON3", '',
                                 "DOUBLE", "NO_ENFORCE_DOMAINS")
    
    # sum up the weighted distance by the resale flats
    weight_dist_tbl = "in_memory/weight_dist_tbl"
    weight_dist_tbl = ap.analysis.Statistics(hdb_mktfc_ntb,
                                             weight_dist_tbl,
                                             "Dist_Wt_Mkt_Food_Ctr SUM",
                                             "IN_FID")
    
    # merge the table with the TPY's 2022 resale flat based on infid
    ap.management.JoinField(resale_bldgs, "OBJECTID", weight_dist_tbl,
                            "IN_FID", "Sum_Dist_Wt_Mkt_Food_Ctr")
    
    # rename the column
    ap.AlterField_management(resale_bldgs, "Sum_Dist_Wt_Mkt_Food_Ctr",
                             "Dist_Wt_Mkt_Food_Ctr", "Dist_Wt_Mkt_Food_Ctr")
    # replace null values with 0
    replacement_string = ("0 if !Dist_Wt_Mkt_Food_Ctr! is None "
                          + "else !Dist_Wt_Mkt_Food_Ctr!")
    ap.management.CalculateField(resale_bldgs, "Dist_Wt_Mkt_Food_Ctr",
                                 replacement_string, "PYTHON3")
    # round the sum off to 3 dp
    ap.management.CalculateField(resale_bldgs,
                                 "Dist_Wt_Mkt_Food_Ctr",
                                 "round(!Dist_Wt_Mkt_Food_Ctr!, 3)",
                                 "PYTHON3", '', "DOUBLE",
                                 "NO_ENFORCE_DOMAINS")
    
    # save the resulting feature class back to the geopackage
    if show_messages:
        print("Exporting Count of NEA's Market & Food Centre Weighted by Distance from each Resale Building Feature Class")
    path_to_save = os.path.join(output_gdb_path, output_fc_name)
    return_object = ap.CopyFeatures_management(resale_bldgs, path_to_save)
    # delete the intermediate feature class
    ap.Delete_management(resale_bldgs)
    return return_object
