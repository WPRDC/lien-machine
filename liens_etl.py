import os, sys, json, re, datetime
from marshmallow import fields, pre_load, post_load
import process_liens
import export_db_to_csv

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
from pprint import pprint
import time
import process_foreclosures
import ckanapi

from parameters.local_parameters import SETTINGS_FILE, DATA_PATH
from util.notify import send_to_slack
from util.ftp import fetch_files

import pipe_liens_from_csv, pipe_sats_from_csv, pipe_raw_lien_records_from_csv, pipe_summary_from_csv

def open_a_channel(settings_file,server):
    # Get parameters to communicate with a CKAN instance
    # from the specified JSON file.
    with open(settings_file) as f:
        settings = json.load(f)
        API_key = settings["loader"][server]["ckan_api_key"]
        site = settings["loader"][server]["ckan_root_url"]
        package_id = settings['loader'][server]['package_id']
    return site, API_key, package_id, settings

def resource_show(ckan,resource_id):
    # A wrapper around resource_show (which could be expanded to any resource endpoint)
    # that tries the action, and if it fails, tries to dealias the resource ID and tries
    # the action again.
    try:
        metadata = ckan.action.resource_show(id=resource_id)
    except ckanapi.errors.NotFound:
        # Maybe the resource_id is an alias for the real one.
        real_id = dealias(site,resource_id)
        metadata = ckan.action.resource_show(id=real_id)
    except:
        msg = "{} was not found on that CKAN instance".format(resource_id)
        print(msg)
        raise ckanapi.errors.NotFound(msg)

    return metadata

def get_resource_parameter(site,resource_id,parameter,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
        print("get_resource_parameter: metadata")
        pprint(metadata)
        desired_string = metadata[parameter]

        #print("The parameter {} for this resource is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

    return desired_string


def upload_file(site,package_id,API_key,zip_file_path,resource_name,description):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_create(
        package_id = package_id,
        name = resource_name,
        description = description,
        url = 'dummy-value',  # ignored but required by CKAN<2.6
        upload = open(zip_file_path, 'rb')) # Returns the metadata for the resource.
    print("upload_file results")
    pprint(metadata)

def zip_and_deploy_file(settings_file,server,filepath,zip_file_name,source_resource_name,resource_id):
    import shutil
    original_file_name = filepath.split("/")[-1]
    dpath = '/'.join(filepath.split("/")[:-1]) + '/'
    if dpath == '/':
        dpath = ''

    print("dpath={}".format(dpath))
    zip_path = dpath + "zipped"
    # If this path doesn't exist, create it.
    if not os.path.exists(zip_path):
        os.makedirs(zip_path)

    #cp synthesized-liens.csv zipped/liens-with-current-status-beta.csv
    file_to_zip = zip_path+'/'+original_file_name
    zip_file_path = zip_path+'/'+zip_file_name
    shutil.copy2(filepath, file_to_zip)

    #zip zipped/liens-with-current-status-beta.zip zipped/liens-with-current-status-beta.csv
    import zipfile
    process_zip = zipfile.ZipFile(zip_file_path, 'w')
    process_zip.write(file_to_zip, compress_type=zipfile.ZIP_DEFLATED)
    process_zip.close()

    #rm zipped/liens-with-current-status-beta.csv
    os.remove(file_to_zip)

    #[get location of PREVIOUSLY uploaded zip file if any] (maybe from the primary resource's download link)
    site, API_key, package_id, settings = open_a_channel(settings_file,server)

    original_url = get_resource_parameter(site,resource_id,'url',API_key)
    # Example of a URL than just dumps from the datastore:
    #   https://data.wprdc.org/datastore/dump/1bb6be50-bc7d-4b21-a3a0-1ac27a9e5994
    # Example of a URL that has been modified to link to another file:
    #   https://data.wprdc.org/dataset/22fe57da-f5b8-4c52-90ea-b10591a66f90/resource/7d4c4428-e7a3-4d0e-9d1a-2db348dec233/download/liens-with-current-status-beta.zip
    # So if the URL contains 'dump' there's no old zipped-file resource to terminate.


    # [ ] ALSO, track down the resource for the zipped file (if it exists) and send the 
    # updated zip file to that resource.


    #[upload zipped file to CKAN]
    #ckanapi resource_create package_id=22fe57da-f5b8-4c52-90ea-b10591a66f90
    # Example name: Raw tax-lien records (beta) [compressed CSV file]
    # Example description: 
    #       This is a compressed CSV file version of the table of the raw tax-liens records available here:
    #
    #       https://data.wprdc.org/dataset/allegheny-county-tax-liens-filed-and-satisfied/resource/8cd32648-757c-4637-9076-85e144997ca8
    description = 'This is a compressed CSV file version of the data in the resource "{}"'.format(source_resource_name)

    upload_file(site,package_id,API_key,zip_file_path,
        resource_name=source_resource_name + " [compressed CSV file]", 
        description=description)
    # 

    #[get location of uploaded zip file]

    #utility__belt$ set_url [resource_id] [location of uploaded zip file]

    # Delete old zipped-file resource on CKAN.

def main(*args,**kwargs):
    # Get the latest files through FTP
    print("Pulling the latest foreclosures data from the FTP server.")

    # Change path to script's path for cron job
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    local_path = dname+"/latest_pull"
    # If this path doesn't exist, create it.
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    search_terms = ['pitt1_','pitt_lien', 'pitt_sat']

    server = kwargs.get('server','test-production')

    just_testing = True
    if just_testing:
        DATA_PATH = '/Users/daw165/data/TaxLiens/lien-machine/tmp' ## temporary (for testing)
        server = 'test-production'
    else:
        server = 'production'

    get_files_by_ftp = True
    if get_files_by_ftp:###################################### temporary (for testing)
        file_locations = fetch_files(SETTINGS_FILE,local_path,DATA_PATH,search_terms)

        print("file_locations = {}".format(file_locations))

    # STEP 2: caffeinate -i python /Users/daw165/data/TaxLiens/lien-machine/process_liens.py cv_m_pitt_lien_monYEAR.txt cv_m_pitt_sat_monYEAR.txt liens.db
        if len(file_locations) <= 2:
            raise ValueError("Not enough files found to complete the extraction.")
        db_file_name = 'liens.db'
        process_liens.main(liens_file_path = file_locations[1], sats_file_path = file_locations[2], db_file_path = DATA_PATH+'/'+db_file_name)
    else:
        db_file_name = 'liens.db'
        process_liens.main(liens_file_path = DATA_PATH+'/'+'cv_m_pitt_lien_OCT2017.lst', sats_file_path = DATA_PATH+'/'+'cv_m_pitt_sat_OCT2017.lst', db_file_path = DATA_PATH+'/'+db_file_name)


# STEP 3: caffeinate -i python ../lien-machine/export_db_to_csv.py liens.db # It takes some number of minutes (maybe 10 or 20) to run this
    export_db_to_csv.main(db_file_path = DATA_PATH+'/'+db_file_name)
# STEP 4
#caffeinate -i python pipe_sats_from_csv.py /PATH/TO/raw-sats-liens.csv [clear_first]
#caffeinate -i python pipe_raw_lien_records_from_csv.py /PATH/TO/raw-liens.csv [clear_first] # Takes almost three hours to run.
#caffeinate -i python pipe_liens_from_csv.py /PATH/TO/synthesized-liens.csv [clear_first]
#caffeinate -i python pipe_summary_from_csv.py /PATH/TO/summary-liens.csv



    liens_input_file = DATA_PATH+'/synthesized-liens.csv'
    liens_resource_id = pipe_liens_from_csv.main(input_file=liens_input_file, server=server)
    raw_liens_records_input_file = DATA_PATH+'/raw-liens.csv'
    raw_resource_id = pipe_raw_lien_records_from_csv.main(input_file=raw_liens_records_input_file, server=server)
    sats_resource_id = pipe_sats_from_csv.main(input_file=DATA_PATH+'/raw-sats-liens.csv', server=server)
    summary_resource_id = pipe_summary_from_csv.main(input_file=DATA_PATH+'/summary-liens.csv', server=server)

# This step could be accelerated if we exported and then piped to CKAN 
# only the new changed rows, but that might not be so easy to figure out. 
# The best way to do that might be to track in the database which rows 
# have been successfully upserted (upsert some rows, check that the 
# process finished without exception, and then flag all those rows
# in the local database as upserted).
    print("Piped all new liens data to the {} server.".format(server))

# STEP 5 - Compress and deploy the raw lien records CSV file and the 
# liens CSV file as CKAN resources, and also switch the main resource 
# download link to download the zipped version.

    zip_and_deploy_file(settings_file=SETTINGS_FILE, server=server, 
            filepath=liens_input_file, zip_file_name='liens-with-current-status-beta.zip', 
            source_resource_name="Tax liens with current status (beta)",
            resource_id=liens_resource_id)
    zip_and_deploy_file(settings_file=SETTINGS_FILE, server=server, 
            filepath=raw_liens_records_input_file, zip_file_name='raw-liens-records-beta.zip', 
            source_resource_name="Raw tax-lien records (beta)",
            resource_id=liens_resource_id)
    print("There's still more to do!")

    # Every time this is run, new resources are created. Instead of deleting old ZIP files when they're obsolete,
    # REPLACE the ZIP file in the existing resource.

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    if len(sys.argv) > 1:
        server = sys.argv[1]
        main(server = server)
    else:
        main()
