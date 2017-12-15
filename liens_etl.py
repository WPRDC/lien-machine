import os, sys, json, re
from marshmallow import fields, pre_load, post_load
import process_liens
import export_db_to_csv

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
from datetime import datetime, date, timedelta
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

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

    return desired_string

def set_package_parameters_to_values(site,package_id,parameters,new_values,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
        payload = {}
        payload['id'] = package_id
        for parameter,new_value in zip(parameters,new_values):
            payload[parameter] = new_value
        results = ckan.action.package_patch(**payload)
        print(results)
        print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))

    return success

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

def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    """Sets the given resource parameters to the given values for the specified
    resource.

    This fails if the parameter does not currently exist. (In this case, use
    create_resource_parameter()."""
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_resource_parameter(site,resource_id,p,API_key) for p in parameters]
        payload = {}
        payload['id'] = resource_id
        for parameter,new_value in zip(parameters,new_values):
            payload[parameter] = new_value
        #For example,
        #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
        results = ckan.action.resource_patch(**payload)
        print(results)
        print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))

    return success

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
        #print("get_resource_parameter: metadata")
        #pprint(metadata)
        desired_string = metadata[parameter]

        #print("The parameter {} for this resource is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

    return desired_string

def get_resource_name(site,resource_id,API_key=None):
    return get_resource_parameter(site,resource_id,'name',API_key)

def upload_file_to_existing_resource(site,package_id,API_key,zip_file_path,resource_id,description):
    # The two "upload_file" functions could be combined by sending resource_id or resource_name
    # as named kwargs, adding them to a kwparams dict and sending that to an appropriately
    # chosen ckan API call function (either resource_create or resource_update).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    print("      Uploading file to an existing resource ({})".format(resource_id))
    metadata = ckan.action.resource_update(
        id = resource_id,
        description = description,
        url = 'dummy-value',  # ignored but required by CKAN<2.6
        upload = open(zip_file_path, 'rb')) # Returns the metadata for the resource.
    print("upload_file results")
    pprint(metadata)
    return metadata['url']

def upload_file_to_new_resource(site,package_id,API_key,zip_file_path,resource_name,description):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_create(
        package_id = package_id,
        name = resource_name,
        description = description,
        url = 'dummy-value',  # ignored but required by CKAN<2.6
        upload = open(zip_file_path, 'rb')) # Returns the metadata for the resource.
    #print("upload_file results")
    #pprint(metadata)
    return metadata['url']

def zip_and_deploy_file(settings_file,server,filepath,zip_file_name,resource_id,original_url):
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
    process_zip.write(file_to_zip, original_file_name, compress_type=zipfile.ZIP_DEFLATED)
    process_zip.close()

    #rm zipped/liens-with-current-status-beta.csv
    os.remove(file_to_zip)

    #[get location of PREVIOUSLY uploaded zip file if any] (maybe from the primary resource's download link)
    site, API_key, package_id, settings = open_a_channel(settings_file,server)
    source_resource_name = get_resource_name(site,resource_id,API_key)

    # Example of a URL than just dumps from the datastore:
    #   https://data.wprdc.org/datastore/dump/1bb6be50-bc7d-4b21-a3a0-1ac27a9e5994
    # Example of a URL that has been modified to link to another file:
    #   https://data.wprdc.org/dataset/22fe57da-f5b8-4c52-90ea-b10591a66f90/resource/7d4c4428-e7a3-4d0e-9d1a-2db348dec233/download/liens-with-current-status-beta.zip
    # So if the URL contains 'dump' there's no old zipped-file resource to terminate.
    #e.g., #url = "https://data.wprdc.org/dataset/22fe57da-f5b8-4c52-90ea-b10591a66f90/resource/7d4c4428-e7a3-4d0e-9d1a-2db348dec233/download/liens-with-current-status-beta.zip"
    #e.g., #url.split('/')
    #e.g., #['https:', '', 'data.wprdc.org', 'dataset', '22fe57da-f5b8-4c52-90ea-b10591a66f90', 'resource', '7d4c4428-e7a3-4d0e-9d1a-2db348dec233', 'download', 'liens-with-current-status-beta.zip']
    # whereas a dump URL splits like this:
    #e.g., #['https:', '', 'data.wprdc.org', 'datastore', 'dump', '1bb6be50-bc7d-4b21-a3a0-1ac27a9e5994']
    # [X] ALSO, track down the resource for the zipped file (if it exists) and send the 
    # updated zip file to that resource.
    description = 'This is a compressed CSV file version of the data in the resource "{}"'.format(source_resource_name)

    uploaded = False
    if original_url is not None and re.search("\.zip$",original_url.split('/')[-1]) is not None: # It's a zip file in the filestore.
        url_parts = original_url.split('/')
        print("url_parts = {}".format(url_parts))

        #if url_parts[3] == 'datastore':
            # Somehow the file was being stored in the datastore and no link to the zip file was found.
            # Therefore a new resource needs to be created and the download URL needs to be updated.
        print("  zip_and_deploy: Found an existing zip file in the download URL: {}".format(original_url))
        zip_resource_id = url_parts[-3]
        assert url_parts[-2] == 'download' # These are checks to be 
        assert url_parts[-4] == 'resource' # sure that the URL format is correct.
        # The package ID could also be verified.
        
        # Just update the zip file and return to the calling code:
        print("  zip_and_deploy: Uploading file to EXISTING resource ({})...".format(zip_resource_id))
        try:
            url_of_file = upload_file_to_existing_resource(site,package_id,API_key,zip_file_path,
                resource_id=zip_resource_id,
                description=description)
            uploaded = True
        except ckanapi.errors.NotFound:  #ckanapi.errors.NotFound: Resource was not found.
            # The zip file resource was deleted so fall back to uploading a new resource.
            uploaded = False
            print("The resource that the original_url was pointing to is no longer there.")

    if not uploaded:
        if original_url is None:
            print("No original_url found (maybe because the resource did not initially exist).")
        else:
            print("Unable to update an existing zip-file resource, so let's create a new one.")
        #[upload zipped file to CKAN]
        #ckanapi resource_create package_id=22fe57da-f5b8-4c52-90ea-b10591a66f90
        # Example name: Raw tax-lien records (beta) [compressed CSV file]
        # Example description: 
        #       This is a compressed CSV file version of the table of the raw tax-liens records available here:
        #
        #       https://data.wprdc.org/dataset/allegheny-county-tax-liens-filed-and-satisfied/resource/8cd32648-757c-4637-9076-85e144997ca8

        print("  zip_and_deploy: Uploading file to new resource...")
        url_of_file = upload_file_to_new_resource(site,package_id,API_key,zip_file_path,
            resource_name=source_resource_name + " [compressed CSV file]", 
            description=description)

    #[get location of uploaded zip file]
    print("url_of_file = {}".format(url_of_file))

    #utility__belt$ set_url [resource_id] [location of uploaded zip file]
    set_resource_parameters_to_values(site,resource_id,['url'],[url_of_file],API_key)
    # Delete the local zipped file.
    os.remove(zip_file_path)

def end_of_last_month():
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_of_previous_month

def up_to_date(settings_file,server):
    site, API_key, package_id, settings =  open_a_channel(settings_file,server)
    coverage = get_package_parameter(site,package_id,'temporal_coverage',API_key)
    # temporal_coverage should be of the form '/09-30-17'
    if coverage == '' or coverage is None:
        return False
    covered_until = datetime.strptime(coverage.split('/')[1],'%m-%d-%y').date()
    eom = end_of_last_month() 
    if covered_until > eom:
        raise ValueError("temporal_coverage ({}) should not be later than the end of last month ({}).".format(covered_until,eom))
    return covered_until == eom

def update_temporal_coverage(settings_file,server):
    # Update the 'temporal coverage' package-level field, so it's clear that the ETL process has already
    # been done for the month.
    site, API_key, package_id, settings =  open_a_channel(settings_file,server)
    # temporal_coverage should be of the form '/09-30-17'
    covered_until = datetime.strftime(end_of_last_month(),"%m-%d-%y")
    set_package_parameters_to_values(site,package_id,['temporal_coverage'],['/'+covered_until],API_key)

def main(*args,**kwargs):
    # Check the 'temporal_coverage' package-level metadata field to see if the package has been updated for last month.
    server = kwargs.get('server','test-production')

    if up_to_date(SETTINGS_FILE,server):
        print("According to the 'temporal_coverage' metadata field, this package is up to date.")
        return

    # Get the latest files through FTP
    print("Pulling the latest liens data from the FTP server.")

    # Change path to script's path for cron job
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    local_path = dname+"/latest_pull"
    # If this path doesn't exist, create it.
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    search_terms = ['pitt1_','pitt_lien', 'pitt_sat']

    global DATA_PATH

    just_testing = False
    if just_testing:
        DATA_PATH = '/Users/daw165/data/TaxLiens/lien-machine/tmp' ## temporary (for testing)
        server = 'test-production'
    else:
        server = 'production'

    get_files_by_ftp = True
    if get_files_by_ftp:
        file_locations = fetch_files(SETTINGS_FILE,local_path,DATA_PATH,search_terms)

        print("file_locations = {}".format(file_locations))

    # STEP 2: caffeinate -i python /Users/daw165/data/TaxLiens/lien-machine/process_liens.py cv_m_pitt_lien_monYEAR.txt cv_m_pitt_sat_monYEAR.txt liens.db
        if len(file_locations) <= 2:
            raise ValueError("Not enough files found to complete the extraction.")
            # Strictly speaking only the pitt_lien and pitt_sat files are needed
            # for the extraction, but for some reason, we're downloading the pitt1_
            # file as well. 
        db_file_name = 'liens.db'
        process_liens.main(liens_file_path = file_locations[1], sats_file_path = file_locations[2], db_file_path = DATA_PATH+'/'+db_file_name)
    else: ###################################### temporary (for testing)
        db_file_name = 'liens.db'
        # The next line hard-codes particular files, just for testing purposes.
        process_liens.main(liens_file_path = DATA_PATH+'/'+'cv_m_pitt_lien_OCT2017.lst', sats_file_path = DATA_PATH+'/'+'cv_m_pitt_sat_OCT2017.lst', db_file_path = DATA_PATH+'/'+db_file_name)


# STEP 3: caffeinate -i python ../lien-machine/export_db_to_csv.py liens.db # It takes some number of minutes (maybe 10 or 20) to run this
    export_db_to_csv.main(db_file_path = DATA_PATH+'/'+db_file_name)
# STEP 4
#caffeinate -i python pipe_sats_from_csv.py /PATH/TO/raw-sats-liens.csv [clear_first]
#caffeinate -i python pipe_raw_lien_records_from_csv.py /PATH/TO/raw-liens.csv [clear_first] # Takes almost three hours to run.
#caffeinate -i python pipe_liens_from_csv.py /PATH/TO/synthesized-liens.csv [clear_first]
#caffeinate -i python pipe_summary_from_csv.py /PATH/TO/summary-liens.csv
    liens_input_file = DATA_PATH+'/synthesized-liens.csv'
    liens_resource_id, original_liens_url = pipe_liens_from_csv.main(input_file=liens_input_file, server=server)
    raw_liens_records_input_file = DATA_PATH+'/raw-liens.csv'
    raw_resource_id, original_raw_url = pipe_raw_lien_records_from_csv.main(input_file=raw_liens_records_input_file, server=server)
    sats_resource_id, _ = pipe_sats_from_csv.main(input_file=DATA_PATH+'/raw-sats-liens.csv', server=server)
    summary_resource_id, _ = pipe_summary_from_csv.main(input_file=DATA_PATH+'/summary-liens.csv', server=server)

    # This step could be accelerated if we exported and then piped to CKAN 
    # only the new changed rows, but that might not be so easy to figure out. 
    # The best way to do that might be to track in the database which rows 
    # have been successfully upserted (upsert some rows, check that the 
    # process finished without exception, and then flag all those rows
    # in the local database as upserted).
    print("Piped all new liens data to the {} server.".format(server))

# STEP 5 - Compress and deploy the raw lien records CSV file and the 
# liens CSV file as CKAN resources, and also switch the main resource 
# download link (if necessary) to download the zipped version.

    zip_and_deploy_file(settings_file=SETTINGS_FILE, server=server, 
            filepath=liens_input_file, 
            zip_file_name='liens-with-current-status-beta.zip', 
            resource_id=liens_resource_id,
            original_url=original_liens_url)
    zip_and_deploy_file(settings_file=SETTINGS_FILE, server=server,
            filepath=raw_liens_records_input_file, 
            zip_file_name='raw-liens-records-beta.zip', 
            resource_id=raw_resource_id,
            original_url=original_raw_url)

    update_temporal_coverage(SETTINGS_FILE,server)

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    if len(sys.argv) > 1:
        server = sys.argv[1]
        main(server = server)
    else:
        main()
