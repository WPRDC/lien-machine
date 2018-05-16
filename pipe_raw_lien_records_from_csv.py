import re, sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time
import ckanapi

from parameters.local_parameters import SETTINGS_FILE

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
        desired_string = metadata[parameter]
        #print("The parameter {} for this resource is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

    return desired_string

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Stolen from utility-belt.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

    return desired_string

def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name. (Stolen from utility-belt).
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None


class RawLiensSchema(pl.BaseSchema): # This schema supports raw lien records 
    # (rather than synthesized liens).
    pin = fields.String(dump_to="pin", allow_none=False)
    block_lot = fields.String(dump_to="block_lot", allow_none=False)
    filing_date = fields.Date(dump_to="filing_date", allow_none=True)
    tax_year = fields.Integer(dump_to="tax_year", allow_none=False)
    dtd = fields.String(dump_to="dtd", allow_none=False)
    lien_description = fields.String(dump_to="lien_description", allow_none=False)
    municipality = fields.String(dump_to="municipality", allow_none=True)
    ward = fields.String(dump_to="ward", allow_none=True)
    last_docket_entry = fields.String(dump_to="last_docket_entry", allow_none=True)
    amount = fields.Float(dump_to="amount", allow_none=True)
    party_type = fields.String(dump_to="party_type", allow_none=True)
    party_name = fields.String(dump_to="party_name", allow_none=True)
    assignee = fields.String(allow_none=False)
    # Isn't it the case that 'assignee' was chosen as a field for the raw lien records because 
    # it is the only field that can be used to differentiate some records? 

    # Never let any of the key fields have None values. It's just asking for 
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no 
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.

#    party_first = fields.String(dump_to="first_name", allow_none=True)
#    party_middle = fields.String(dump_to="middle_name", allow_none=True)
    # It may be possible to exclude party_first and party_middle
    # if they are never present for lien holders (but that may
    # not be so if the lien holder is an individual).
    #property_description = fields.String(dump_to="property_description", allow_none=True)


    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same 
    #   type is not guaranteed. If you need to guarantee order of different 
    #   processing steps, you should put them in the same processing method.
    @pre_load
    def omit_owners_and_avoid_null_keys(self, data):
        if data['party_type'] == 'Property Owner':
            data['party_type'] = '' # If you make these values
            # None instead of empty strings, CKAN somehow
            # interprets each None as a different key value,
            # so multiple rows will be inserted under the same
            # DTD/tax year/lien description even though the
            # property owner has been redacted.
            data['party_name'] = ''
            #data['party_first'] = '' # These need to be referred
            # to by their schema names, not the name that they
            # are ultimately dumped to.
            #data['party_middle'] = ''
            data['assignee'] = '' # A key field can not have value
            # None or upserts will work as blind inserts.
        else:
            data['assignee'] = str(data['party_name'])
        del data['party_type']
        del data['party_name']
    # The stuff below was originally written as a separate function 
    # called avoid_null_keys, but based on the above warning, it seems 
    # better to merge it with omit_owners.
        if data['assignee'] is None: 
            #data['assignee'] = '' # This should never happen based
            # on the above logic. This is just a double check to 
            # ensure the key fields are OK.
           pprint.pprint(data)
           raise ValueError("Found a null value for 'assignee'")
        if data['block_lot'] is None:
            data['block_lot'] = ''
            print("Missing block-lot identifier")
            pprint.pprint(data)
        if data['pin'] is None:
            data['pin'] = ''
            print("Missing PIN")
            pprint.pprint(data)
        if data['dtd'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'dtd'")
        if data['lien_description'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'lien_description'")
        if data['tax_year'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'tax_year'")


    @pre_load
    def fix_date(self, data):
        if data['filing_date']:
            try: # This may be the satisfactions-file format.
                data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S").date().isoformat()
            except:
                try:
                    data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S.%f").date().isoformat()
                except:
                    # Try the original summaries format
                    try:
                        data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%d-%b-%y").date().isoformat()
                    except:
                        # Try the format I got in one instance when I exported the 
                        # data from CKAN and then reimported it:
                        data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d").date().isoformat()
        else:
            print("No filing date for {} and data['filing_date'] = {}".format(data['dtd'],data['filing_date']))
            data['filing_date'] = None

# Resource Metadata
#package_id = '626e59d2-3c0e-4575-a702-46a71e8b0f25'     # Production
#package_id = '85910fd1-fc08-4a2d-9357-e0692f007152'     # Stage
###############
# FOR SOME PART OF THE BELOW PIPELINE, I THINK...
#The package ID is obtained not from this file but from
#the referenced settings.json file when the corresponding
#flag below is True.
def transmit(**kwargs):
    target = kwargs.pop('target') # raise ValueError('Target file must be specified.')
    update_method = kwargs.pop('update_method','upsert')
    if 'schema' not in kwargs:
        raise ValueError('A schema must be given to pipe the data to CKAN.')
    schema = kwargs.pop('schema')
    key_fields = kwargs['key_fields']
    if 'fields_to_publish' not in kwargs:
        raise ValueError('The fields to be published have not been specified.')
    fields_to_publish = kwargs.pop('fields_to_publish')
    server = kwargs.pop('server', 'production')
    pipe_name = kwargs.pop('pipe_name', 'generic_liens_pipeline_name')
    clear_first = kwargs.pop('clear_first', False) # If this parameter is true,
    # the datastore will be deleted (leaving the resource intact).

    log = open('uploaded.log', 'w+')


    # There's two versions of kwargs running around now: One for passing to transmit, and one for passing to the pipeline.
    # Be sure to pop all transmit-only arguments off of kwargs to prevent them being passed as pipepline parameters.

    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']

    if 'resource_name' in kwargs:
        resource_specifier = kwargs['resource_name']
        original_resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)
    else:
        resource_specifier = kwargs['resource_id']
        original_resource_id = kwargs['resource_id']

    try:
        original_url = get_resource_parameter(site,original_resource_id,'url',API_key)
    except RuntimeError:
        print("Exception thrown when trying to obtain original_url.")
        original_url = None

    # It's conceivable that original_resource_id may not match resource_id (obtained
    # below), in instances where the resource needs to be created by the pipeline.

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,resource_specifier,package_id,site))
    time.sleep(1.0)

    print("fields_to_publish = {}".format(fields_to_publish))
    lien_and_mean_pipeline = pl.Pipeline(pipe_name,
                                      pipe_name,
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              clear_first=clear_first,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              #key_fields=['dtd','lien_description','tax_year','pin','block_lot','assignee'],
              # A potential problem with making the pin field a key is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.
              method=update_method,
              **kwargs).run()

    if 'resource_name' in kwargs:
        resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)
    else:
        resource_id = kwargs['resource_id']
    
    print("Piped data to {} on the {} server".format(resource_specifier,server))
    log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),resource_specifier))
    log.close()
    return resource_id, original_url

schema = RawLiensSchema
key_fields = ['dtd','lien_description','tax_year','pin','block_lot','assignee']
fields0 = schema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0

def main(*args,**kwargs):

    server = kwargs.get('server','test-production')
    if server == 'production':
        kwparams = dict(resource_id='8cd32648-757c-4637-9076-85e144997ca8', schema=schema, key_fields=key_fields, server=server, pipe_name='raw_tax_liens_pipeline', fields_to_publish = fields0)
    else:
        kwparams = dict(resource_name='Raw tax-lien records to present (gamma)', schema=schema, key_fields=key_fields, server=server, pipe_name='raw_tax_liens_pipeline', fields_to_publish=fields0)

    target_file = kwargs.get('input_file',None)
    if target_file is None:
        if len(sys.argv) > 1:
            target_file = sys.argv[1]
        else:
            raise ValueError("No target specified.")

    clear_first = kwargs.get('clear_first', False)
    kwparams['clear_first'] = clear_first

    if len(sys.argv) > 2:
        if sys.argv[2] == 'clear_first':
            kwparams['clear_first'] = True
        else:
            raise ValueError("Unrecognized second argument")
    resource_id, original_url = transmit(target=target_file, **kwparams)
    return resource_id, original_url

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
