import sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time
#import yesterday

from parameters.local_parameters import SETTINGS_FILE

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
def main(target = None):
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Null - Raw tax-lien records to present (beta)'}
    #else:
        #kwargs = {'resource_id': ''}
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    if target is None:
        #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.
        target = '/Users/drw/WPRDC/Tax_Liens/lien_machine/testing/raw-seminull-test.csv'
    log = open('uploaded.log', 'w+')

    #test = yesterday.run()
    #if not test:
    #    exit(0)

    server = "production"
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)

    lien_and_mean_pipeline = pl.Pipeline('lien_and_mean_pipeline',
                                      'Raw Tax Liens Pipeline',
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(RawLiensSchema) \
        .load(pl.CKANDatastoreLoader, 'production',
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['dtd','lien_description','tax_year','pin','block_lot','assignee'],
              # A potential problem with making the pin field a key is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.
              method='upsert',
              **kwargs).run()
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished upserting {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished upserting {}\n".format(kwargs['resource_id']))
    log.close()

fields0 = RawLiensSchema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
       if len(sys.argv) > 1:
            target_file = sys.argv[1]
            main(target=target_file)
       else:
            main()
