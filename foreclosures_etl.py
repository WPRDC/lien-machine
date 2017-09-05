import sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time
import process_foreclosures

from parameters.local_parameters import FORECLOSURE_SETTINGS_FILE

class ForeclosurePetitionSchema(pl.BaseSchema): # This schema supports raw lien records 
    # (rather than synthesized liens).
    pin = fields.String(dump_to="pin", allow_none=False)
    block_lot = fields.String(dump_to="block_lot", allow_none=False)
    filing_date = fields.Date(dump_to="filing_date", allow_none=True)
    case_id = fields.String(dump_to="case_id", allow_none=False)
    municipality = fields.String(dump_to="municipality", allow_none=True)
    ward = fields.String(dump_to="ward", allow_none=True)
    docket_type = fields.String(dump_to="docket_type", allow_none=True)
    amount = fields.Float(dump_to="amount", allow_none=True)
    #party_type = fields.String(dump_to="party_type", allow_none=True)
    #party_name = fields.String(dump_to="party_name", allow_none=True)
    plaintiff = fields.String(allow_none=False)
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
    def plaintiffs_only_and_avoid_null_keys(self, data):
        #if data['party_type'] != 'Plaintiff':
        #    data['party_type'] = '' # If you make these values
        #    # None instead of empty strings, CKAN somehow
        #    # interprets each None as a different key value,
        #    # so multiple rows will be inserted under the same
        #    # DTD/tax year/lien description even though the
        #    # property owner has been redacted.
        #    data['party_name'] = ''
        #    #data['party_first'] = '' # These need to be referred
        #    # to by their schema names, not the name that they
        #    # are ultimately dumped to.
        #    #data['party_middle'] = ''
        #    data['plaintiff'] = '' # A key field can not have value
        #    # None or upserts will work as blind inserts.
        #else:
        #    data['plaintiff'] = str(data['party_name'])
        #del data['party_type']
        #del data['party_name']
    # The stuff below was originally written as a separate function 
    # called avoid_null_keys, but based on the above warning, it seems 
    # better to merge it with omit_owners.
        if data['plaintiff'] is None: 
            data['plaintiff'] = ''
            print("Missing plaintiff")
        if data['block_lot'] is None:
            data['block_lot'] = ''
            print("Missing block-lot identifier")
            pprint.pprint(data)
        if data['pin'] is None:
            data['pin'] = ''
            print("Missing PIN")
            pprint.pprint(data)
        if data['case_id'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'case_id'")
        if data['docket_type'] is None:
            data['docket_type'] = ''
            pprint.pprint(data)
            print("Found a null value for 'docket_type'")


    @pre_load
    def fix_date(self, data):
        if data['filing_date']:
            try: # This may be the satisfactions-file format.
                data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d").date().isoformat()
            except:
                try:
                    data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S.%f").date().isoformat()
                except:
                    # Try the original summaries format
                    try:
                        data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S").date().isoformat()
                    except:
                        # Try the format I got in one instance when I exported the 
                        # data from CKAN and then reimported it:
                         data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%d-%b-%y").date().isoformat()
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
def main():
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Foreclosure filings (beta)'}
    #else:
        #kwargs = {'resource_id': ''}
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.

    # Call function that converts fixed-width file into a CSV file. The function 
    # returns the target file path.

    fixed_width_file = sys.argv[1]
#    target = '/Users/drw/WPRDC/Tax_Liens/foreclosure_data/raw-seminull-test.csv'
    target = process_foreclosures.main(input = fixed_width_file)
    print("target = {}".format(target))


    #test = yesterday.run()
    #if not test:
    #    exit(0)

    server = "production"
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(FORECLOSURE_SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)

    foreclosure_petition_pipeline = pl.Pipeline('foreclosure_petition_pipeline',
                                      'Pipeline for the Petitions for Foreclosures',
                                      log_status=False,
                                      settings_file=FORECLOSURE_SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(ForeclosurePetitionSchema) \
        .load(pl.CKANDatastoreLoader, 'production',
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['case_id','pin','block_lot','plaintiff'],
              # A potential problem with making the pin field a key is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.
              method='upsert',
              **kwargs).run()
    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished upserting {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished upserting {}\n".format(kwargs['resource_id']))
    log.close()

fields0 = ForeclosurePetitionSchema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
#fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
#fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))

if __name__ == "__main__":
   # stuff only to run when not called via 'import' here
    main()
