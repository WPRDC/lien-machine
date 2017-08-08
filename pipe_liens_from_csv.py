import datetime
from marshmallow import fields, pre_load, post_load
import pipeline as pl
from subprocess import call
import pprint
import time
#import yesterday

from parameters.local_parameters import SETTINGS_FILE

class RawLiensSchema(pl.BaseSchema):
    pin = fields.String(dump_to="pin", allow_none=True)
    block_lot = fields.String(dump_to="block_lot", allow_none=True)
    filing_date = fields.Date(dump_to="filing_date", allow_none=True)
    tax_year = fields.Integer(dump_to="tax_year", allow_none=True)
    dtd = fields.String(dump_to="dtd", allow_none=True)
    lien_description = fields.String(dump_to="lien_description", allow_none=True)
    municipality = fields.String(dump_to="municipality", allow_none=True)
    ward = fields.String(dump_to="ward", allow_none=True)
    last_docket_entry = fields.String(dump_to="last_docket_entry", allow_none=True)
    amount = fields.Float(dump_to="amount", allow_none=True)
    party_type = fields.String(dump_to="party_type", allow_none=True)
    party_name = fields.String(dump_to="party_name", allow_none=True)
    assignee = fields.String(allow_none=False)
    # Isn't it the case that 'assignee' was chosen as a field for the raw lien records because 
    # it is the only field that can be used to differentiate some records? In that case, maybe 
    # empty or '' assignee values should be replaced by '-' or ' ' or something. (This is based
    # on the previous finding/suspicion that both None value and empty strings resulted in keys
    # that would not work properly in certain cases.) And what about the case of integer fields
    # or other types?

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
        data['assignee'] = str(data['party_name'])
        del data['party_type']
        del data['party_name']
    # The stuff below was originally written as a  separate function 
    # called avoid_null_keys, but based on the above warning, it seems 
    # better to merge it with omit_owners.
        if data['assignee'] is None:
    #        data['assignee'] = ' '
            pprint.pprint(data)
            raise ValueError("Found a null value for 'assignee'")
        if data['dtd'] is None:
    #        data['dtd'] = ' '
            pprint.pprint(data)
            raise ValueError("Found a null value for 'dtd'")
        if data['lien_description'] is None:
    #        data['lien_description'] = ' '
            pprint.pprint(data)
            raise ValueError("Found a null value for 'lien_description'")
        if data['tax_year'] is None:
    #        data['tax_year'] = 0
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
#package_id = '22fe57da-f5b8-4c52-90ea-b10591a66f90' # This is
# the package created by the county previously to house liens
# data.

#package_id = '626e59d2-3c0e-4575-a702-46a71e8b0f25'
#package_id = '22fe57da-f5b8-4c52-90ea-b10591a66f90'
###############
# FOR SOME PART OF THE BELOW PIPELINE, I THINK...
#The package ID is obtained not from this file but from
#the referenced settings.json file when the corresponding
#flag below is True.
def main():
    resource_name = 'Raw tax-lien records to present (beta)'
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.
    target = '/Users/drw/WPRDC/Tax_Liens/lien_machine/testing/raw-liens-test.csv' # This path is also hard-coded.
    log = open('uploaded.log', 'w+')

    #test = yesterday.run()
    #if not test:
    #    exit(0)

    server = "production"
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']

    print("Preparing to pipe data from {} to package ID {} on {}".format(target,package_id,site))
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
              resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['dtd','lien_description','tax_year','pin','assignee'],
              # A potential problem with making the pin field a key (is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.)
              method='upsert').run()
    #          key_fields=['DTD','LIEN_DESCRIPTION','TAX_YEAR','PARTY_TYPE','PARTY_NAME','PARTY_FIRST','PARTY_MIDDLE'],
    log.write("Finished upserting {}\n".format(resource_id))
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
    main()
