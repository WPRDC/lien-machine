import datetime
from marshmallow import fields, pre_load, post_load
import pipeline as pl
from subprocess import call
import pprint
#import yesterday

SETTINGS_FILE = '/Users/daw165/data/TaxLiens/etl/settings-liens.json'

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
    assignee = fields.String(allow_none=True)
#    party_first = fields.String(dump_to="first_name", allow_none=True)
#    party_middle = fields.String(dump_to="middle_name", allow_none=True)
    # It may be possible to exclude party_first and party_middle
    # if they are never present for lien holders (but that may
    # not be so if the lien holder is an individual).
    #property_description = fields.String(dump_to="property_description", allow_none=True)


    class Meta:
        ordered = True

    @pre_load
    def omit_owners(self, data):
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
                        # Try the format I got in one instance when I exported the data from CKAN and then reimported it:
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
    #resource_name = 'Raw tax-lien records (beta 7)'
    resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv'
    log = open('uploaded.log', 'w+')

    #test = yesterday.run()
    #if not test:
    #    exit(0)

    lien_and_mean_pipeline = pl.Pipeline('lien_and_mean_pipeline',
                                      'Tax Liens Pipeline',
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
              key_fields=['dtd','lien_description','tax_year','pin', 'assignee'],
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
