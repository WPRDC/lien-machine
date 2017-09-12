import re, sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time

from pipe_raw_lien_records_from_csv import RawLiensSchema

from parameters.local_parameters import SETTINGS_FILE

class LiensSchema(RawLiensSchema): # This schema supports liens that have been
    # synthesized from the raw-liens records. 
    satisfied = fields.Boolean(dump_to="satisfied", allow_none=True)
    # Never let any of the key fields have None values. It's just asking for 
    # multiplicity problems on upsert.

    # Another way to guard against inserts would be to change the method to 'update'.

    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same 
    #   type is not guaranteed. If you need to guarantee order of different 
    #   processing steps, you should put them in the same processing method.
    @pre_load
    def omit_owners_and_avoid_null_keys(self, data):
        if data['assignee'] is None: 
            data['assignee'] = ''
        if data['pin'] is None and data['block_lot'] is None:
            print("No block_lot or pin value found")
            pprint.pprint(data)
        if data['pin'] is None:
            data['pin'] = ''
        if data['block_lot'] is None:
            data['block_lot'] = ''
        if data['dtd'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'dtd'")
        if data['lien_description'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'lien_description'")
        if data['tax_year'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'tax_year'")

def main(target=None,update_method='upsert'):
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Tax liens to present (beta)'}
    #else:
        #kwargs = {'resource_id': ''}
    if target is None:
        raise ValueError('Target file must be specified.')
    log = open('uploaded.log', 'w+')

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
                                      'Tax Liens Pipeline',
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(LiensSchema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['dtd','lien_description','tax_year','block_lot','pin'],
              # A potential problem with making the pin field a key is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.
              method=update_method,
              **kwargs).run()
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),kwargs['resource_id']))
    log.close()

fields0 = LiensSchema().serialize_to_ckan_fields()
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
