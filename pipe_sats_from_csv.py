import re, sys, json, datetime, pprint, time
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl')
import pipeline as pl
from subprocess import call
from pipe_raw_lien_records_from_csv import RawLiensSchema, fields_to_publish

from parameters.local_parameters import SETTINGS_FILE

def main(target=None,update_method='upsert'):
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Null - Tax-lien satisfaction records 1995 to present (beta)'}
    #else:
        #kwargs = {'resource_id': ''}
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    if target is None:
        #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.
        target = '/Users/drw/WPRDC/Tax_Liens/lien_machine/testing/raw-sats-liens-test.csv'
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

    sats_pipeline = pl.Pipeline('sats_pipeline',
                                      'Tax-Lien Satisfactions Pipeline',
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(RawLiensSchema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['dtd','lien_description','tax_year','pin','block_lot'],
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

fields0 = RawLiensSchema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))
#actual_fields = [f['id'] for f in fields_to_publish]
#for key in key_fields:
#    if key not in actual_fields:
#        raise ValueError("This is never going to work because {} is not in the list of fields to publish ({})".format(key,actual_fields))

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
       if len(sys.argv) > 1:
            target_file = sys.argv[1]
            main(target=target_file)
       else:
            main()
