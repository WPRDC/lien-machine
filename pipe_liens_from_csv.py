import re, sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time

import pipe_raw_lien_records_from_csv as pipe

from parameters.local_parameters import SETTINGS_FILE

class LiensSchema(pipe.RawLiensSchema): # This schema supports liens that have been
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


schema = LiensSchema
key_fields = ['dtd','lien_description','tax_year','block_lot','pin']
fields0 = schema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0
#print("fields_to_publish = {}".format(fields_to_publish))

def main(*args,**kwargs):
    server = kwargs.get('server','test-production')
    if server == 'production':
        kwparams = dict(resource_id='65d0d259-3e58-49d3-bebb-80dc75f61245', schema=schema, key_fields=key_fields, server=server, pipe_name='tax_liens_pipeline', fields_to_publish=fields_to_publish)
    else:
        kwparams = dict(resource_name='Tax liens to present (alpha)', schema=schema, key_fields=key_fields, server=server, pipe_name='tax_liens_pipeline', fields_to_publish=fields_to_publish)

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
    resource_id = pipe.transmit(target=target_file, **kwparams)
    return resource_id

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
