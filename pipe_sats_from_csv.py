import re, sys, json, datetime, pprint, time
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl')
import pipeline as pl
from subprocess import call
import pipe_raw_lien_records_from_csv as pipe

from parameters.local_parameters import SETTINGS_FILE

schema = pipe.RawLiensSchema
key_fields = pipe.key_fields
fields0 = schema().serialize_to_ckan_fields()
fields_to_publish = pipe.fields_to_publish
print("fields_to_publish = {}".format(fields_to_publish))


def main(*args,**kwargs):
    server = kwargs.get('server','test-production')
    if server == 'production':
        kwparams = dict(resource_id='346a4e9d-e72d-4701-b881-bd95cc7d0f5a', schema=schema, key_fields=key_fields, server=server, pipe_name='sats_pipeline', fields_to_publish=fields_to_publish)
    else:
        kwparams = dict(resource_name='Tax-lien satisfaction records to present (alpha)', schema=schema, key_fields=key_fields, server=server, pipe_name='sats_pipeline', fields_to_publish=fields_to_publish)

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
