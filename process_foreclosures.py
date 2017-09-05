import os
import sys
import re, csv
from util.the_pin import valid_pins
from datetime import datetime, date
import pprint
from process_liens import convert_blocklot_to_pin

from copy import deepcopy
from fixedwidth.fixedwidth import FixedWidth

# This code can be run like this:
# $ python process_foreclosures.py /path/to/foreclosures-data.txt

def write_to_csv(filename,list_of_dicts,keys): # Taken from utility_belt

    try:
        with open(filename, "w", encoding="utf-8") as output_file:
            dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
            dict_writer.writeheader()
            dict_writer.writerows(list_of_dicts)
    except: # Python 2 file opening
        with open(filename,'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
            dict_writer.writeheader()
            dict_writer.writerows(list_of_dicts)

def validate_input_files(filein1):
    errorstring = ""

    if filein1 == "":
        errorstring = errorstring + "This script requires a filename to be passed as a parameter.\n"

    if filein1[0:10] != "cv_m_pitt_" or filein1[len(filein1)-4:len(filein1)] not in [".txt", ".lst"]:
        errorstring = errorstring + "The first input file must be a foreclosures data file (of the form cv_m_pitt_[something].txt)\n"

    #monthyear = filein1[len(filein1)-11:len(filein1)-4]

    return errorstring

def parse_file(filein):
    with open(filein,'r') as f:
        rows = f.readlines()
    k = 0
    while rows[k] == '' or rows[k][0:8] == 'CASE_FIL' or re.match('^[- ]*$',rows[k]) is not None:
        k += 1 # Skip past any headers.
    
    first = k 
    config = {

        'filing_date': {
            'required': True,
            'type': 'string',
            'start_pos': 1,
            'end_pos': 9,
            'alignment': 'right',
            'padding': ' '
        },

        'case_id': {
            'required': True,
            'type': 'string',
            'start_pos': 10,
            'end_pos': 25,
            'alignment': 'left',
            'padding': ' '
        },

        'outdated_status': {
            'required': True,
            'type': 'string',
            'start_pos': 26,
            'end_pos': 76,
            'alignment': 'left',
            'padding': ' '
        },

        'party_type': {
            'required': True,
            'type': 'string',
            'start_pos': 77,
            'end_pos': 87,
            'alignment': 'left',
            'padding': ' '
        },

        'party_name': {
            'required': True,
            'type': 'string',
            'start_pos': 88,
            'end_pos': 148,
            'alignment': 'left',
            'padding': ' '
        },

        'block_lot': {
            'required': True,
            'type': 'string',
            'start_pos': 149,
            'end_pos': 169,
            'alignment': 'left',
            'padding': ' '
        },

        'amount': {
            'required': True,
            'type': 'numeric',
            'start_pos': 170,
            'end_pos': 182,
            'alignment': 'right',
            'padding': ' '
        },

        'municipality': {
            'required': True,
            'type': 'string',
            'start_pos': 183,
            'end_pos': 203,
            'alignment': 'left',
            'padding': ' '
        },

        'ward': {
            'required': True,
            'type': 'string',
            'start_pos': 204,
            'end_pos': 224,
            'alignment': 'left',
            'padding': ' '
        },
    }
    fw_config = deepcopy(config)
    fw_obj = FixedWidth(fw_config)
    list_of_dicts = []
    for n,row in enumerate(rows[first:]):
        if re.match('^\s*$',row) is None: # Filter out the empty
            # lines that sometimes appear in the raw data files.
            fw_obj.line = row
            values = fw_obj.data
            print("n = {}".format(n))
            try:
                values['amount'] = float(values['amount']) # This totally should not be
                # necessary, but FixedWith is not doing its job here.
            except:
                print("n = {}, amount = {}.".format(n,values['amount']))
                raise ValueError("")
            pin = convert_blocklot_to_pin(values['block_lot'],values['case_id'])
            if pin is not None:
                values['pin'] = pin

            values['filing_date'] = datetime.strptime(values['filing_date'], "%Y%m%d")
            values['filing_date'] = datetime.strftime(values['filing_date'], "%Y-%m-%d")
            # [ ] Convert date string to date type.

            del values['outdated_status']

            if values['party_type'] == 'Plaintiff':
                values['plaintiff'] = values['party_name']
                del values['party_name']
                del values['party_type']
                pprint.pprint(values)
                list_of_dicts.append(values)
            else:
                print("Somehow a non-Plaintiff row slipped into this file!")
    return list_of_dicts

def main(*args, **kwargs):
    print("kwargs = {}".format(kwargs))
    
    if 'input' in kwargs:
        filein1 = kwargs['input']
    else:
        try:
            print("sys.argv = {}".format(sys.argv))
            filein1 = sys.argv[1] # The file with the new foreclosures.
        except:
            filein1 = ""

    filename1 = filein1.split("/")[-1]

    dpath = '/'.join(filein1.split("/")[:-1]) + '/'
    if dpath == '/':
        dpath = ''

    errorstring = validate_input_files(filename1)

    errorstring = ""

    if errorstring != "":
        print(errorstring)
    else:
        new_foreclosures_file = filein1
        # Process new liens.
        list_of_dicts = parse_file(filein1)
        
        # Output results to a CSV file and then return the file path to the calling function.
        fields_to_write = ['pin','block_lot','filing_date','case_id','municipality','ward','amount','plaintiff']

        csv_path = dpath + 'csv/'
        print('os.path.isdir(csv_path) = {}'.format(os.path.isdir(csv_path)))
        if not os.path.isdir(csv_path):
            os.makedirs(csv_path) 
        output_file = csv_path + filename1 + ".csv"
        write_to_csv(output_file,list_of_dicts,fields_to_write)

        print("\nFiles processed successfully.")
        try:
            with open(dpath+'processed.log', 'a', encoding="utf-8") as processed:
                processed.write('Processed {}\n'.format(filein1))
        except: # Python 2 backup approach
            with open(dpath+'processed.log', 'ab') as processed:
                processed.write('Processed {}\n'.format(filein1))
        return output_file

if __name__ == "__main__":
    main()
