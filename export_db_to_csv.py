# cd to the directory with liens.db in it and run this script as follows
# > python ../lien-machine/export_db_to_csv.py liens.db
# OR it also works like this:
# > python export_db_to_csv.py testing/liens.db
# In both cases, the CSV files get deposited in the same directory as the database.
import sys
import csv
import re
import dataset

def write_or_append_to_csv(filename,list_of_dicts,keys):
    # Stolen from parking-data util.py file.
    if not os.path.isfile(filename):
        with open(filename, 'wb') as g:
            g.write(','.join(keys)+'\n')
    with open(filename, 'ab') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def main(*args, **kwargs)
    db_file = kwargs.get('db_file_path',sys.argv[1])
    db = dataset.connect('sqlite:///'+db_file)
    if '/' in db_file:
        path, filename = db_file.rsplit('/',1)
        path += '/'
    else:
        path = ''
        filename = db_file
    name = re.sub("\..*","",filename) #name = re.sub("\..*","",db_file)

    # export all users into a single JSON
    #result = db['raw_liens'].all()
    #dataset.freeze(result, format='json', filename='users.json')
    #dataset.freeze(result, format='csv', filename='raw-liens-2013-2017.csv')

    #output_table_to_csv(db,table_name,output_file)

    #sorted_raw = db.query('SELECT * FROM raw_liens ORDER BY filing_date ASC, DTD ASC, tax_year ASC, description ASC, party_type DESC;')
    #PIN,BLOCK_LOT,FILING_DATE,DTD,LIEN_DESCRIPTION,MUNICIPALITY,WARD,LAST_DOCKET_ENTRY,AMOUNT,TAX_YEAR,PARTY_TYPE,PARTY_NAME,PARTY_FIRST,PARTY_MIDDLE,PROPERTY_DESCRIPTION
    print("Accessing raw-liens database.")
    sorted_raw = db.query('SELECT PIN as PIN, block_lot as BLOCK_LOT, filing_date as FILING_DATE, DTD as DTD, description as LIEN_DESCRIPTION, municipality as MUNICIPALITY, ward as WARD, last_docket_entry as LAST_DOCKET_ENTRY, amount as AMOUNT, tax_year as TAX_YEAR, party_type as PARTY_TYPE, last_name as PARTY_NAME, first_name as PARTY_FIRST, middle_name as PARTY_MIDDLE, property_description as PROPERTY_DESCRIPTION FROM raw_liens ORDER BY filing_date ASC, tax_year ASC, DTD ASC, description ASC, party_type DESC;')
    dataset.freeze(sorted_raw, format='csv', filename= path + 'raw-' + name + '.csv')

    print("Wrote raw liens to a CSV file.")

    # These are all liens, active and inactive (i.e., "Liens with current status").
    sorted_liens = db.query('SELECT PIN as PIN, block_lot as BLOCK_LOT, filing_date as FILING_DATE, DTD as DTD, description as LIEN_DESCRIPTION, municipality as MUNICIPALITY, ward as WARD, last_docket_entry as LAST_DOCKET_ENTRY, amount as AMOUNT, tax_year as TAX_YEAR, assignee as ASSIGNEE, property_description as PROPERTY_DESCRIPTION, satisfied as SATISFIED FROM liens ORDER BY filing_date ASC, tax_year ASC, DTD ASC, description ASC, assignee DESC, satisfied DESC;')
    dataset.freeze(sorted_liens, format='csv', filename= path + 'synthesized-' + name + '.csv')

    sorted_sats = db.query('SELECT PIN as PIN, block_lot as BLOCK_LOT, filing_date as FILING_DATE, DTD as DTD, description as LIEN_DESCRIPTION, municipality as MUNICIPALITY, ward as WARD, last_docket_entry as LAST_DOCKET_ENTRY, amount as AMOUNT, tax_year as TAX_YEAR, party_type as PARTY_TYPE, last_name as PARTY_NAME, first_name as PARTY_FIRST, middle_name as PARTY_MIDDLE, property_description as PROPERTY_DESCRIPTION FROM raw_satisfactions ORDER BY filing_date ASC, tax_year ASC, DTD ASC, description ASC, party_type DESC;')
    # When a lien comes from a six-month summary file and
    # has a last docket entry that is a satisfaction, the filing
    # date is the date of the lien and so the filing date of the
    # satisfaction may be unknown. Thus, we might consider making
    # tax year the first thing to sort on.
    dataset.freeze(sorted_sats, format='csv', filename= path + 'raw-sats-' + name + '.csv')

    summary_table = db.query("SELECT PIN as pin, COUNT(*) as number, ROUND(SUM(AMOUNT),2) as total_amount FROM liens WHERE PIN <> '' AND NOT(SATISFIED) GROUP BY PIN;")
    dataset.freeze(summary_table, format='csv', filename=path + 'summary-' + name + '.csv')
    # At last count, there were between 9 and 10 thousand liens with no identified PIN 
    # (on the order of 1% of all liens), totalling about 2 million dollars.

if __name__ == "__main__":
    main()
