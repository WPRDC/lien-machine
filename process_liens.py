import os.path
import sys
import re
from the_pin import valid_pins
from collections import defaultdict
from datetime import datetime, date
import dataset
# This version of the code just ignores matching of the PINs
# or block-lot numbers between liens and satisfactions.
#       Though if I made that decision based on the alternate
#       PINs, I could try to identify those weird PINs
#       (an example is 9905-X-82273-0000-00) and just do
#       Property-Assessment-Database lookups on those to
#       translate them to the regular PINs.
import pprint
from json import loads, dumps
import timeit

write_to_ckan = False

# The version of the code that maintained support for
# storing the processed data in text files is in the branch
# text_file_output. From here on, storage will be in a
# database (either a local database through dataset
# or a CKAN datastore)

# This code can be run like this:
# db-test drw$ python ../../taxliens.py pitt_lien_summaries.txt satisfactions.txt liens.db

def access_db(db_filename):
    #db = dataset.connect('sqlite:///liens.db')
    db = dataset.connect('sqlite:///'+db_filename)
    raw_table = db['raw_liens']
    active_table = db['active']
    sats_table = db['raw_satisfactions']
    return raw_table, active_table, sats_table

# Next steps:
# [ ] Test processing of summary files.
#   [X] Find a way to deal with DTD-03-037477, which has three block lot
# codes, two of which are different codes for the same PIN. (Upserts into the raw-liens table are covered by including the block lot in the unique keys. The active file is still problematic.)

# [ ] Split the master table into active and inactive tables, eliminating
# the 'satisfied' boolean and reducing the size of the table.
#   [ ] Plan B: Select on the 'satisfied' field to eliminate satisfied rows from being upserted
#   in the first place.

#   However, still problematic is how to delete from the Datastore liens when they get
#   satisfied.

#   Which brings us back to the idea of having a 'satisfied' field.
#   At least this will a) allow a liens map to be made pretty easily and b) simplify
#   getting liens into the property API.


# [ ] Implement pushing this stuff to CKAN.
#   [ ] Consider abstracting input and output, if possible.
#           Aside from the two table-printing loops at the end of the main function,
#           there's only four lines that directly interface with the database,
#           and they could be consolidated/abstratced into the matching_lien
#           function and the store_record_in_db function, so adding a CKAN mode
#           seems plausible.

# [ ] Make validate_input_files relevant to new scheme.

# [ ] Handle mutating the type of lien. For instance, a lien
# may initially be a City & School Tax Lien but then get
#   Satisfied as to City ONLY
# making it a School Tax Lien. This will just require some
# clever regexes.

def add_if_new(xs,x):
    if x in xs:
        return xs
    else:
        return xs.append(x)

def to_dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))

def add_years(d, years):
    """Return a date that's `years` years after the date (or datetime)
    object `d`. Return the same calendar date (month and day) in the
    destination year, if it exists, otherwise use the following day
    (thus changing February 29 to March 1)."""
    # from http://stackoverflow.com/questions/15741618/add-one-year-in-current-date-python
    try:
        return d.replace(year = d.year + years)
    except ValueError:
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))

def active_key_list():
    # Define a key to use for the liens database.
    # The tax year absolutely has to be included because of multi-year liens
    # that have liens from different years under the same docket.
    return ['DTD', 'description', 'tax_year']
    # Comment from the since-removed sat_key function:
        # In principle, if someone paid the same amount on a lien twice, I think
        # this might result in the same sat_key and therefore in a collision that
        # (based on code below) would prevent the second payment from being registered.

        # Adding the satisfaction type to this key would not necessarily work
        # because a SATISFACTION record can be duplicated with type SATISFIED
        # TO [WHATEVER] ONLY.

def dict_of_keys(record):
    return {key: record[key] for key in active_key_list()}

def tidy(s):
   return re.sub("\s+"," ",s.strip())

def repackage_parties(lien):
    # Instead of just eliminating the party information, let's detect the party
    # type and create either an "Assignee" field or a "Property owner" field.
    party_type = lien.pop('party_type', None)
    last_name = lien.pop('last_name', '')
    first_name = lien.pop('first_name', '')
    middle_name = lien.pop('middle_name', '')
    if first_name == None:
        first_name = ''
    if middle_name == None:
        middle_name = ''
    if last_name == None:
        last_name = ''

    allowed_party_types = ['Property Owner','Assignee']
    if party_type in allowed_party_types:
        lien[party_type] = tidy(' '.join([first_name,middle_name,last_name]))
    else:
        print("THIS IS A DISALLOWED PARTY TYPE. THE PARTY POLICE WILL NEVER STAND FOR THIS ({})!".format(party_type))
    return lien

def retype_fields(lien):
    # Coerce lien types.
    augmented_lien = lien
    # Convert dates from crazy string format:
    #   29-OCT-13
    # to a datetime.
    augmented_lien['filing_date'] = datetime.strptime(lien['filing_date'],"%d-%b-%y")
    # The filing date is the filing date of the lien
    # for the liens files but the filing date of the
    # satisfaction for the satisfaction files!
    augmented_lien['amount'] = float(lien['amount'])
    augmented_lien['tax_year'] = int(lien['tax_year'])
    return augmented_lien


def augment_lien(lien):
    # Add fields to the lien so that it can be added to the database:
    #   1) Add a SATISFIED field.
    #   2) Add an EXPIRES field.
    augmented_lien = lien
    augmented_lien['expires'] = add_years(augmented_lien['filing_date'],20)
    augmented_lien['satisfied'] = False

#    if augmented_lien['last_docket_entry'] in ['Satisfaction', 'Partial Exoneration',...]:
#       These types should never be showing up here anyhow, I think.
#        augmented_lien['satisfied'] = True
    return augmented_lien

def unsatisfied_db(lien,sats_table):
    # Return True only if the lien is completely satisfied. That is, the satisfaction
    # is one of the following types:
    satisfaction_types = ['Exoneration of Tax Lien', 'Satisfaction', 'Lien Entered in Error by Filer', 'Exoneration', 'Satisfied']
    match = matching_lien(lien,sats_table)
    if (match is None):
        return True
    elif match['last_docket_entry'] in satisfaction_types:
        return True
    else:
        return False

def form_lien_dict(linetosplit, fieldlist, filetype):
    # [The fieldlist argument is supporting some legacy functionality
    # that has been phased out.]

    # This function takes a line (in linetosplit) and uses the filetype
    # to split it into fields and construct a dict to access the elements.

    # If linetosplit is empty, the function instead uses the fields
    # in fieldlist to construct the lien dict.

	# At present, this script is just using the raw data in the file,
	# and is therefore not determining of including the PIN.

    # FIELD INDICES
    N_BLOCK_LOT = 0
    N_FILING_DATE = 1
    N_DTD = 2
    N_DESCRIPTION = 3
    N_MUNICIPALITY = 4
    N_WARD = 5
    N_LAST_DOCKET_ENTRY = 6
    N_AMOUNT = 7
    N_TAX_YEAR = 8
    N_PARTY_TYPE = 9
    N_PARTY_NAME = 10
    N_PARTY_FIRST = 11
    N_PARTY_MIDDLE = 12
    N_PROPERTY_DESCRIPTION = 13

    # SUMMARY FIELD INDICES (These are the corresponding indices for the one-month
    # files except that 1 has been added to account for the insertion of the
    # PIN field.)
    TAX_DTD = 3
    TAX_YEAR = 9
    TAX_AMOUNT = 8

    # SATISFACTION KEY FIELD INDICES
    SAT_DTD = 2
    SAT_YEAR = 3
    SAT_AMOUNT = 4


    lien = {}
    if linetosplit != '':
        # initialize field list
        fieldlist = ["", "", "", "", "", "", "", "", "", "", "", "", "", ""]

        #1-MONTH SATISFACTION FILES (e.g. cv_m_pitt_sat_JUL2013.txt)
        # COLUMN POSITIONS
        # 1   BLOCK_LOT
        # 22   FILING_DATE
        # 32   DTD
        # 48   DESCRIPTION
        # 79   MUNICIPALITY
        # 100   WARD
        # 121   LAST_DOCKET_ENTRY
        # 171   AMOUNT
        # 184   TAX_YEAR
        # 194   PARTY_TYPE
        # 210   PARTY_NAME
        # 271   PARTY_FIRST
        # 287   PARTY_MIDDLE
        # 303   PROPERTY_DESCRIPTION

        #1-MONTH LIEN FILES (e.g. cv_m_pitt_lien_AUG2013.txt)
        # COLUMN POSITIONS
        # 1   BLOCK_LOT
        # 22   FILING_DATE
        # 32   DTD
        # 48   DESCRIPTION
        # 79   MUNICIPALITY
        # 100   WARD
        # 121   LAST_DOCKET_ENTRY
        # 151   AMOUNT
        # 164   TAX_YEAR
        # 174   PARTY_TYPE
        # 190   PARTY_NAME
        # 251   PARTY_FIRST
        # 267   PARTY_MIDDLE
        # 283   PROPERTY_DESCRIPTION

        # fields common to all file types
        beg_block_lot = 1
        end_block_lot = 21

        beg_filing_date = 22
        end_filing_date = 31

        beg_dtd = 32
        end_dtd = 47

        beg_description = 48
        end_description = 78

        beg_municipality = 79
        end_municipality = 99

        beg_ward = 100
        end_ward = 120

        if filetype == TYPE_ONEMONTHSAT:

            beg_last_docket_entry = 121
            end_last_docket_entry = 170

            beg_amount = 171  # this position will support values up to $9B
            end_amount = 183

            beg_tax_year = 184
            end_tax_year = 193

            beg_party_type = 194
            end_party_type = 209

            beg_party_name = 210
            end_party_name = 270

            beg_party_first = 271
            end_party_first = 286

            beg_party_middle = 287
            end_party_middle = 302

            beg_property_description = 303
            end_property_description = 800

        elif filetype == TYPE_ONEMONTHLIEN:

            beg_last_docket_entry = 121
            end_last_docket_entry = 150

            beg_amount = 151  # this position will support values up to $9B
            end_amount = 163

            beg_tax_year = 164
            end_tax_year = 173

            beg_party_type = 174
            end_party_type = 189

            beg_party_name = 190
            end_party_name = 250

            beg_party_first = 251
            end_party_first = 266

            beg_party_middle = 267
            end_party_middle = 282

            beg_property_description = 283
            end_property_description = 800

        elif filetype == TYPE_SIXMONTHLIEN:

            beg_last_docket_entry = 121
            end_last_docket_entry = 173

            beg_amount = 174  # this position will support values up to $9B
            end_amount = 186

            beg_tax_year = 187
            end_tax_year = 198

            beg_party_type = 199
            end_party_type = 214

            beg_party_name = 215
            end_party_name = 275

            beg_party_first = 276
            end_party_first = 291

            beg_party_middle = 292
            end_party_middle = 307

            beg_property_description = 308
            end_property_description = 800


        # replace commas with spaces because values will be written to a csv file
        linetosplit = linetosplit.replace(",", " ")

        # retrieve individual field values
        fieldlist[N_BLOCK_LOT] = linetosplit[beg_block_lot - 1 : end_block_lot].strip()
        fieldlist[N_FILING_DATE] = linetosplit[beg_filing_date - 1 : end_filing_date].strip()
        fieldlist[N_DTD] = linetosplit[beg_dtd - 1 : end_dtd].strip()
        fieldlist[N_DESCRIPTION] = linetosplit[beg_description - 1 : end_description].strip()
        fieldlist[N_MUNICIPALITY] = linetosplit[beg_municipality - 1 : end_municipality].strip()
        fieldlist[N_WARD] = linetosplit[beg_ward - 1 : end_ward].strip()
        fieldlist[N_LAST_DOCKET_ENTRY] = linetosplit[beg_last_docket_entry - 1 : end_last_docket_entry].strip()
        fieldlist[N_AMOUNT] = linetosplit[beg_amount - 1 : end_amount].strip()
        fieldlist[N_TAX_YEAR] = linetosplit[beg_tax_year - 1 : end_tax_year].strip()
        fieldlist[N_PARTY_TYPE] = linetosplit[beg_party_type - 1 : end_party_type].strip()
        fieldlist[N_PARTY_NAME] = linetosplit[beg_party_name - 1 : end_party_name].strip()
        fieldlist[N_PARTY_FIRST] = linetosplit[beg_party_first - 1 : end_party_first].strip()
        fieldlist[N_PARTY_MIDDLE] = linetosplit[beg_party_middle - 1 : end_party_middle].strip()
        fieldlist[N_PROPERTY_DESCRIPTION] = linetosplit[beg_property_description - 1 : end_property_description].strip()

    offset = 0
    if filetype == TYPE_SUMMARY:
        offset = 1
    lien['block_lot'] = fieldlist[N_BLOCK_LOT+offset]
    lien['filing_date'] = fieldlist[N_FILING_DATE+offset]
    lien['DTD'] = fieldlist[N_DTD+offset]
    lien['description'] = fieldlist[N_DESCRIPTION+offset]
    lien['municipality'] = fieldlist[N_MUNICIPALITY+offset]
    lien['ward'] = fieldlist[N_WARD+offset]
    lien['last_docket_entry'] = fieldlist[N_LAST_DOCKET_ENTRY+offset] # This was previously 'type'
    # but 'type' is a bit of a misnomer based on how Version 1 of the data-dump process works.
    lien['amount'] = fieldlist[N_AMOUNT+offset]
    lien['tax_year'] = fieldlist[N_TAX_YEAR+offset]
    lien['party_type'] = fieldlist[N_PARTY_TYPE+offset]
    lien['last_name'] = fieldlist[N_PARTY_NAME+offset]
    lien['first_name'] = fieldlist[N_PARTY_FIRST+offset]
    lien['middle_name'] = fieldlist[N_PARTY_MIDDLE+offset]
    lien['property_description'] = fieldlist[N_PROPERTY_DESCRIPTION+offset]

    return lien


def convert_blocklot_to_pin(blocklot,dtd):
    # This function has been improved with respect to the
    # original function.

    empty_pin = ""
    # default PIN to all zeros
    pin = "0000000000000000"

    # set temp variables
    part123 = ""
    part45 = ""
    part1 = ""
    part2 = ""
    part3 = ""
    part4 = ""
    part5 = ""

    foundpart1 = False
    foundpart2 = False
    foundpart3 = False
    foundpart4 = False
    foundpart5 = False

    blocklot = blocklot.strip() # Remove whitespace from blocklot

    # check if the blocklot has hyphens/spaces or not (different processing if not present)

    count_hyphens = blocklot.count("-")
    count_spaces = blocklot.count(" ")

    if count_hyphens == 0 and count_spaces == 0:
        part123 = blocklot

    if count_hyphens > 0:
        parts = blocklot.split("-")
        if count_hyphens == 1:
            part123 = parts[0]
            part45 = parts[1]
        if count_hyphens == 2:
            part123 = parts[0]
            part4 = parts[1]
            part5 = parts[2]
        if count_hyphens == 4:
            part1 = parts[0]
            part2 = parts[1]
            part3 = parts[2]
            part4 = parts[3]
            part5 = parts[4]

    if count_spaces > 0:
        parts = blocklot.split(" ")
        if count_spaces == 1:
            part123 = parts[0]
            part45 = parts[1]
        if count_spaces == 2:
            part123 = parts[0]
            part4 = parts[1]
            part5 = parts[2]
        if count_spaces == 4:
            part1 = parts[0]
            part2 = parts[1]
            part3 = parts[2]
            part4 = parts[3]
            part5 = parts[4]


    if part123 != "":
        # three required sections (block part 1 and 2, lot)
        if len(part123) > 10:
            print("ERROR: block/lot >10 characters ({})".format(blocklot))
            return empty_pin

        counter = 0
        for letter in part123:
            counter += 1
            if not foundpart2 and letter.isalpha():
                # found part2, characters before part2 are part1, characters after part2 are part3
                part1 = part123[0:counter-1]
                part2 = part123[counter-1:counter]
                part3 = part123[counter:len(blocklot)]
                foundpart2 = True

        if not foundpart2:
            print("ERROR: no alphabetical character in {} ({})".format(blocklot,dtd))
            return empty_pin

        if foundpart2 and len(part1) == 0:
            print("ERROR: block part 1 not found in {} ({})".format(blocklot,dtd))
            return empty_pin

        if foundpart2 and len(part3) == 0:
            print("ERROR: lot not found in {} ({})".format(blocklot,dtd))
            return empty_pin

        if foundpart2 and part3.isalpha():
            print("ERROR: non-numeric lot in {} ({})".format(blocklot,dtd))
            return empty_pin

        # pad parts 1 and 3 with zeros
        part1 = part1.zfill(4)
        part3 = part3.zfill(5)

    if part45 != "":

        # two optional sections (unit/condo, tie-breaker)
        if len(part45) > 6:
            print("ERROR: unit/tiebreaker > 6 characters ({})".format(blocklot))
            return empty_pin

        if len(part45) > 2:
            part4 = part45[0:4]
            part5 = part45[4:6]
            if re.match('[A-Z]000',part45):
                # Note that
                #   re.match('pattern',s)
                # is equivalent to
                #   re.search('^pattern',s)

                # 236G132 A000 is a sort of erroneous representation of
                # what should be written as
                # 236G132 000A
                part4 = "000" + part45[0]
                part5 = "00"

        if len(part45) in [1,2]:
            if part45.isdigit():
                ## return "WARNING: ambiguous unit/tiebreaker value (" + blocklot + ")"
                ## Assuming that a single character is the unit/condo value
                ##part4 = part45
                # That assumption was wrong, at least in some cases.
                # When part45 is not a letter, we need to query the
                # Property Assessment database to disambiguate PINs.

                pin_candidates = []
                pin_candidates.append(part1 + part2 + part3 + part45.zfill(4) + "00")
                pin_candidates.append(part1 + part2 + part3 + "0000" + part45.zfill(2))
                pins = valid_pins(pin_candidates)
                if len(pins) == 1:
#                    print(blocklot + " => " + pins[0])
                    return pins[0]
                elif len(pins) == 0:
                    # Try handling cases like 307G395 99 => 0307G00395000909
                    if len(part45) == 2:
                        pin_candidates = []
                        pin_candidates.append(part1 + part2 + part3 + part45[0].zfill(4) + part45[1].zfill(2))
                        pins = valid_pins(pin_candidates)
                        if len(pins) == 1:
#                            print(blocklot + " => " + pins[0])
                            return pins[0]
                    print("WARNING: No corresponding PIN found for {} ({})".format(blocklot,dtd))
                    return empty_pin
                else:
                    print("WARNING: Multiple PINs found for {} ({}): {}".format(blocklot,dtd,pins))
                    return empty_pin
            else:
                part4 = part45

    # pad parts 4 and 5 with zeros
    part4 = part4.zfill(4)
    part5 = part5.zfill(2)

    # build pin from parts
    pin = part1 + part2 + part3 + part4 + part5

    return pin

def matching_lien(record,table):
    # This function returns a lien from the active table that
    # has the same key as lien_record. If none is found, the
    # function returns None.
    return table.find_one(**dict_of_keys(record))

def replace_character(s,ch):
    return re.sub(ch,'/',s)

def add_element_to_set_string(x,set_string):
    # To avoid the same name appearing multiple times in such a string, we will
    # regard it as a set and use the add_if_new() function to enfore this property.
    sep = '|' # character for delimiting sets
    x = replace_character(x,re.escape(sep))
    if set_string is None or set_string == '':
        return x
    xs = set_string.split(sep)
    add_if_new(xs,x)
    return sep.join(xs)

def fuse_parts(active_lien,lien_record):
    # For now, this is just for adding property owners to the active lien
    # or changing assignees (maybe).
    switched = None
    fused_lien = active_lien
    for field in lien_record.keys():
        if field in active_lien.keys():
            if active_lien[field] != lien_record[field]:
                if field in ['Property Owner', 'Assignee']:
                    owners = add_element_to_set_string(lien_record[field],active_lien[field])
                    fused_lien[field] = owners
                elif field in ['PIN', 'block_lot']:
                    # Switch to the non-alternative PIN/blocklot, if necessary
                    if active_lien[field][4] == 'X': # The blocklot (PIN) looks like
                        active_lien[field] = lien_record[field] # 9935X50083(000000)
                        switched = True
                    elif active_lien[field] is None or active_lien[field] == '':
                        active_lien[field] = lien_record[field]
                        switched = True
                    elif switched:
                        active_lien[field] = lien_record[field]
# What about handling this situation?:
#ERROR: no alphabetical character (2 E1)
#The field PIN doesn't match. The active lien has a value of 2000E00001000000 while the lien record has a value of
#The field block_lot doesn't match. The active lien has a value of 2000E1 while the lien record has a value of 2 E1

# Here, since we are sticking with the active lien's PIN,
# we should also stick with the active lien's block_lot.

# The above "switched" scheme is designed to take care of this.
# It takes advantages of the fact that 'PIN' always seems
# to get checked before block_lot.

                elif field != '_id':
                    if active_lien[field] is None or active_lien[field] == '':
                        active_lien[field] = lien_record[field]
                    elif lien_record[field] is not None and lien_record[field] != '':
                        print("The field {} doesn't match. The active lien has a value of {} while the lien record has a value of {}".format(field,active_lien[field],lien_record[field]))
                        fused_lien[field] = lien_record[field]
        else:
           print("The field {} is in the lien record but not in the active lien.".format(field))
           if field in ['Property Owner', 'Assignee']:
               fused_lien[field] = lien_record[field]
               print("... so let's add it.")
    return fused_lien

def process_types(some_record,active_table): # The lien_record is a dict.
    # Handle special processing that particular lien records may demand.

    # This function takes a lien record/filing and returns a lien (which may
    # involve looking up an old one and modifying some fields) which is to
    # be stored.

    # Summary types:

    # JULY-DECEMBER 2013 record types:
    # "               Tax Lien" (95436)
    # Suggestion/Averment Nonpayment (205)


    # Windwalker:pitt_lien_1995_2014 drw$ grep -v "Exoneration of Tax Lien" cv_m_pitt_sat_*2013.txt |grep -v "Satisfaction" |wc
    #j 9850  366432 10115950
    # The list of satisfaction types found in the 2013 files:
    # Satisfaction
    # Satisfied as to School ONLY
    # Satisfied as to Borough ONLY
    # Satisfied as to Township ONLY
    # Satisfied as to Library ONLY
    # Satisfied as to City ONLY
    # Exoneration of Tax Lien
    # Lien Entered in Error by Filer

    match = matching_lien(some_record,active_table)
    extant = match is not None
    new_types = ['Tax Lien', 'Pittsburgh Tax Lien (2 years)'
        'Pittsburgh Tax Lien (3 years)', 'Pittsburgh Tax Lien (4 years)',
        'Three Year Tax Lien', 'Two Year Tax Lien',
        'Reinstatement of Tax Lien']

    lien_record = dict(some_record) # Make a copy of the passed record
    # for local manipulation. Otherwise, if we get a satisfaction record
    # and add the 'expires' and 'satisfied' field in this function (when
    # augment_lien is called), those fields will also show up in the
    # database when it's # stored as a satisfaction.
    correction_types = ['Amended', 'Amendment', 'Correction', 'Docket Entry Entered in Error', 'Order of Court', 'Suggestion of Death', 'Tax Lien Amendment']
    if not extant:
        lien_record = augment_lien(lien_record) # Add 'expires' and 'satisfied' fields.

    lien_to_store = lien_record
    if lien_record['last_docket_entry'] in new_types:
        lien_to_store = lien_record
        if extant:
            # This is a case like
            #   Tax Lien    Property Owner  Doctor Impossible
            #   Tax Lien    Property Owner  Fred Impossible
            #   Tax Lien    Property Owner  Suzy Impossible
            #
            #...where we have to regard each line after the first one as an addition
            #to the lien already in the active-lien table (kind of like an assignment).

#            print(">>>>>>> Found a match for this lien I wanted to insert in the active database: {}".format(dict_of_keys(lien_record)))

            lien_to_store = fuse_parts(match,lien_record) # Pipe-delimit lists

    elif lien_record['last_docket_entry'] in correction_types:   # If this is a correction,
        # the active lien table should be updated too.
        if extant:
            lien_to_store = match
            # Run through the fields in the correction record and just copy all
            # those onto the active lien (except for the type, party information
            # (which may go in a separate table), and the filing_date (which should
            # maybe show up as a last_modified date in the active_table)).
            for field in lien_record.keys():
                if field in ['party_type','last_name','first_name','middle_name']:
                    # Update the parties, somehow...
                    # Based on v1.0 of the FTP files, it is not possible to determine
                    # whether a given 'Amended' docket entry means that the party
                    # is being added or deleted, though it generally seems that the first
                    # party listed in a file for a given DTD is a deleted one and the
                    # last one is an added one. However, this is all guesswork.

                    pass
                elif field not in ['filing_date','last_docket_entry']:
                    lien_to_store[field] = lien_record[field]

            print("\t\tUntil an updated version of the dumped records is obtained, it is impossible to correctly update the parties, and therefore these corrections may not be handled properly.")
        else:
            return lien_record, False # This will just add the lien_record as a
            # new active lien, even though this may not be the correct thing to do.
    elif lien_record['last_docket_entry'] == 'Suggestion/Averment Nonpayments':
        # Look up the matching base lien in the active table
        # and modify its 'expires' field.
        lien_to_store = matching_lien(lien_record)
        lien_to_store['expires'] = add_years(lien_record['filing_date'],20)

    # Code to handle other last_docket_entry values should go here...

    return lien_to_store, extant

def store_record_in_db(record,table,keys=active_key_list()):
    table.upsert(record, keys)
    return record

def store_lien_in_dbs(lien_record,raw_table,active_table):
    # The lien_record is a dict at this point.

    # 1) Store the lien information (which really may be part of a lien,
    # so some renaming may be merited) in the raw-liens database.
    # 2) If the lien is not already in the active database, store a
    # version of it there with SATISFIED and EXPIRES fields and with
    # parties stripped out.

	#PIN,BLOCK_LOT,FILING_DATE,DTD,DESCRIPTION,MUNICIPALITY,WARD,LAST_DOCKET_ENTRY,AMOUNT,TAX_YEAR,PARTY_TYPE,PARTY_NAME,PARTY_FIRST,PARTY_MIDDLE,PROPERTY_DESCRIPTION
    #list_of_unique_keys = list_of_active_keys + ['party_type', 'first_name', 'middle_name', 'last_name', 'description', 'amount', 'last_docket_entry', 'block_lot']
    raw_keys = active_key_list() + ['party_type', 'first_name', 'middle_name', 'last_name', 'description', 'amount', 'last_docket_entry', 'block_lot']
    lien_record = store_record_in_db(lien_record,raw_table,raw_keys)

    # ALSO MAYBE INSERT IT INTO THE MASTER DATABASE
    lien_to_store = repackage_parties(lien_record)
    active_lien, extant = process_types(lien_to_store,active_table)

    found_match = matching_lien(active_lien,active_table)
    if extant:
        active_table.update(active_lien, active_key_list()) # Could be changed to store_record_in_db
    elif found_match is None:
        active_table.insert(active_lien) # Could be changed to store_record_in_db

def delete_ckan_row(server,resource_id):
    #http://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_delete

    filters = {"Subway": "TRUE", "Number": 3}
    url = 'https://data.wprdc.org/api/action/datastore_search?resource_id={}&filters={}'.format(resource_id,filters)
    resource_id = ''
    url = 'https://demo.ckan.org/api/action/datastore_search?resource_id={}&filters={}'.format(resource_id,filters)

    #deletion_url = 'https://data.wprdc.org/api/action/datastore_delete?limit=5&q=title:jones'
    try:
        r = requests.get(URL)
    except: # If at first you don't succeed,
        time.sleep(0.1)
        r = requests.get(URL) # ... try, try again.
    if r.status_code != 200:
        r = requests.get(URL)
    if r.status_code == 200:
        records = json.loads(r.text)["result"]["records"]
        pprint.pprint(records)
    else:
        print("Uh oh! Status code = {}".format(r.status_code))

# Example of how to add rows to a CKAN datastore through the command line, curl, and a post request with a body/payload.

#http://stackoverflow.com/questions/17417835/using-ckan-datastore-search-rest-api-with-filters # Filters work if they are proper JSON (double-quoted values instead of single-quoted ones).


def process_satisfaction(sat,sats_table,active_table):
    store_record_in_db(sat,sats_table)
    match = matching_lien(sat,active_table)
    if match is not None:
        match['satisfied'] = True
        store_record_in_db(match,active_table)
        # To eliminate the 'satisfied' boolean, it's necessary
        # to delete the row from the corresponding database
        # (either the local database or CKAN).

def is_self_satisfied(lien,filetype,table):
    # Satisfactions and exonerations (and things that should be
    # thought of as such, including maybe Tax Lien Strike-Off,
    # despite the fact that we were told it was bogus) are
    # sometimes found in the older six-month summaries. To handle
    # that, this function looks at the record type (the last docket
    # entry field) and will override the filetype where necessary
    # so that the satisfaction can be used correctly.

    # table is the table that the function is looking in for a match.

    if filetype == TYPE_ONEMONTHSAT: # Also, a satisfaction without a corresponding
        match = matching_lien(lien,table)
        if match is None: # lien implies the existence of
            return True # the lien, so one should be created.
        # It should be noted that it's not clear there is any
        # evidence of such a satisfaction IN A SATISFACTIONS
        # FILE without a corresponding lien in a lien file.
        # (No satisfactions are for DTD numbers that predate 95).
        # The self-satisfying thing was invented to handle
        # detection of satisfactions in the liens file!

        clone = dict(lien)
        for_comparison = repackage_parties(clone)
        # If the satisfaction contains information not present in the corresponding
        # lien in the active table, make sure that that information gets included
        # in the active table. (This is a different way of looking at something
        # being self-satisfying, so perhaps a more precise name would be a
        # good idea.)
        if 'Property Owner' in match and 'Property Owner' in for_comparison:
            if match['Property Owner'] != for_comparison['Property Owner']:
                return True
        if 'Assignee' in match and 'Assignee' in for_comparison:
            if match['Assignee'] != for_comparison['Assignee']:
                return True
        return False

    satisfaction_types = ['Exoneration of Tax Lien', 'Satisfaction', 'Lien Entered in Error by Filer', 'Exoneration', 'Satisfied']

    # The below should only be for filetypes other than
    # one-month satisfaction files.
    if lien['last_docket_entry'] in satisfaction_types:
        return True
    return False

    #partial_satisfaction_types = ['Satisfied as to School ONLY', 'Satisfied as to Borough ONLY', 'Satisfied as to Township ONLY', 'Satisfied as to Library ONLY', 'Satisfied as to City ONLY', 'Partial Exoneration - Tax Lien']

def process_records(filename, filetype, raw_table, sats_table, active_table, raw_batch_insert_mode = False):
    record_type_count = defaultdict(int)

    # Re-examine whether these types are really bogus.
    # [ ] Go into the Court Records web site and look some of these up.
    bogus_record_types = ['Affidavit of Service', 'Amended Municipal Claim', 'Certificate of Service', 'Copy', 'Correction to Judgment Index', 'Mail Returned', 'Motion & Order', 'Notice', 'Suggestion of Bankruptcy', 'Tax Lien Strike-off', 'LAST_DOCKET_ENTRY']
    #   Ones that are (at least in some cases) NOT bogus:
    # Affidavit of Service # This matches exactly one lien which (coincidentally)
    #   was satisfied in Septembr of 2016... we just don't have that file yet.
    # Amended Municipal Claim
    # Certificate of Service
    # Copy # Goes with a docket where the docket text says "of Bankruptcy Order".
    #      # The corresponding satisfaction exists.

    # Notice # There are tons of these, and they don't seem to affect the lien's status.
    # Suggestion of Bankruptcy # In most cases, bankruptcy doesn't allow you to dodge
    # tax liens (as far as I understand).

    bogus_record_types = ['Correction to Judgment Index', 'Mail Returned', 'Motion & Order',
        'Tax Lien Strike-off', 'LAST_DOCKET_ENTRY', 'last_docket_entry']
    # I'm keeping Mail Returned since this is a case where the Satisfaction actually
    # occurred but was then buried by "Mail Returned" (DTD-97-004687), so it's better
    # to not put it in the database to begin with.

    # This one raises the issue that lots of Satisfactions for liens in the six-month
    # summary files might have been similarly buried.

    # Motion & Order: Many of these were striking the lien from the record.
    # This one actually isn't: DTD-10-004725. It just strikes one party from
    # the lien.

    # Correction to Judgment Index # Weirdly only occurs in the another DTD that
    # also has one of the Motion & Orders that does not itself strike the lien.

    # Tax Lien Strike-offs strike liens from the record, so this is correctly assigned
    # to bogus_record_types.

    # open input file
    filein = open(filename)
    count = 0
    satisfied = []
    if raw_batch_insert_mode:
        raw_liens_to_add = []
    for linein in filein:
        if linein.strip() != "":
            # Characterizing execution times of things in this loop reveals the following:
            # form_lien_dict execution time: 5.98430633545e-05
            # convert_blocklot_to_pin execution time: 0.000293970108032
            # store_lien_in_dbs execution time: 0.354511976242
            # Overall execution time: 0.361096858978

            if count % 1000 == 0:
                print("Working on line {}".format(count))
            # Convert raw data into a field-addressable dict

            lien = form_lien_dict(linein.strip(), None, filetype)

            # Convert block_lot to PIN.
            pin = convert_blocklot_to_pin(lien['block_lot'],lien['DTD'])
            if pin is not None:
                lien['PIN'] = pin
            lien = retype_fields(lien)
            # Eliminate one bogus combination of DTD and block_lot,
            # as this was verified by Court Records to be an anomalous
            # additional block_lot associated with this docket.
            save = not((lien['block_lot'] == '1180R62') and
                        (lien['DTD'] == 'DTD-03-037477'))

            if lien['last_docket_entry'] not in bogus_record_types:
                record_type_count[lien['last_docket_entry']] += 1

            if raw_batch_insert_mode:
                if filetype == TYPE_ONEMONTHSAT: # raw_batch_insert_mode is defaulting to
                # False when process_records is called for satisfactions, so this
                    self_satisfied = False # code is never
                else:                      # even called.
                    self_satisfied = is_self_satisfied(lien,filetype,raw_table)
                # Another option here would be to try
                # self_satisfied = is_self_satisfied(lien,filetype,raw_table)
                # but I'm not sure that would work.
                # 1) There's no evidence of self-satisfying
                # records in the satisfactions file.
                # 2) If we're using raw batch insert mode,
                # we could just build a check for satisfactions
                # into the end, but that would require intelligent
                # detection of satisfactions (which may have
                # a variety of docket-entry forms).
                # Thus, we'll try checking against the raw-liens
                # table.
            else:
                self_satisfied = is_self_satisfied(lien,filetype,active_table)
            # For those oddball lien records that both establish
            # their own existence and satisfaction, put the record
            # in the raw, raw_satisfactions, and inactive tables.

            # One drawback to how self-satisfaction works is that the
            # first satisfaction record establishes a lien with a
            # property owner, and then the next satisfaction record
            # sees the existing lien and is therefore not self-satisfying.
            if self_satisfied:
    #            if filetype == TYPE_ONEMONTHSAT: # maybe add "and raw_batch_insert_mode"
    #                print("{} is self-satisfying, but we're not going to copy it over to the raw liens table for this import.".format(lien['DTD']))
    #                self_satisfied = False
    #            else:
    # The above stuff was intended to expedite initial uploading of liens, but
    # since there are not that many liens that are self-satisfying, maybe it's OK
    # to try to let the satisfactions self-satisfy (and insert new liens) and ...
                if (dict_of_keys(lien) not in satisfied) and filetype != TYPE_ONEMONTHSAT:
                    print("{} is self-satisfying".format(lien['DTD']))

            if save and lien['last_docket_entry'] not in bogus_record_types:
                if (filetype != TYPE_ONEMONTHSAT and unsatisfied_db(lien,sats_table)) or self_satisfied:
                    if raw_batch_insert_mode:
                        raw_liens_to_add.append(lien)
                    else:
                        store_lien_in_dbs(lien, raw_table, active_table)
                    # Currently store_lien_in_dbs has all the logic
                    # for handling the processing of different
                    # docket entries for liens (under "process_types").
                    # Similar functionality will eventually be
                    # required for satisfactions.

                    # To convert this to something like
                    #       active_liens_to_add.append(process_lien(lien))
                    # it would be necessary to also query those liens to add
                    # whenever doing fusing operations on liens to make sure
                    # that any collisions are resolved. That is, it is not
                    # actually possible to bulk-add active liens unless you
                    # keep them ALL in memory or are pretty careful.



                if filetype == TYPE_ONEMONTHSAT:
                    if dict_of_keys(lien) not in satisfied:
                        process_satisfaction(lien, sats_table, active_table)
                        satisfied.append(dict_of_keys(lien))
                elif self_satisfied and filetype == TYPE_SIXMONTHLIEN:
                # Note that if a lien from the six-month summary
                # files is self-satisfying, the filing_date field
                # is the filing date of the original lien, but the
                # exact filing date of the satisfaction can not be
                # determined (unlike for liens that come from the
                # satisfaction files).
                    clone_for_satisfaction = dict(lien)
                    clone_for_satisfaction['filing_date'] = None
                    if dict_of_keys(lien) not in satisfied:
                        process_satisfaction(clone_for_satisfaction, sats_table, active_table)
                        satisfied.append(dict_of_keys(lien))
            count += 1

# In cases like 'Satisfied as to Borough ONLY', we could
# modify the description of the lien.

# For 'Void Satisfaction' (there are 2 of these), we
# could move the lien back from inactive to active.
# (For now, we will forget about having an inactive liens
# table, since in the 2 cases of Void Satisfaction (or other
# such cases), the lien can be reconstructed from the raw
# and satisfactions tables.)

    if raw_batch_insert_mode:
        raw_table.insert_many(raw_liens_to_add)
    print("record_type_count (after filtering out bogus types)= "+str(record_type_count))
    filein.close()


def validate_input_files(filein1, filein2, filein3):
    errorstring = ""

    if filein1 == "" or filein2 == "" or filein3 == "":
        errorstring = errorstring + "Script requires three filenames to be passed as parameters.\n"

    if filein1[0:15] != "cv_m_pitt_lien_" or filein1[len(filein1)-4:len(filein1)] not in [".txt", ".lst"]:
        errorstring = errorstring + "The first input file must be a tax lien text file (of the form cv_m_pitt_lien_MTHYEAR.txt)\n"

    if filein2[0:14] != "cv_m_pitt_sat_" or filein2[len(filein2)-4:len(filein2)] not in [".txt", ".lst"]:
        errorstring = errorstring + "The second input file must be a satisfaction text file (of the form cv_m_pitt_sat_MTHYEAR.txt)\n"

    lien_monthyear = filein1[len(filein1)-11:len(filein1)-4]
    sats_monthyear = filein2[len(filein2)-11:len(filein2)-4]

    if lien_monthyear != sats_monthyear:
        errorstring = errorstring + "The month and year for the lien and satisfaction files do not match. (" + lien_monthyear + "," + sats_monthyear + ")\n"

    return errorstring

def detect_format(new_liens_file):
    #TYPE_SIXMONTHLIEN = 1, TYPE_ONEMONTHLIEN = 3
    if re.match("pitt_lien_", new_liens_file):
        return TYPE_SIXMONTHLIEN
    else:
        return TYPE_ONEMONTHLIEN

def main():
    ### main code
    # Legacy comment:
    # "Originally, one script was written to process 6-month summary files,
    # monthly satisfaction files, and monthly lien files.  This script was
    # split into two scripts:  one to process summary files (one input file
    # at a time) and one to process ongoing monthly satisfaction/lien files
    # (two input files at a time).  Parts of the original structure remain
    # in these new scripts."

    try:
        filein1 = sys.argv[1] # The file with the new liens
    except:
        filein1 = ""

    try:
        filein2 = sys.argv[2] # The file with the new satisfactions
    except:
        filein2 = ""

    try:
        filein3 = sys.argv[3] # The file that contains the local database
    except:
        filein3 = ""

    filename1 = filein1.split("/")[-1]
    filename2 = filein2.split("/")[-1]
    db_filename = filein3.split("/")[-1]

    dpath = '/'.join(filein1.split("/")[:-1]) + '/'
    if dpath == '/':
        dpath = ''

    errorstring = validate_input_files(filename1, filename2, db_filename)

    errorstring = ""

    if errorstring != "":
        print errorstring
    else:
        new_liens_file = filein1
        filetype = detect_format(new_liens_file)
        new_sats_file = filein2
        raw_table, active_table, sats_table = access_db(db_filename)
        # Process new liens.
        process_records(new_liens_file, filetype, raw_table, sats_table, active_table, raw_batch_insert_mode = False)
        # Then process new satisfactions.
        if filein2 != "":
            process_records(new_sats_file, TYPE_ONEMONTHSAT, raw_table, sats_table, active_table)

        print("\nFiles processed successfully.")
        with open(dpath+'processed.log', 'ab') as processed:
            processed.write('Processed {}\n'.format(filein1))
            if filein2 != "":
                processed.write('Processed {}\n'.format(filein2))
        #print("The resulting active table is:")
        #active_list = active_table.all()
        #for lien in active_list:
        #    pprint.pprint(dict(lien))

        #print("The satisfactions table looks like this:")
        #for sat in sats_table.all():
        #    pprint.pprint(dict(sat))


        #print("The raw table looks like this:")
        #for r in raw_table.all():
        #    pprint.pprint(dict(r))
# FILE TYPES
TYPE_SIXMONTHLIEN = 1
TYPE_ONEMONTHSAT = 2
TYPE_ONEMONTHLIEN = 3
TYPE_SUMMARY = 4

if __name__ == "__main__":
    main()
