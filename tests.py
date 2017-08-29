import unittest

from process_liens import convert_blocklot_to_pin

class MyTest(unittest.TestCase):
    def test_base_blocklot_conversion(self):
#       The first column in the tax-liens files is a property
#       identifier called "BLOCK_LOT". For cases where there is no
#       space or dash in the Block Lot number, zeros can be added to
#       generate the corresponding 16-character PIN:

#        For example,
#            558H67 => 0558-H-00067-0000-00

        self.assertEqual(convert_blocklot_to_pin("1360A268","0"), "1360A00268000000")
        self.assertEqual(convert_blocklot_to_pin("352B94","1"), "0352B00094000000")
        self.assertEqual(convert_blocklot_to_pin("159C221","2"), "0159C00221000000")
        self.assertEqual(convert_blocklot_to_pin("159D250","3"), "0159D00250000000")
        self.assertEqual(convert_blocklot_to_pin("2014E81","4"), "2014E00081000000")
        self.assertEqual(convert_blocklot_to_pin("233F181","5"), "0233F00181000000")
        self.assertEqual(convert_blocklot_to_pin("214G46","6"), "0214G00046000000")
        self.assertEqual(convert_blocklot_to_pin("558H67","7"), "0558H00067000000")
        self.assertEqual(convert_blocklot_to_pin("2013J377","8"), "2013J00377000000")
        self.assertEqual(convert_blocklot_to_pin("214L128","9"), "0214L00128000000")
        self.assertEqual(convert_blocklot_to_pin("430M330","10"), "0430M00330000000")
        self.assertEqual(convert_blocklot_to_pin("215N56","11"), "0215N00056000000")
        self.assertEqual(convert_blocklot_to_pin("161P50","12"), "0161P00050000000")
        self.assertEqual(convert_blocklot_to_pin("214R184","13"), "0214R00184000000")
        self.assertEqual(convert_blocklot_to_pin("161S334","14"), "0161S00334000000")
        # 8000T[0-9]* represents a trailer.

    def test_anomalous_blocklot_conversion(self):
#       However, when there is a space in the Block Lot number,
#       the characters after the space appear in the fourth part
#       of the PIN in some cases...

#            232L374 2 => 0232-L-00374-0002-00
#                                         ^
        self.assertEqual(convert_blocklot_to_pin("232L374 2","15"), "0232L00374000200")
        # [This turns out to be the default assumption for the original
        # script.]
#       ... and in the fifth part in other cases

#            233J211 2 => 0233-J-00211-0000-02
#                                            ^
        self.assertEqual(convert_blocklot_to_pin("233J211 2","16"), "0233J00211000002")

        self.assertEqual(convert_blocklot_to_pin("214L167 1","17"), "0214L00167000100")
        self.assertEqual(convert_blocklot_to_pin("236G99 A","18"), "0236G00099000A00")


        # The tax-liens file describes the following Block Lot number as
        #   THE SUMMIT CONDO LOT 130X192.80 CALIFORNIA AVE PT 7 STY BRK HI RISE APT BLDG UNIT 307
        # The Property Assessment database describes the corresponding PIN as a condominium
        #   "THE SUMMIT CONDO LOT 130 X 192.80 CALIFORNIA","AVE","PT 7 STY BRK HI RISE APT BLDG UNIT 307",
        # So, condominia are considered properties, even though the owner
        # many not own the ground-floor-level data.
        self.assertEqual(convert_blocklot_to_pin("160A154 307","19"), "0160A00154030700")
        self.assertEqual(convert_blocklot_to_pin("160A132 104","20"), "0160A00132010400")
        self.assertEqual(convert_blocklot_to_pin("160J60 708","21"), "0160J00060070800")
        self.assertEqual(convert_blocklot_to_pin("329G20 27D","22"), "0329G00020027D00")
        self.assertEqual(convert_blocklot_to_pin("58P100 T1","23"), "0058P0010000T100")

        # Two-digit number at the end:
        self.assertEqual(convert_blocklot_to_pin("215J110 01","24"), "0215J00110000001")


        # Anomalous case where 99B175 21 translates to
        # 0099-B-00175-0002-01, but there's also PINs like this:
        # 0099-B-00175-0026-00, so there is a chance that if we
        # look for PINs of the former shape, we may find false
        # positives and screw up our counts more. Therefore,
        # we will only do this last transformation as a last resort.
        self.assertEqual(convert_blocklot_to_pin("307G395 99","25"), "0307G00395000909")
        self.assertEqual(convert_blocklot_to_pin("99B175 21","26"), "0099B00175000201")
        self.assertEqual(convert_blocklot_to_pin("10G235 21","27"), "0010G00235000201")
        # Completely anomalous and difficult to categorize:

        # Four alphanumeric characters after the space:
        self.assertEqual(convert_blocklot_to_pin("236G132 A000","28"), "0236G00132000A00") #<== Possibly this one is a case of human error, but it has happened for many different properties (~25). All cases (which occurred between 2012 and 2014 inclusive), have been manually checked to verify that the new rule concocted to handle this case holds for all such Block Lot numbers.

        self.assertEqual(convert_blocklot_to_pin("37N210 A463","29"),"0037N00210A46300")
        # Four-digit number after the space:
        self.assertEqual(convert_blocklot_to_pin("100C50 4099","30"), "0100C00050409900")
        self.assertEqual(convert_blocklot_to_pin("2 E1","31"), "2000E00001000000")
        #   19R100 02 (from a 1995 lien) => 0019R00100000000???

# 	56J104 02 => nothing.
#   Only 0056-J-00104-0000-00 is currently in the county Property
#   Assessment database. It's possible that 0056-J-00104-0002-00
#   no longer exists as a property.

        # Seemingly human errors:
#        self.assertEqual(convert_blocklot_to_pin("1345L1362 1701"),"1345L00362170100") # 0362 got typo-ed as 1362.

# 366H3174 004 actually maps to 0366H00317400400.
# That is, two extra zeros need to be inserted after the H, even though
# there are four trailing digits. The algorithm would expect
# 0366H00317400400 to be reduced to the Block Lot number 366H317 4004

#    def test_disambiguating_blocklots(self):
# Send the owner name as an extra parameter to assist in disambiguating block lots.
#        self.assertEqual(convert_blocklot_to_pin("174M67 1","Anderson,Thelma,Y"), "0174M00067000100")



#    def test_unconvertible_blocklots(self):
        # 8000T[0-9]* represents a trailer. These trailer codes do not map
        # to PINs.
        ### Actually, some trailers ARE represented by parcels with PINs.
#        self.assertEqual(convert_blocklot_to_pin("8000T2033"), "")

if __name__ == '__main__':
    unittest.main()
