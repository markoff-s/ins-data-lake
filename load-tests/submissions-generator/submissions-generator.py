import random
import csv
import math
# import datetime

# import numpy as np, numpy.random
import pandas as pd

# from random import randint
from faker import Faker
# from numpy import array

from datetime import datetime, date, timedelta

# from dateutil.relativedelta import relativedelta
# from dateutil.relativedelta import *

pd.options.mode.chained_assignment = None  # default='warn'

fake = Faker()

number_of_applications = 10000
first_submission = 1000


def coalesce(*arg): return next((a for a in arg if a is not None), None)


def nocommas(dirtystring):
    cleanstring = dirtystring.replace(',', '')
    return cleanstring


# Change locations to use real addresses. This is a large file, so we won't use dataframes.
# source_locations = pd.read_csv("locations.csv")
source_locations = open('C:\\temp\\big-data-sets\\NAD_r3_revised_ASCII\\NAD_r3.txt')
source_locations_size = source_locations.seek(0, 2)
source_locations_header_size = 403  # The header line is 403 bytes. Skip this.

source_locations_columns = {
    "OID": 0,
    "State": 1,
    "County": 2,
    "Inc_Muni": 3,
    "Uninc_Comm": 4,
    "Nbrhd_Comm": 5,
    "Post_Comm": 6,
    "Zip_Code": 7,
    "Plus_4": 8,
    "Bulk_Zip": 9,
    "Bulk_Plus4": 10,
    "StN_PreMod": 11,
    "StN_PreDir": 12,
    "StN_PreTyp": 13,
    "StN_PreSep": 14,
    "StreetName": 15,
    "StN_PosTyp": 16,
    "StN_PosDir": 17,
    "StN_PosMod": 18,
    "AddNum_Pre": 19,
    "Add_Number": 20,
    "AddNum_Suf": 21,
    "LandmkPart": 22,
    "LandmkName": 23,
    "Building": 24,
    "Floor": 25,
    "Unit": 26,
    "Room": 27,
    "Addtl_Loc": 28,
    "Milepost": 29,
    "Longitude": 30,
    "Latitude": 31,
    "NatGrid_Coord": 32,
    "GUID": 33,
    "Addr_Type": 34,
    "Placement": 35,
    "Source": 36,
    "AddAuth": 37,
    "UniqWithin": 38,
    "LastUpdate": 39,
    "Effective": 40,
    "Expired": 41,
    "OrigOID": 42
}

output_columns = [
    "submission_id",
    "submission_status",
    "submitted_on",
    "underwriter",
    "organization",
    "street",
    "city",
    "state",
    "zip",
    "website",
    "years_at_location",
    "principal_name",
    "years_in_business",
    "nature_of_operation",
    "previous_carrier",
    "expiry_date",
    "expiring_premium",
    "building_owned",
    "area_occupied",
    "stories",
    "year_built",
    "age",
    "class_of_construction",
    "wall",
    "roof",
    "floor",
    "roof_updated",
    "wiring_updated",
    "plumbing_updated",
    "heating_updated",
    "occupied_stories",
    "adjacent_north",
    "adjacent_south",
    "adjacent_east",
    "adjacent_west",
    "fire_protection",
    "fire_alarm",
    "sprinklered",
    "burglar_alarm",
    "deadbolt_locks",
    "other_protection",
    "safe",
    "safe_type",
    "average_cash",
    "maximum_cash",
    "mortgagee",
    "building_value",
    "equipment_value",
    "improvements_value",
    "office_contents",
    "edp_equipment",
    "edp_data_media",
    "laptops_projectors",
    "customer_goods",
    "others_property",
    "stock_value",
    "gross_earnings",
    "profits",
    "transit",
    "employee_dishonesty",
    "money_orders_securities",
    "flood",
    "earthquake",
    "boiler",
    "number_of_boilers",
    "boiler_maintenance_contract",
    "air_conditioning",
    "central_air_conditioning",
    "tons",
    "air_conditioning_maintenance_contract",
    "automatic_backup_power",
    "voltage_surge_suppression",
    "claim_date",
    "claim_description",
    "claim_amount",
    "signed_name",
    "title_position"
]

# output_columns = [
#     "Submission ID",
#     "Submission Status",
#     "Submitted On",
#     "Underwriter",
#     "Company",
#     "Street",
#     "City",
#     "State",
#     "Zip",
#     "Website",
#     "Years at Location",
#     "Principal Name",
#     "Years in Business",
#     "Nature of Operation",
#     "Previous Carrier",
#     "Expiry Date",
#     "Expiring Premium",
#     "Building Owned",
#     "Area Occupied",
#     "Stories",
#     "Year Built",
#     "Age",
#     "Class of Construction",
#     "Wall",
#     "Roof",
#     "Floor",
#     "Roof Updated",
#     "Wiring Updated",
#     "Plumbing Updated",
#     "Heating Updated",
#     "Occupied Stories",
#     "Adjacent North",
#     "Adjacent South",
#     "Adjacent East",
#     "Adjacent West",
#     "Fire Protection",
#     "Fire Alarm",
#     "Sprinklered",
#     "Burglar Alarm",
#     "Deadbolt Locks",
#     "Other Protection",
#     "Safe",
#     "Safe Type",
#     "Average Cash",
#     "Maximum Cash",
#     "Mortgagee",
#     "Building Value",
#     "Equipment Value",
#     "Improvements Value",
#     "Office Contents",
#     "EDP Equipment",
#     "EDP Data Media",
#     "Laptops Projectors",
#     "Customer Goods",
#     "Others Property",
#     "Stock Value",
#     "Gross Earnings",
#     "Profits",
#     "Transit",
#     "Employee Dishonesty",
#     "Money Orders Securities",
#     "Flood",
#     "Earthquake",
#     "Boiler",
#     "Number of Boilers",
#     "Boiler Maintenance Contract",
#     "Air Conditioning",
#     "Central Air Conditioning",
#     "Tons",
#     "Air Conditioning Maintenance Contract",
#     "Automatic Backup Power",
#     "Voltage Surge Suppression",
#     "Claim Date",
#     "Claim Description",
#     "Claim Amount",
#     "Signed Name",
#     "Title Position"
# ]

nature_of_operation = [
    "Accounting & Auditing Services",
    "Advertising Agency",
    "Aviation Services and Products",
    "Automotive Repair and Maintenance",
    "Auto Transmission Repair Service and Installation",
    "Agriculture and Farming",
    "Architects & Architecture Services",
    "Auto Parts and Supplies Retail Store",
    "Automotive Body Repair and Collision Shops",
    "Appraisers",
    "Agricultural Consultant",
    "Graphic Artists & Designers",
    "Performing Arts",
    "Sports and Recreation Services",
    "Travel Agency",
    "Weaponry Services and Products",
    "Actuarial Services",
    "Acupuncturist",
    "Air Conditioning & Heating Equipment Store",
    "Air Conditioning Cleaning & Repair",
    "Air Conditioning Equip. & Sys. – Install Service or Repair",
    "Antique Store",
    "Appliance Store - Residential",
    "Art Studio",
    "Audio Consultant",
    "Car Audio & Video Sales Installation & Repair",
    "Claim Adjuster",
    "Coach Advisor or Mentor",
    "Insurance Agency",
    "Kitchen Accessories Store",
    "Lingerie & Accessories Store",
    "Real Estate Agency",
    "Real Estate Appraiser",
    "Style and Talent Management",
    "Substance Abuse Counseling",
    "Tire Center - Automotive",
    "Title Agency",
    "Adult Day Care",
    "Air Conditioning Equipment Manufacturing",
    "Animal Hospital",
    "Appliance Repair Shop",
    "Art Conservator",
    "Art Poster & Picture Store - Original & Reproduced Work",
    "Audio Visual Equipment Store - Sales & Service",
    "Auto Dealer (No Repairs)",
    "Auto Leasing",
    "Auto Parts & Supplies Distributor",
    "Auto Parts & Supplies Store",
    "Automotive Glass Replacement",
    "Awning Installation",
    "Detective Agency",
    "Drug Testing and Background Checks",
    "Employment Agency",
    "Event and Entertainment Services",
    "Fine Art School",
    "Heating & Air Conditioning Distributor",
    "Household Appliances Manufacturing",
    "Nannies and In-Home Care Givers",
    "Ambulance Service",
    "Ambulatory Surgery Centers",
    "Analytical Chemist",
    "Analytical Testing Services",
    "Answering Service",
    "Apartment Owner",
    "Application Service Providers",
    "Army & Navy Store",
    "Art & Drafting Supplies Store",
    "Art Dealer or Gallery",
    "Artists Supplies & Picture Frames Distributor",
    "Assembly Work & Packaging of Electronics for Direct Consumer Distribution",
    "Assembly Work & Subassembly of Electronic Parts",
    "Association",
    "Audio Video & Photo Equipment Manufacturing",
    "Author",
    "Bed and Breakfast",
    "Canned and Plastic Drinks Manufacturing",
    "Carpentry Work - Finish and Trim Only",
    "Commercial Condo Association",
    "Computer Refurbish and Resale",
    "Electrical Parts or Accessories Manufacturing",
    "Energy Services and Products",
    "Hearing Aids Distributor",
    "Home Appliance Installation",
    "Household Appliances Distributor",
    "Interior Auto Detailing",
    "Iron Works - Decorative or Artistic",
    "Kitchen Accessories Distributor",
    "Mailing or Addressing Services",
    "Motorcycle and RV Dealer",
    "Patent Agent",
    "Printed Circuit Boards Assembly",
    "Religious Articles & Church Supplies Distributor",
    "Research & Development - Tech Medical and Electronics",
    "Soccer Apparel & Equipment Store",
    "Telephone & Telegraph Apparatus Manufacturing",
    "Ticket Agency",
    "Tours Retreats and Demonstrations",
    "Utilities Install and Maintenance",
    "Waste Removal and Restoration Services",
    "Financial Planner",
    "HVAC Equip. & Sys. Installation",
    "Prepackaged Software",
    "Residential Care",
    "Software & Internet Design Services",
    "Business & Management Consultant",
    "Computer Consultant",
    "Consultant",
    "Jewelry Maker",
    "Lawyers & Law Firms",
    "Other Professional Offices",
    "Beer Liquor & Wine Store",
    "Department Store",
    "Electrical Work",
    "Embroidery",
    "Engineers & Engineering Services",
    "Gourmet & Specialty Food Shop",
    "Hospitality",
    "Investment Company",
    "Life Coach",
    "Medical Safety Training",
    "Micro Breweries & Malt Beverage Manufacturing",
    "Office Machines Manufacturing",
    "Pet Shop & Supply Store",
    "Physical Fitness Facility",
    "Practical Nursing School",
    "Recording Studio",
    "Retail Clothing Store",
    "Safety & Loss Control Consultant",
    "Security Services",
    "Tax Preparer & Bookkeeper",
    "Transportation Services",
    "Wedding Consultant",
    "Brass Copper & Pewter Specialty Distributor",
    "Brass Copper & Pewter Specialty Store",
    "Bridal Shop",
    "Business & Secretarial Schools",
    "Cable or Subscription TV Services",
    "Cameras & Photo Equipment Store",
    "Career Coach",
    "Carpets Rugs & Floor Covering Store",
    "Chiropractor",
    "Clothing Distributor",
    "Clothing Manufacturing(All Other)",
    "Clothing Manufacturing(Children’s Wear)",
    "Clothing Store - Men & Boys",
    "Clothing Store - Sports",
    "Clothing Store - Uniform or Maternity",
    "Clothing Store - Women & Girls",
    "Day Care Center",
    "Draftsmen",
    "Driveway Parking or Sidewalk Paving or Repaving",
    "Drug Store with Food",
    "Educational Services",
    "Engraving",
    "Equipment Rental",
    "Exterior Carwash",
    "Extermination",
    "Forestry Consultant",
    "Gasoline Service Station",
    "Gift Baskets - Retail",
    "Gifts Distributor",
    "Hobby & Model Shop",
    "Hosiery Store",
    "Insurance Companies",
    "Kennels",
    "Landscaping",
    "Linen & White Goods Store",
    "Linens Distributor - Bed & Bath",
    "Magnetic & Optical Recording Media Manufacturing",
    "Market Research Firm",
    "Media Consultant",
    "Medical Office",
    "Medical Transcription",
    "Museum",
    "Online Retailer",
    "Other Restaurant",
    "Personal Trainer",
    "Pet Grooming Services",
    "Pet Supplies Distributor",
    "Pet Training Services",
    "Physicians & Surgeons",
    "Picture Frames & Framing Retail Store",
    "Plastering & Drywall",
    "Podiatrist",
    "Political Club",
    "Public Speaker",
    "Publisher without Printing Operations",
    "Quick Lube Center",
    "Radio & TV Communication Equipment Manufacturing",
    "Recreational Club (Non Profit)",
    "Religious Organization",
    "Roofing Siding & Gutters",
    "Rubber Stamps Store",
    "Sale of Used Goods",
    "Shoe Distributor",
    "Shoe Store",
    "Sporting Goods Distributor",
    "Sporting Goods Store",
    "Structured Settlement Consultant",
    "TShirt Store",
    "Tailor",
    "Taxidermist",
    "Tie Shop",
    "Towing",
    "Trophy Distributor",
    "Tutoring Services",
    "Urgent Care",
    "Veterinarian",
    "Vocational Schools",
    "Website Design & Development Services",
    "Zoo"
]

carriers = [
    "The Hartford",
    "C and S",
    "BI Berk",
    "Acuity",
    "Nationwide",
    "Insureon",
    "Farmers",
    "IRMI",
    "EM Broker",
    "Travelers",
    "AXA XL",
    "Chubb"
]

choices = [
    "Yes",
    "No"]

construction_classes = [
    "A",
    "B",
    "C",
    "D",
    "S"
]

walls = {
    "A": ["concrete", "masonry", "glass"],
    "B": ["concrete", ],
    "C": ["concrete", "masonry"],
    "D": ["wood frame", ],
    "S": ["steel", ]
}

floors = {
    "A": ["reinforced concrete", "masonry"],
    "B": ["concrete", "masonry"],
    "C": ["concrete on grade", "wood", "steel deck"],
    "D": ["wood", "concrete slab"],
    "S": ["concrete slab", "steel deck"]
}

roofs = {
    "A": ["reinforced concrete", "masonry"],
    "B": ["concrete", "masonry"],
    "C": ["wood", "steel"],
    "D": ["wood", ],
    "S": ["steel", ]
}

adjacent_exposures = [
    "Elementary School",
    "Gas Station",
    "Manufacturing",
    "Office Building",
    "Other",
    "Residential",
    "Restaurant",
    "Retail",
    "Vacant Lot",
    "Water"
]

firepro = [
    "Hydrant within 300 meters",
    "Fire Station within 8km",
    "Unprotected (no hydrants)"
]

alarm = [
    "None",
    "Local",
    "Central Station"
]

# sprinklered = [
# "None",
# "Partial 50%",
# "Yes 100%"
# ]

locks = [
    "Padlocks",
    "Knob Locks",
    "Lever Handle Locks",
    "Cam Locks",
    "Rim/Mortise Locks",
    "Euro Profile Cylinders",
    "Wall Mounted Locks"
]

safes = [
    "Simple Fire Resistant Safes",
    "Burglary Safes (Burglar Fire Safes",
    "Standalone Home Safe",
    "Wall Safes",
    "Floor Safes",
    "Gun Safes",
    "Jewelry Safes",
    "Bank Vaults"
]

claim_types = [
    "Fire due to unattended appliance",
    "Fire due to faulty wiring",
    "Hurricane Damage",
    "Tornado Damage",
    "Loss by Collapse",
    "Earthquake Damage",
    "Vandalism & Theft",
    "Smoke Damage",
    "Mold & Asbestos",
    "Air Conditioning Leaks",
    "Roof Leaks",
    "Dishwasher Leaks",
    "Theft",
    "Plumbing Damage",
    "Fire Damage",
    "Ice or Snow Load",
    "Storm Damage",
    "Hot Water Heater Leaks",
    "Flood & Water Damage",
    "Hail Damage",
    "Maritime Disaster",
    "Liability",
    "Sinkholes",
    "Crime Scene Cleanup",
    "Business Interruption",
    "Crop Damage",
    "Information Systems Failure",
    "Wind Damage",
    "Workers Comp",
    "Aviation Disaster"
]

boiler_types = [
    "Hot water",
    "Steam",
    "None"
]

surge_types = [
    "Yes at main panel",
    "Yes at each individual refrigeration unit",
    "None"
]

equipment_types = [
    "Load Cells",
    "Air Actuators and Ball Valves",
    "Filling Lance Assembly",
    "Roller Bearings",
    "Gravity Rollers",
    "Powered Rollers",
    "Limit Switches",
    "Photo Eyes",
    "Air Bags",
    "Pneumatic & Hydraulic Cylinders",
    "Solenoids",
    "Chains & Sprockets",
    "Gearmotors",
    "Pneumatic Brake",
    "Pneumatic and Hydraulic Brakes",
    "Filter / Regulator / Lubricator",
    "Pillow Block Bearings",
    "Conveyor Belting",
    "Conveyor Pulleys",
    "Flanged Bearings"
]

optional_values = [
    True,
    False
]

blank_10 = [.9, .1]
blank_20 = [.8, .2]
blank_30 = [.7, .3]
blank_40 = [.6, .4]
blank_50 = [.5, .5]
blank_60 = [.4, .6]
blank_70 = [.3, .7]
blank_80 = [.2, .8]
blank_90 = [.1, .9]

print("Generating Insurance Applications ...")

with open("submissions.csv", 'w', newline='') as csvfile:
    App_Data = csv.DictWriter(csvfile, fieldnames=output_columns)
    App_Data.writeheader()

    for t in range(1, number_of_applications + 1):

        # COMMERCIAL PROPERTY INSURANCE APPLICATION
        new_application = {}

        application_date = fake.date_time_between(start_date='-5y', end_date='now', tzinfo=None)
        date_built = fake.date_time_between(start_date='-70y', end_date='-10y', tzinfo=None)
        # location = source_locations.sample(1).to_dict("records")[0]
        construction_class = random.choice(construction_classes)

        location = []
        city = ''
        zipcode = ''

        while not city or not zipcode:
            source_locations.seek(random.randint(source_locations_header_size, source_locations_size))
            source_locations.readline()  # Throw away to avoid partial lines
            location_line = source_locations.readline()
            if len(location_line) == 0:  # end of file, go back to first line
                source_locations.seek(source_locations_header_size)
                location_line = source_locations.readline()
            location = list(csv.reader([location_line]))[0]
            city = coalesce(location[source_locations_columns.get("Post_Comm")],
                            location[source_locations_columns.get("Nbrhd_Comm")],
                            location[source_locations_columns.get("Uninc_Comm")],
                            location[source_locations_columns.get("Inc_Muni")],
                            location[source_locations_columns.get("County")])
            zipcode = location[source_locations_columns.get("Zip_Code")]

        # generate a street address frome the elements of the NAD
        address_elements = [
            location[source_locations_columns.get("AddNum_Pre")],
            location[source_locations_columns.get("Add_Number")],
            location[source_locations_columns.get("AddNum_Suf")],
            location[source_locations_columns.get("StN_PreMod")],
            location[source_locations_columns.get("StN_PreDir")],
            location[source_locations_columns.get("StN_PreTyp")],
            location[source_locations_columns.get("StN_PreSep")],
            location[source_locations_columns.get("StreetName")],
            location[source_locations_columns.get("StN_PosTyp")],
            location[source_locations_columns.get("StN_PosDir")],
            location[source_locations_columns.get("StN_PosMod")]
        ]

        # NEW FIELDS
        new_application["submission_id"] = first_submission + t
        new_application["submission_status"] = 'open'
        new_application["submitted_on"] = application_date
        new_application["underwriter"] = ''

        # 1. APPLICANT INFORMATION
        new_application["organization"] = nocommas(fake.company())
        new_application["street"] = ' '.join(filter(None, address_elements))
        new_application["city"] = city
        new_application["state"] = location[source_locations_columns.get("State")]
        new_application["zip"] = zipcode
        new_application["website"] = fake.url(schemes=None)
        new_application["years_at_location"] = random.randint(1, 20)
        new_application["principal_name"] = fake.name()
        new_application["years_in_business"] = random.randint(1, 50)
        new_application["nature_of_operation"] = random.choice(nature_of_operation)
        new_application["previous_carrier"] = random.choice(carriers)
        new_application["expiry_date"] = fake.date_time_between_dates(datetime_start=application_date,
                                                                     datetime_end=application_date + timedelta(days=90),
                                                                     tzinfo=None).strftime("%m/%d/%y")
        new_application["expiring_premium"] = round(random.randint(1000, 50000), -2)

        # 2. LOCATION INFORMATION (COMPLETE FOR EACH LOCATION COVERED)
        # new_application["Location Same"] = random.choice(choices)
        # new_application["Location Other"] = ""
        # if new_application["Location Same"] == "No":
        #  new_application["Location Other"] = fake.address()
        new_application["building_owned"] = random.choice(choices)
        new_application["area_occupied"] = str(random.randint(2, 20) * 5) + "%"
        new_application["stories"] = random.randint(1, 20)
        new_application["year_built"] = date_built.year
        new_application["age"] = math.floor((application_date - date_built).days / 365.25)
        new_application["class_of_construction"] = construction_class
        new_application["wall"] = random.choice(walls.get(construction_class))
        new_application["roof"] = random.choice(roofs.get(construction_class))
        new_application["floor"] = random.choice(floors.get(construction_class))
        new_application["roof_updated"] = fake.date_time_between(start_date=date_built, end_date=application_date,
                                                                tzinfo=None).year
        new_application["wiring_updated"] = fake.date_time_between(start_date=date_built, end_date=application_date,
                                                                  tzinfo=None).year
        new_application["plumbing_updated"] = fake.date_time_between(start_date=date_built, end_date=application_date,
                                                                    tzinfo=None).year
        new_application["heating_updated"] = fake.date_time_between(start_date=date_built, end_date=application_date,
                                                                   tzinfo=None).year
        new_application["occupied_stories"] = random.randint(1, new_application["stories"])
        # new_application["occupied space"] = new_application["occupied stories"] * random.randint(1000, 30000)
        new_application["adjacent_north"] = random.choice(adjacent_exposures)
        new_application["adjacent_south"] = random.choice(adjacent_exposures)
        new_application["adjacent_east"] = random.choice(adjacent_exposures)
        new_application["adjacent_west"] = random.choice(adjacent_exposures)
        new_application["fire_protection"] = random.choice(firepro)
        new_application["fire_alarm"] = random.choice(alarm)
        new_application["sprinklered"] = str(random.randint(0, 20) * 5) + "%"
        new_application["burglar_alarm"] = random.choice(alarm)
        new_application["deadbolt_locks"] = random.choice(choices)
        new_application["other_protection"] = ""
        if new_application["deadbolt_locks"] == "no":
            new_application["other_protection"] = random.choice(locks)
        new_application["safe"] = random.choice(choices)
        new_application["safe_type"] = ""
        if new_application["safe"] == "yes":
            new_application["safe_type"] = random.choice(safes)
        new_application["average_cash"] = 0
        new_application["maximum_cash"] = 0
        if random.choices(optional_values, blank_40)[0]:
            new_application["average_cash"] = round(random.randint(1000, 20000), -3)
            new_application["maximum_cash"] = round(random.randint(new_application["average_cash"], 100000), -3)
        # new_application["loss payee"] = new_application["principal name"] + " " + new_application["address"]
        new_application["mortgagee"] = ''
        if random.choices(optional_values, blank_20)[0]:
            new_application["mortgagee"] = nocommas(fake.company())

        # 3. coverages, limits & notes
        new_application["building_value"] = round(random.randint(10000, 1000000), -3)
        # new_application["building value location 2"] = random.randint(0, 1000000)
        # new_application["building value location 3"] = random.randint(0, 1000000)
        new_application["equipment_value"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["equipment_value"] = round(random.randint(10000, 1000000), -3)
        # new_application["equipment value location 2"] = random.randint(0, 1000000)
        # new_application["equipment value location 3"] = random.randint(0, 1000000)
        new_application["improvements_value"] = 0
        if random.choices(optional_values, blank_40)[0]:
            new_application["improvements_value"] = round(random.randint(10000, 1000000), -3)
        # new_application["improvements value location 2"] = random.randint(0, 1000000)
        # new_application["improvements value location 3"] = random.randint(0, 1000000)
        new_application["office_contents"] = 0
        if random.choices(optional_values, blank_10)[0]:
            new_application["office_contents"] = round(random.randint(10000, 1000000), -3)
        # new_application["office contents location 2"] = random.randint(0, 1000000)
        # new_application["office contents location 3"] = random.randint(0, 1000000)
        new_application["edp_equipment"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["edp_equipment"] = round(random.randint(10000, 1000000), -3)
        # new_application["edp equipment location 2"] = random.randint(0, 1000000)
        # new_application["edp equipment location 3"] = random.randint(0, 1000000)
        new_application["edp_data_media"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["edp_data_media"] = round(random.randint(10000, 1000000), -3)
        # new_application["edp data media location 2"] = random.randint(0, 1000000)
        # new_application["edp data media location 3"] = random.randint(0, 1000000)
        new_application["laptops_projectors"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["laptops_projectors"] = round(random.randint(10000, 1000000), -3)
        # new_application["laptops projectors location 2"] = random.randint(0, 1000000)
        # new_application["laptops projectors location 3"] = random.randint(0, 1000000)
        new_application["customer_goods"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["customer_goods"] = round(random.randint(10000, 1000000), -3)
        # new_application["customer goods location 2"] = random.randint(0, 1000000)
        # new_application["customer goods location 3"] = random.randint(0, 1000000)
        new_application["others_property"] = 0
        if random.choices(optional_values, blank_50)[0]:
            new_application["others_property"] = round(random.randint(10000, 1000000), -3)
        # new_application["others property location 2"] = random.randint(0, 1000000)
        # new_application["others property location 3"] = random.randint(0, 1000000)
        new_application["stock_value"] = 0
        if random.choices(optional_values, blank_30)[0]:
            new_application["stock_value"] = round(random.randint(10000, 1000000), -3)
        # new_application["stock value location 2"] = random.randint(0, 1000000)
        # new_application["stock value location 3"] = random.randint(0, 1000000)
        new_application["gross_earnings"] = round(random.randint(10000, 1000000), -3)
        # new_application["gross earnings location 2"] = random.randint(0, 1000000)
        # new_application["gross earnings location 3"] = random.randint(0, 1000000)
        new_application["profits"] = round(random.randint(10000, 1000000), -3)
        # new_application["profits location 2"] = random.randint(0, 1000000)
        # new_application["profits location 3"] = random.randint(0, 1000000)
        new_application["transit"] = 0
        if random.choices(optional_values, blank_90)[0]:
            new_application["transit"] = round(random.randint(10000, 1000000), -3)
        # new_application["transit location 2"] = random.randint(0, 1000000)
        # new_application["transit location 3"] = random.randint(0, 1000000)
        new_application["employee_dishonesty"] = 0
        if random.choices(optional_values, blank_90)[0]:
            new_application["employee_dishonesty"] = round(random.randint(10000, 1000000), -3)
        # new_application["employee dishonesty location 2"] = random.randint(0, 1000000)
        # new_application["employee dishonesty location 3"] = random.randint(0, 1000000)
        new_application["money_orders_securities"] = 0
        if random.choices(optional_values, blank_90)[0]:
            new_application["money_orders_securities"] = round(random.randint(10000, 1000000), -3)
        # new_application["money orders securities location 2"] = random.randint(0, 1000000)
        # new_application["money orders securities location 3"] = random.randint(0, 1000000)
        new_application["flood"] = random.choices(choices, blank_90)[0]
        new_application["earthquake"] = random.choices(choices, blank_80)[0]

        # 4. boiler and machinery (equipment breakdown) if required
        new_application["boiler"] = random.choice(boiler_types)
        new_application["number_of_boilers"] = 1
        if random.choices(optional_values, blank_90)[0]:
            new_application["number_of_boilers"] = random.randint(2, 6)
        new_application["boiler_maintenance_contract"] = random.choice(choices)
        if new_application["boiler"] == "none":
            new_application["number_of_boilers"] = 0
            new_application["boiler_maintenance_contract"] = "no"
        new_application["air_conditioning"] = random.choice(choices)
        new_application["central_air_conditioning"] = random.choice(choices)
        new_application["tons"] = random.randint(3, 20)
        new_application["air_conditioning_maintenance_contract"] = random.choice(choices)
        # new_application["number of compressors"] = random.randint(1, 10)
        if new_application["air_conditioning"] == "no":
            new_application["central_air_conditioning"] = "no"
            new_application["tons"] = 0
            new_application["air_conditioning_maintenance_contract"] = "no"
        #  new_application["number of compressors"] = 0
        # new_application["pressure vessels"] = random.choice(choices)
        # new_application["over 24 inches"] = random.choice(choices)
        # new_application["number of pressure vessels"] = random.randint(1, 10)
        # new_application["pressure vessel maintenance contract"] = random.choice(choices)
        # if new_application["pressure vessels"] == "no":
        #  new_application["over 24 inches"] = "no"
        #  new_application["number of pressure vessels"] = 0
        #  new_application["pressure vessel maintenance contract"] = "no"
        # new_application["temperature sensitive monitor"] = random.choice(choices)
        new_application["automatic_backup_power"] = random.choice(choices)
        new_application["voltage_surge_suppression"] = random.choice(surge_types)
        # new_application["specialty equipment"] = random.choice(equipment_types)
        # new_application["speciailty equipment replacement time"] = str(random.randint(95, 400)) + " days"

        # 5. claim information / all property & boiler and machinery
        new_application["claim_date"] = fake.date_time_between(start_date="-5y", end_date=application_date,
                                                              tzinfo=None).strftime("%m/%d/%y")
        new_application["claim_description"] = random.choice(claim_types)
        claim_amount = round(random.randint(1000, 200000), -3)
        new_application["claim_amount"] = claim_amount

        if random.choices(optional_values, blank_90)[0]:
            new_application["claim_description"] = new_application[
                                                      "claim_description"] + " loss estimated at " + f"${claim_amount:d}"

        # 6. notice concerning personal information

        # 7. warranty statement
        new_application["signed_name"] = new_application["principal_name"]
        new_application["title_position"] = nocommas(fake.job())

        App_Data.writerow(new_application)

        if t % 1000 == 0:
            print("Written " + str(t) + " applications.")
