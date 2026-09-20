"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 269 / 400
================================================================================
- Group: ThirdTest_pincode_group_27_parts_261_to_270
- Assigned PIN Codes: 48 (Range: 630612 to 631702)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_269.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_269.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_269"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-269] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "630612",
  "630702",
  "630709",
  "630710",
  "630713",
  "631001",
  "631002",
  "631003",
  "631004",
  "631005",
  "631006",
  "631051",
  "631052",
  "631101",
  "631102",
  "631151",
  "631152",
  "631201",
  "631202",
  "631203",
  "631204",
  "631205",
  "631206",
  "631207",
  "631208",
  "631209",
  "631210",
  "631211",
  "631212",
  "631213",
  "631301",
  "631302",
  "631303",
  "631304",
  "631402",
  "631501",
  "631502",
  "631551",
  "631552",
  "631553",
  "631561",
  "631601",
  "631603",
  "631604",
  "631605",
  "631606",
  "631701",
  "631702"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "630612": {
    "pincode": "630612",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Sivaganga Division",
    "offices": [
      "Pottapalayam SO",
      "Ambalathadi BO",
      "Keeladi BO",
      "Konthagai North BO"
    ]
  },
  "630702": {
    "pincode": "630702",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Sivaganga Division",
    "offices": [
      "Ariyandipuram BO",
      "Karunchutti BO",
      "Ilayangudi SO",
      "Keelayur BO",
      "Kottaiyur BO",
      "Melayur BO",
      "Nettur BO",
      "North Andakudi BO",
      "Uthamanoor BO",
      "Ilayangudi West SO"
    ]
  },
  "630709": {
    "pincode": "630709",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Sivaganga Division",
    "offices": [
      "Pudur Ilayangudi SO",
      "Indankulam BO",
      "Kannamangalam BO",
      "Melaseithur BO",
      "Nagamuguthangudi BO",
      "Sathani BO",
      "Sothugudi BO",
      "Thayamangalam BO"
    ]
  },
  "630710": {
    "pincode": "630710",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Sivaganga Division",
    "offices": [
      "Salaigramam SO",
      "Kalagathankottai BO",
      "Muthur BO",
      "Samudram BO",
      "Sathanur BO",
      "Thogavur BO",
      "Vandal BO"
    ]
  },
  "630713": {
    "pincode": "630713",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Sivaganga Division",
    "offices": [
      "Suranam SO",
      "Akkavayal BO",
      "Alavidangan BO",
      "Rajani BO",
      "Sakkur BO",
      "Viswanoor BO"
    ]
  },
  "631001": {
    "pincode": "631001",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Arakkonam HO",
      "Ashoknagar SO Vellore"
    ]
  },
  "631002": {
    "pincode": "631002",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Palanipet SO",
      "Ammanur BO",
      "Kilandurai BO",
      "Nagavedu BO",
      "Parithiputhur BO",
      "Perumuchi BO",
      "Sirunamalli BO"
    ]
  },
  "631003": {
    "pincode": "631003",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Jothinagar SO",
      "Anaipakkam BO",
      "Chitteri BO",
      "Ichiputhur BO",
      "Kainoor BO",
      "Kilandur BO",
      "Kilpakkam BO",
      "Kumpinipet BO",
      "Mudur BO",
      "Perungalathur BO",
      "Thanigaipolur BO",
      "Vadamambakkam BO",
      "Valaikulam BO"
    ]
  },
  "631004": {
    "pincode": "631004",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Ekhunagar SO",
      "Kavanur BO",
      "Kilkuppam B.O",
      "Mosur BO",
      "Puliyamangalam BO",
      "Seyyoor BO"
    ]
  },
  "631005": {
    "pincode": "631005",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Winterpet SO"
    ]
  },
  "631006": {
    "pincode": "631006",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Ins Rajali SO"
    ]
  },
  "631051": {
    "pincode": "631051",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Namali SO",
      "Attupakkam BO",
      "Ganapathipuram BO",
      "Kilvenkatapuram BO",
      "Pallur BO",
      "Sayanavaram BO",
      "Sendamangalam BO",
      "Tirumalpur BO",
      "Kilvenbakkam BO"
    ]
  },
  "631052": {
    "pincode": "631052",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Panapakkam SO",
      "Jagirthandalam BO",
      "Karnavur BO",
      "Melapulam BO",
      "Peruvalayam BO",
      "Pillaipakkam BO",
      "Reddivalam BO",
      "Ulianallore BO"
    ]
  },
  "631101": {
    "pincode": "631101",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Guruvarajpet SO",
      "Kizhavanam BO",
      "Mittapettai BO",
      "Nandiveduthangal BO"
    ]
  },
  "631102": {
    "pincode": "631102",
    "circle": "Tamilnadu circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Ayipedu BO",
      "Jambukulam BO",
      "Karikkal BO",
      "Kesavanankuppam BO",
      "Kodakkal BO",
      "Kondapalayam BO",
      "Pandianallore BO",
      "Perunkanchi BO",
      "Rendadi BO",
      "Somasamudram BO",
      "Talikkal BO",
      "Vangupattu BO",
      "Sholingur SO",
      "Sholingur Bazaar SO"
    ]
  },
  "631151": {
    "pincode": "631151",
    "circle": "Tamilnadu circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Arigilapadi BO",
      "Iluppaithandalam BO",
      "Kadambainallore BO",
      "Takkolam SO",
      "Mangattucherry BO",
      "Sagayathottam BO",
      "Uriyur BO"
    ]
  },
  "631152": {
    "pincode": "631152",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Arakkonam Division",
    "offices": [
      "Suraksha Cisf Campus SO"
    ]
  },
  "631201": {
    "pincode": "631201",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Arungolam S.O",
      "Chivada B.O",
      "Nemili B.O"
    ]
  },
  "631202": {
    "pincode": "631202",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Athimanjeripeta S.O",
      "Athimanjeri B.O",
      "Balakrishnapuram B.O",
      "Karlambakkam B.O",
      "Kodivalasa B.O",
      "Konasamudram B.O",
      "Kothakuppam B.O",
      "Nochili B.O"
    ]
  },
  "631203": {
    "pincode": "631203",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Kadambathur S.O",
      "Egattur B.O",
      "Kaivandur B.O",
      "Pandur B.O",
      "Pattaraiperumbudur B.O",
      "Pudumavilangai B.O",
      "Selai B.O",
      "Senji B.O",
      "Tirupasur B.O",
      "Vidaiyur B.O"
    ]
  },
  "631204": {
    "pincode": "631204",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Kanakammachatram S.O",
      "Arcot B.O",
      "Illuppur B.O",
      "Kanchipadi B.O",
      "Muthukondapuram B.O",
      "Nabalur B.O",
      "Nedambaram B.O",
      "Ramanjeri B.O",
      "Ramapuram B.O"
    ]
  },
  "631205": {
    "pincode": "631205",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Kg Kandigai S.O",
      "Beerakuppam B.O",
      "Cherukkanur B.O",
      "Koramangalam B.O",
      "Mambakkam B.O",
      "S.Agraharam B.O",
      "Sirugumi B.O",
      "T.C.Kandigai B.O",
      "Thadur B.O",
      "V.K.N.Kandigai B.O",
      "Veerakaverirajapuram B.O"
    ]
  },
  "631206": {
    "pincode": "631206",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Madduru S.O",
      "Buchireddipalli B.O",
      "Krishnasamudram B.O",
      "Ramasamudram B.O"
    ]
  },
  "631207": {
    "pincode": "631207",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Pallipat S.O",
      "Echambadi B.O",
      "Karimbedu B.O",
      "Kolathur B.O",
      "Konetampet B.O",
      "Kumararajupeta B.O",
      "Nediyam B.O",
      "Punyam B.O",
      "Samanthavada B.O",
      "Sanakuppam B.O",
      "Veliagaram B.O",
      "Venkatarajakuppam B.O"
    ]
  },
  "631208": {
    "pincode": "631208",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Podaturpeta S.O",
      "Bommarajupeta B.O",
      "C.N.Kandigai B.O",
      "Jangalapalli B.O",
      "Kakalur B.O",
      "Keechalam B.O",
      "Nedigallu B.O",
      "Pandravedu B.O",
      "Perumanallur B.O",
      "Pettaikandigai B.O",
      "Savattur B.O",
      "Sorakayapeta B.O",
      "T.T.Kandigai B.O"
    ]
  },
  "631209": {
    "pincode": "631209",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Tiruttani S.O",
      "Agooru B.O",
      "Murukkambattu B.O",
      "Netterikandigai B.O",
      "Pattabiramapuram B.O",
      "Srinivasapuram B.O",
      "Suryanagaram B.O",
      "Velanjeri B.O",
      "Tiruttanieast S.O",
      "Upper Tirutani B.O",
      "Tirutani Hills B.O"
    ]
  },
  "631210": {
    "pincode": "631210",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Tiruvelangadu S.O",
      "Athipattu B.O",
      "Chinnammapet B.O",
      "Harichandrapuram B.O",
      "Kaverirajapuram B.O",
      "Kunnavalam B.O",
      "Kurmavilasapuram B.O",
      "Mannur B.O",
      "Pakasala B.O",
      "Palayanur B.O",
      "Peddakalakattur B.O",
      "Veeraraghavapuram B.O",
      "Vyasapuram B.O"
    ]
  },
  "631211": {
    "pincode": "631211",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Saraswathinagar S.O",
      "Karthikeyapuram B.O",
      "Kasavarajpetta B.O"
    ]
  },
  "631212": {
    "pincode": "631212",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Poonimangadu S.O",
      "Nallathur B.O",
      "Thalavedu B.O"
    ]
  },
  "631213": {
    "pincode": "631213",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Ponpadi Rs S.O",
      "Alamelumangapuram B.O",
      "Ponpadi B.O"
    ]
  },
  "631301": {
    "pincode": "631301",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Ammavarikuppam S.O",
      "Balapuram B.O",
      "Chandravilasapuram B.O",
      "Kadananagaram B.O",
      "Mahankalikapuram B.O",
      "Thyagapuram B.O"
    ]
  },
  "631302": {
    "pincode": "631302",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Erumbi S.O",
      "Chanurmallavaram B.O",
      "Myladumparai B.O",
      "Paivalasa B.O",
      "Peddanagapudi B.O",
      "Peddaramapuram B.O",
      "Rangapuram B.O",
      "Srikalikapuram B.O",
      "Thamaraikulam B.O",
      "V.P.R.Puram B.O",
      "Vediyangadu B.O",
      "Veeramangalam B.O"
    ]
  },
  "631303": {
    "pincode": "631303",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Ramakrishnarajupeta S.O",
      "Adivaragapuram B.O",
      "Ammaneri B.O",
      "Veeranathur B.O"
    ]
  },
  "631304": {
    "pincode": "631304",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Vanganur S.O",
      "Emrkandigai B.O",
      "Rajanagaram B.O"
    ]
  },
  "631402": {
    "pincode": "631402",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Perambakkam S.O",
      "Kilacheri B.O",
      "Kondancheri B.O",
      "Kuvam B.O",
      "Mappedu B.O",
      "Narasingapuram B.O"
    ]
  },
  "631501": {
    "pincode": "631501",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Kanchipuram H.O",
      "Kanchipuram Collectorate S.O",
      "Kanchipuram Cutchery S.O",
      "Little Kanchipuram S.O",
      "Pillayarpalayam S.O",
      "Thoopul S.O"
    ]
  },
  "631502": {
    "pincode": "631502",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Big Kanchipuram S.O",
      "Ayyankarkulam B.O",
      "Govindavadi B.O",
      "Kammavarpalayam B.O",
      "Kilakattur B.O",
      "Kuram B.O",
      "Melkadirpur B.O",
      "Orikkai B.O",
      "Perumanallur B.O",
      "Sevilimedu B.O",
      "Sirukaveripakkam B.O",
      "Thimamsamudram B.O",
      "Veliyur B.O",
      "Olimohamedpet B.O",
      "Sankaramutt S.O"
    ]
  },
  "631551": {
    "pincode": "631551",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Balchettychatram S.O",
      "Damal B.O",
      "Krishnapuram B.O",
      "Musaravakkam B.O",
      "Muthavedu B.O"
    ]
  },
  "631552": {
    "pincode": "631552",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Karaipettai S.O",
      "Karai B.O",
      "Parandur B.O",
      "Siruvakkam B.O",
      "Siruvallur B.O"
    ]
  },
  "631553": {
    "pincode": "631553",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Edayarpakkam S.O",
      "Chellampattidai B.O",
      "Ekanapuram B.O",
      "Kappankottur B.O",
      "Melmaduramangalam B.O",
      "Pichivakkam B.O",
      "Pullalur B.O",
      "Purisai B.O",
      "Valathur B.O"
    ]
  },
  "631561": {
    "pincode": "631561",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Enathur S.O",
      "Attuputhur B.O",
      "Nirvalur B.O",
      "Rajakulam B.O",
      "Singadivakkam B.O",
      "Vaiyavur B.O",
      "Vedal B.O"
    ]
  },
  "631601": {
    "pincode": "631601",
    "circle": "Tamilnadu circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Ekanampet B.O",
      "Ayyampettai S.O",
      "Naickenpet B.O",
      "Thenambakkam B.O",
      "Thimmarajampettai B.O"
    ]
  },
  "631603": {
    "pincode": "631603",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Magaral S.O",
      "Arpakkam B.O",
      "Kavanthandalam B.O",
      "Malayankulam B.O",
      "Neyyadupakkam B.O",
      "Olugarai B.O",
      "Puthali B.O",
      "Vayalakkavur B.O"
    ]
  },
  "631604": {
    "pincode": "631604",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Thenneri S.O",
      "Echoor B.O",
      "Kunnam B.O",
      "Panruti B.O",
      "Sinnivakkam B.O",
      "Varanavasi B.O"
    ]
  },
  "631605": {
    "pincode": "631605",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Walajabad S.O",
      "Asoor B.O",
      "Avalur B.O",
      "Nathanallur B.O",
      "Puliambakkam B.O",
      "Thammanur B.O",
      "Uttukadu B.O"
    ]
  },
  "631606": {
    "pincode": "631606",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Kanchipuram Division",
    "offices": [
      "Palayaseevaram S.O",
      "Madhur B.O",
      "Padur B.O",
      "Thirumukkudal B.O",
      "Ullavur B.O"
    ]
  },
  "631701": {
    "pincode": "631701",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Tiruvannamalai Division",
    "offices": [
      "Akkur SO Tiruvannamalai",
      "Madipakkam BO",
      "Mathur BO",
      "Sozhavaram BO",
      "Ukkal BO"
    ]
  },
  "631702": {
    "pincode": "631702",
    "circle": "Tamilnadu Circle",
    "region": "Chennai City Region",
    "division": "Tiruvannamalai Division",
    "offices": [
      "Mamandur Kanchipuram SO",
      "Arasanipalai BO",
      "Chinnaelacheri BO",
      "Dusi BO",
      "Menalur BO",
      "Pudupalayam BO",
      "Vadakalpakkam BO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
