"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 266 / 400
================================================================================
- Group: ThirdTest_pincode_group_27_parts_261_to_270
- Assigned PIN Codes: 48 (Range: 628716 to 629171)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_266.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_266.csv & .json
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

PART_ID = "part_266"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-266] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "628716",
  "628718",
  "628720",
  "628721",
  "628722",
  "628751",
  "628752",
  "628753",
  "628801",
  "628802",
  "628809",
  "628851",
  "628901",
  "628902",
  "628903",
  "628904",
  "628905",
  "628906",
  "628907",
  "628908",
  "628952",
  "629001",
  "629002",
  "629003",
  "629004",
  "629101",
  "629102",
  "629151",
  "629152",
  "629153",
  "629154",
  "629155",
  "629156",
  "629157",
  "629158",
  "629159",
  "629160",
  "629161",
  "629162",
  "629163",
  "629164",
  "629165",
  "629166",
  "629167",
  "629168",
  "629169",
  "629170",
  "629171"
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
  "628716": {
    "pincode": "628716",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Nalatinputhur S.O",
      "Idaiseval B.O",
      "K.Sivagnanapuram B.O",
      "Villiseri B.O"
    ]
  },
  "628718": {
    "pincode": "628718",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Pasuvandanai S.O",
      "Achankulam B.O",
      "Meenakshipuram B.O",
      "Nagampatti B.O",
      "Sillankulam B.O"
    ]
  },
  "628720": {
    "pincode": "628720",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Turaiyur S.O",
      "Irachi B.O",
      "Kamanaickanpatti B.O",
      "Koppampatti B.O",
      "Mudukkalankulam B.O",
      "Semmapudur B.O"
    ]
  },
  "628721": {
    "pincode": "628721",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Vanaramutti S.O",
      "Alagappapuram B.O",
      "Kalangaraipatti B.O",
      "Kattalankulam B.O",
      "Nachiyarpuram B.O",
      "Pillaiyarnatham B.O",
      "Zamindevarkulam B.O"
    ]
  },
  "628722": {
    "pincode": "628722",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Kurukkuchalai S.O",
      "Melaseithalai B.O",
      "Vedanatham B.O"
    ]
  },
  "628751": {
    "pincode": "628751",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Pannaivilai S.O",
      "Nattathi B.O",
      "Pandaravilai B.O"
    ]
  },
  "628752": {
    "pincode": "628752",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Perungulam S.O (Tuticorin)"
    ]
  },
  "628753": {
    "pincode": "628753",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Sivagalai S.O"
    ]
  },
  "628801": {
    "pincode": "628801",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Eral S.O",
      "Korkai B.O",
      "Vallavallan B.O"
    ]
  },
  "628802": {
    "pincode": "628802",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Siruthondanallur S.O",
      "Arumugamangalam B.O",
      "Idaiyarkadu B.O",
      "Maramangalam B.O",
      "Kottarakurichi B.O"
    ]
  },
  "628809": {
    "pincode": "628809",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Seydunganallur S.O",
      "Anavaradanallur B.O",
      "Vittilapuram B.O"
    ]
  },
  "628851": {
    "pincode": "628851",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Tuticorin Division",
    "offices": [
      "Ellanaickenpatti S.O",
      "Aniaparanallur B.O",
      "Pottalurani B.O",
      "Pudupatti B.O",
      "Singathakurichi B.O",
      "Vadakkukariseri B.O"
    ]
  },
  "628901": {
    "pincode": "628901",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Ariyanayagipuram S.O",
      "Melmandai B.O",
      "Surangudi B.O",
      "Tattaneri B.O"
    ]
  },
  "628902": {
    "pincode": "628902",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Ettaiyapuram S.O",
      "Chinnamalaikundru B.O",
      "Ilambuvanam B.O",
      "Kadalai B.O",
      "Kalugachalapuram B.O",
      "Muthulapuram B.O",
      "Nambipuram B.O",
      "Padanthapuli B.O",
      "Perilovanpatti B.O",
      "Pillaiyarnatham B.O",
      "Sindalakarai B.O",
      "Thalaikattupuram B.O"
    ]
  },
  "628903": {
    "pincode": "628903",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Kulathur (TVL) S.O",
      "Kilavaippar B.O",
      "Sippikulam B.O",
      "Vaippar B.O",
      "Veppalodai B.O"
    ]
  },
  "628904": {
    "pincode": "628904",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Nagalapuram S.O (Tuticorin)",
      "Attankarai B.O",
      "Duraisamypuram B.O",
      "Guruvarpatti B.O",
      "Kilakarandai B.O",
      "Sankaralingapuram B.O",
      "Vedapatti B.O",
      "Vowalthothi B.O"
    ]
  },
  "628905": {
    "pincode": "628905",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Pudur S.O",
      "Boothalapuram B.O",
      "Kandasamypuram B.O",
      "Kilaarunachalapuram B.O",
      "L.Venkateswarapuram B.O",
      "Maniyakaranpatti B.O",
      "Mettilpatti B.O",
      "Sennamareddipatti B.O",
      "Sivalarpatti B.O",
      "Vembur B.O"
    ]
  },
  "628906": {
    "pincode": "628906",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Vembar S.O",
      "Periyasamipuram B.O"
    ]
  },
  "628907": {
    "pincode": "628907",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Vilathikulam S.O",
      "Bommiahpuram B.O",
      "Kadalkudi B.O",
      "Karisalkulam B.O",
      "Kilavilathikulam B.O",
      "Meenakshipuram B.O",
      "Ramachandrapuram B.O",
      "V.Vedapatti B.O",
      "Vadamalaisamudram B.O"
    ]
  },
  "628908": {
    "pincode": "628908",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Kilairal S.O",
      "Pungavarnatham B.O",
      "Sholapuram B.O"
    ]
  },
  "628952": {
    "pincode": "628952",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kovilpatti Division",
    "offices": [
      "Kayathar S.O",
      "Athikulam B.O",
      "Ayyanaruthu B.O",
      "Moortheeswaram B.O",
      "Pannikulam B.O",
      "Therkkuilandaikulam B.O",
      "Vellalankottai B.O"
    ]
  },
  "629001": {
    "pincode": "629001",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Nagercoil H.O",
      "Putheri B.O",
      "Nesamony Nagar B.O",
      "Krishnancoil B.O",
      "Catherin Booth Hospital S.O",
      "Nagercoil Bazaar S.O",
      "Nagercoil Collectorate S.O",
      "Nagercoil Town S.O",
      "Nagercoil West S.O",
      "Oluganacheri S.O",
      "Vadasery S.O",
      "Vadiveeswaram S.O"
    ]
  },
  "629002": {
    "pincode": "629002",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Edalakudy B.O",
      "Kottar S.O",
      "Irulappapuram B.O",
      "Maravankudiyiruppu S.O"
    ]
  },
  "629003": {
    "pincode": "629003",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Chunkankadai B.O",
      "Kaniyakulam B.O",
      "Peruvilai B.O",
      "Vetturnimadam S.O",
      "Parvathipuram B.O"
    ]
  },
  "629004": {
    "pincode": "629004",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Erumbukadu B.O",
      "Nagercoil Industrial Estate S.O"
    ]
  },
  "629101": {
    "pincode": "629101",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Alancholai B.O",
      "Arukani B.O",
      "Maruthemparai B.O",
      "Netta B.O",
      "Pathukani B.O",
      "Kaliyal S.O"
    ]
  },
  "629102": {
    "pincode": "629102",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kodayarkilthangal S.O"
    ]
  },
  "629151": {
    "pincode": "629151",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Chitharal B.O",
      "Karod B.O",
      "Manjalumoodu B.O",
      "Arumanai S.O"
    ]
  },
  "629152": {
    "pincode": "629152",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Cheruvallur B.O",
      "Ilanchirai B.O",
      "Malamary B.O",
      "Mancode B.O",
      "Meppalai B.O",
      "Ramavarmanchirai B.O",
      "Vellachiparai B.O",
      "Idaikkode S.O"
    ]
  },
  "629153": {
    "pincode": "629153",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Chathancode B.O",
      "Kulapuram B.O",
      "Malayadi B.O",
      "Meenachel B.O",
      "Sooriacode B.O",
      "Adaikkakuzhi B.O",
      "Kaliakkavilai S.O"
    ]
  },
  "629154": {
    "pincode": "629154",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Nithiravilai B.O",
      "Kanjampuram S.O"
    ]
  },
  "629155": {
    "pincode": "629155",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kanjiracode S.O"
    ]
  },
  "629156": {
    "pincode": "629156",
    "circle": "Tamilnadu Circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kappiyarai S.O"
    ]
  },
  "629157": {
    "pincode": "629157",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Mangarai B.O",
      "Mullanginavilai B.O",
      "Pottetty B.O",
      "Theruvukadai B.O",
      "Tholayavattai B.O",
      "Karinkal S.O"
    ]
  },
  "629158": {
    "pincode": "629158",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kannanore B.O",
      "Maruthurkurichi B.O",
      "Kattathurai S.O",
      "Eravipudurkadai B.O"
    ]
  },
  "629159": {
    "pincode": "629159",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Alanchi B.O",
      "Ritapuram B.O",
      "Sahayanagar B.O",
      "Palapalam S.O"
    ]
  },
  "629160": {
    "pincode": "629160",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Fathima Nagar B.O",
      "Kirathur B.O",
      "Palavilai B.O",
      "Vallavilai B.O",
      "Kollangodu S.O",
      "Marthandamthurai B.O",
      "Urambu B.O"
    ]
  },
  "629161": {
    "pincode": "629161",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Cherupallur B.O",
      "Chittardam Colony B.O",
      "Manalodai B.O",
      "Maniankuzhi B.O",
      "Pechchipparai B.O",
      "Ponmanai B.O",
      "Thiruvarambu B.O",
      "Tiruparappu B.O",
      "Vendalicode B.O",
      "Kulasekharam S.O",
      "Thumbode B.O"
    ]
  },
  "629162": {
    "pincode": "629162",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Irenipuram B.O",
      "Mulankuzhi B.O",
      "Kappukadu S.O"
    ]
  },
  "629163": {
    "pincode": "629163",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Madichel B.O",
      "Maruthencode B.O",
      "Padanthalumoodu B.O",
      "Vanniyur B.O",
      "Kuzhithurai S.O",
      "Thiruthuvapuram B.O",
      "Kuzhithurai West S.O"
    ]
  },
  "629164": {
    "pincode": "629164",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kumarapuram B.O",
      "Mukkampala B.O",
      "Velimalai B.O",
      "Manalikarai S.O"
    ]
  },
  "629165": {
    "pincode": "629165",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Nattalam B.O",
      "Viricode B.O",
      "Marthandam S.O",
      "Gnaramvilai B.O",
      "Nallur marthandam B.O",
      "Marthandam Bridge S.O",
      "Marthandam North S.O"
    ]
  },
  "629166": {
    "pincode": "629166",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Chemparuthivilai B.O",
      "Kallankuzhi B.O",
      "Verkilambi B.O",
      "Mekkamandapam S.O"
    ]
  },
  "629167": {
    "pincode": "629167",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kuzhicode B.O",
      "Mulagumoodu S.O",
      "Alagiamandapam B.O",
      "Kozhiporvilai B.O"
    ]
  },
  "629168": {
    "pincode": "629168",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Anducode B.O",
      "Thickurichy B.O",
      "Pacode S.O"
    ]
  },
  "629169": {
    "pincode": "629169",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Kanjikuzhi B.O",
      "Palliyadi S.O"
    ]
  },
  "629170": {
    "pincode": "629170",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Alampara B.O",
      "Kannumamoodu B.O",
      "Manivilai B.O",
      "Palukal S.O"
    ]
  },
  "629171": {
    "pincode": "629171",
    "circle": "Tamilnadu circle",
    "region": "Southern Region, Madurai",
    "division": "Kanniyakumari Division",
    "offices": [
      "Elavuvilai B.O",
      "Killiyoor B.O",
      "Vencode B.O",
      "Pudukadai S.O",
      "Munchirai B.O"
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
