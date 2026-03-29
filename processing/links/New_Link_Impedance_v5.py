import csv
import re
import numpy
from datetime import datetime
from pathlib import Path

#DEFINES
FOLDER_PATH = Path(r'C:\Users\kioso\OneDrive - The University of Melbourne\Files\cycling_project\python')
FILE_MRS_LINKS = FOLDER_PATH / r'MRS_Links.1.csv'
FILE_MRS_BIKE_ACCOM = FOLDER_PATH / r'MRS_bike_accom.csv'
FILE_MRS_ROAD_CLASS = FOLDER_PATH / r'MRS_road_classes.csv'
FILE_IN_LINKS = FOLDER_PATH / r'data_links.csv'
FILE_OUT_LINKS = FOLDER_PATH / r'data_links_updated.csv'

def clean_string(s):
    if not isinstance(s, str):
        s = str(s)
    return s.strip().replace('\ufeff', '').replace('\r', '').replace('\n', '')

def class_code_to_link_lanes(infra_type):
    mapping = {
        '0': '4', '1': '3', '2': '2', '3': '2',
        '4': '1', '5': '1', '6': '1', '7': '1',
        '8': '1', '9': '1', '13': '1', '14': '1',
    }
    return mapping.get(str(infra_type).strip(), '')

def load_mrs_links_index(filename):
    index = {}

    with open(filename, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [clean_string(h).lower() for h in reader.fieldnames]

        for row in reader:
            row = {clean_string(k).lower(): clean_string(v) for k, v in row.items()}
            prot_type = row.get('prot_type', '')
            road_class = row.get('road_class', '')
            volume = row.get('volume', '')
            speed_limits = set(row.keys()) - {'prot_type', 'road_class', 'volume'}

            if not prot_type or not road_class or not volume:
                continue

            key = (prot_type, road_class, volume)
            if key not in index:
                index[key] = {}

            for speed_limit in speed_limits:
                index[key][speed_limit] = row[speed_limit]

    print(f"Loaded MRS_Links index with {len(index)} keys")
    #print(index)
    return index

def load_transformation(filename):
    index = {}

    with open(filename, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [clean_string(h).lower() for h in reader.fieldnames]

        for row in reader:
            row = {clean_string(k).lower(): clean_string(v) for k, v in row.items()}
            body = set(row.keys()) - {"key"}

            key = row.get('key', '')
            if key not in index:
                index[key] = {}

            for item in body:
                index[key][item] = row[item]

    print(f"Loaded transformation file with {len(index)} keys")
    
    return index

def add_f_roadway_to_Data_Links_Rev3(mrs_links_index, f_bikeaccom_index, f_road_class_index, input_file, output_file):
    print(f"Starting processing of '{input_file}'")

    missing_f_roadway_count = 0
    missing_f_bikeaccom_count = 0
    total_rows = 0
    written_rows = 0
    filtered_rows_count = 0
    defaulted_linklanes_count = 0
    defaulted_speed_count = 0
    defaulted_slope_count = 0
    speed_override_count = 0

    with open(input_file, newline='', encoding='utf-8-sig') as fin, \
         open(output_file, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        reader.fieldnames = [clean_string(h) for h in reader.fieldnames]

        # Add new derived columns
        new_fields = ['Flag_slope','Flag_LTS','Flag_road_type','Flag_POIs']

        for field in new_fields:
            if field not in reader.fieldnames:
                reader.fieldnames.append(field)

        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            total_rows += 1
            if (total_rows > 999 and  total_rows // 1000 == 0):
                print(f"Processed {total_rows} rows")
            row = {clean_string(k): clean_string(v) for k, v in row.items()}

            class_code = f_road_class_index.get(row.get('highway', 'missing').strip()).get("value")


            # Speed
            sl_speed_limit_final = row.get('Road_speed_limit','missing')
            if sl_speed_limit_final == 'missing' or sl_speed_limit_final == '':
                defaulted_speed_count += 1
                sl_speed_limit_final = '50'
            if(int(sl_speed_limit_final) < 30): # set speeds < 30 to 30
                sl_speed_limit_final = '30'
            rem = 0
            if(int(sl_speed_limit_final) != 0): # round speed up to nearest 10
                rem = 10
            sl_speed_limit_final = str((int(sl_speed_limit_final) // 10) * 10 + rem)               


            # setup for accom
            accommodation = row.get('Bike_lane', 'no').strip().lower()
            prot = row.get('Bike_lane_protection', '').strip()
            LTS_value = ''

            # F_bikeaccom logic
            f_bikeaccom_value = 0
            f_bikeaccom = 0
            if accommodation in f_bikeaccom_index:
                f_bikeaccom_value = f_bikeaccom_index.get(accommodation, {}).get("value")
            else:
                #print(f"{accommodation} not in index")
                f_bikeaccom_value = 1
                missing_f_bikeaccom_count += 1
            try: 
                f_bikeaccom = int(f_bikeaccom_value)
            except Exception:
                f_bikeaccom = 0
                missing_f_bikeaccom_count += 1
            #print(f'A: {accommodation}, B: {f_bikeaccom}, P: {prot}')
            if f_bikeaccom == 2:
                if(prot == 'yes' or prot == 'both'):
                    f_bikeaccom = 3

            #Slope
            try: 
                slope = float(row.get('slope_pct'))
            except Exception:
                slope = 0.0
                defaulted_slope_count += 1
            
            #LTS
            # Get volume, will filter later
            volume = 0
            try:
                volume = int(row.get('Volume', ''))
            except Exception:
                volume = 1

            #work out road class to use in key, as options change
            try: 
                road_class_base = class_code
            except Exception:
                road_class_base = 'p'
            road_class = ''

            if(f_bikeaccom > 2):
                road_class = 'any'
            elif(f_bikeaccom == 2):
                if(road_class_base == 'p'):
                    road_class = road_class_base
                else:
                    road_class = 'l,t,s'
            elif(volume < 2001 and road_class_base == 'l'):
                road_class = 'l'
            elif(road_class_base == 'l' or road_class_base == 't'):
                road_class = 'l,t'
            else:
                road_class = 's,p'

            #Road type
            if(f_bikeaccom == 4): #Off-road
                road_type = 'off_road_path'
            elif(f_bikeaccom == 3): #protected
                road_type = 'protected_lane'
            elif(f_bikeaccom == 1):
                if(class_code == 'p'):
                    road_type = 'arterial_mixed_traffic'
                elif(class_code == 's'):
                    road_type = 'collector_mixed_traffic'
                else:
                    road_type = 'local_mixed_traffic'
            elif(f_bikeaccom == 2 and class_code == 'p'):
                road_type = 'arterial_painted_lane'
            else:
                road_type = 'no_type'

            #Now work out what volume key is
            volume_key = ''
            if f_bikeaccom > 2:
                volume_key = 'any'
            elif(f_bikeaccom == 2):
                 if(road_class_base == 'p' or volume > 10000):
                    volume_key = 'any'
                 else:
                     volume_key = 10000
            elif(road_class == 's,p' or (road_class == 'l,t' and volume > 3000)):
                volume_key = 'any'
            elif(road_class == 'l'):
                if(volume < 751):
                    volume_key = 750
                else:
                    volume_key = 2000
            else:
                volume_key = 3000

            key = (str(f_bikeaccom), road_class, str(volume_key))
            if int(sl_speed_limit_final) <= 60:
                LTS_value = mrs_links_index[key][sl_speed_limit_final]
            else:
                LTS_value = mrs_links_index[key]['any']
        
            #if infra_type in ['Separated path (off-road)', 'Shared use path (off-road)'] or 'trail' in ftype_code:
            #    LTS_value = '1'
                        
            #POIs
            pois = numpy.random.randint(0,1500)

            # Store final processed values in new columns

            row['Flag_slope']=f"{slope:.6f}"
            row['Flag_LTS']= LTS_value
            row['Flag_road_type']= road_type
            row['Flag_POIs']=f"{pois:.3f}"

            writer.writerow(row)
            written_rows += 1

            if total_rows % 1000 == 0:
                print(f" Processed {total_rows} rows...")

    print(f" Finished processing {total_rows} rows, wrote {written_rows}")
    print(f" Missing F_roadway: {missing_f_roadway_count} rows")
    print(f" Missing F_bikeaccom: {missing_f_bikeaccom_count} rows")
    print(f" Filtered WURUNDJERI WAY / tunnel rows: {filtered_rows_count}")
    print(f" Defaulted Link_Lanes: {defaulted_linklanes_count} rows")
    print(f" Defaulted SL_speed_limit: {defaulted_speed_count} rows")
    print(f" Slope values defaulted: {defaulted_slope_count}")
    print(f" SL_speed_limit overrides from OS1_other_tags: {speed_override_count}")

def extract_wlink_and_pfi(input_file, output_file):
    print(f" Extracting 'W_link' and 'PFI' from '{input_file}' to '{output_file}'")

    with open(input_file, newline='', encoding='utf-8-sig') as fin, \
         open(output_file, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=['PFI', 'W_link'])
        writer.writeheader()

        for row in reader:
            pfi = row.get('ID_Cleaned', '')
            w_link = row.get('W_link', '')
            writer.writerow({'PFI': pfi, 'W_link': w_link})

    print(f" Extraction complete: '{output_file}' created.")

if __name__ == "__main__":
    print(" Script started")

    mrs_links_index = load_mrs_links_index(FILE_MRS_LINKS)
    f_bikeaccom_index = load_transformation(FILE_MRS_BIKE_ACCOM)
    f_road_classes_index = load_transformation(FILE_MRS_ROAD_CLASS)

    add_f_roadway_to_Data_Links_Rev3(mrs_links_index, f_bikeaccom_index, f_road_classes_index, FILE_IN_LINKS, FILE_OUT_LINKS)

    print("Complete")














