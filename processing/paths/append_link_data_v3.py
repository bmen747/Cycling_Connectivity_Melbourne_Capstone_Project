import csv
from pathlib import Path

#DEFINES
FOLDER_PATH = Path(r'C:\Users\kioso\OneDrive - The University of Melbourne\Files\cycling_project\python') #Ideally only changing this top path
FILE_LINKS = FOLDER_PATH / r'data_links_updated.csv'
FILE_PATHS = FOLDER_PATH / r'shortest_paths.csv'
FILE_OUTPUT = FOLDER_PATH / r'shortest_paths_updated.csv'

ADDITIONAL_FIELDS = [
    'length',
    'prop_1','prop_2','prop_3','prop_4','prop_5','prop_6','prop_7',
    'lts_1','lts_2','lts_3','lts_4',
    'max_slope','POIs','turns','path_size'
] # These are the fields for the aggregate link metrics being added to each path

ROAD_TYPE_MAP = {
    "arterial_painted_lane": "prop_1",
    "arterial_mixed_traffic": "prop_2",
    "local_mixed_traffic": "prop_3",
    "collector_mixed_traffic": "prop_4",
    "protected_lane": "prop_5",
    "off_road_path": "prop_6",
    "no_type": "prop_7"
} # This is used to convert the road type into a more easily iterable variable - the could be changed to use the actual name with some effort

#Load links database with typecast
def load_links(links_file):
    with open(links_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        links_lookup = {
            row["PFI"]: {
                "length": float(row["length"]),
                "POIs": int(float(row["Flag_POIs"])),
                "slope": float(row["Flag_slope"]),
                "LTS": int(row["Flag_LTS"]),
                "road_type": row["Flag_road_type"],
                "paths" : 0
            }
            for row in reader
        }

    return links_lookup

# For a set of links, calculate aggregate metrics
def calculate_links(links, links_lookup):
    link_results = {k: 0.0 for k in ADDITIONAL_FIELDS} #Creates a dictionary for our link flagged properties.

    # Iterate through each link and add properties to dictionary
    for link in links.split(','):
        links_lookup[link]["paths"] += 1
        length = links_lookup[link]["length"]
        link_results["length"] += length #length add
        link_results["POIs"] += links_lookup[link]["POIs"] # POIs add

        slope = links_lookup[link]["slope"]
        if link_results["max_slope"] < slope: #slope take max
            link_results["max_slope"] = slope

        # Add LTS lengths
        lts = links_lookup[link]["LTS"]
        if 1 <= lts <= 4:
            link_results[f"lts_{lts}"] += length / 1000.0
        else:
            print(f"link {link} LTS not found")
        
        # Add road type lengths, will divide by length at the end
        road_prop = ROAD_TYPE_MAP.get(links_lookup[link]["road_type"])
        if road_prop:
            link_results[road_prop] += length
        else:
            print(f"link {link} roadtype not found")

    # Convert road type results into proportions
    total_length = link_results["length"]
    if total_length > 0:
        for i in range(1,8):
            link_results[f"prop_{i}"] = link_results[f"prop_{i}"] / total_length # Proportion conversion
        link_results["length"] = total_length / 1000.0 # Get final length into km
    else:
        print("Error non-positive length")
    
    return link_results, links_lookup

# Function to calculate the path size variable for each path, based on use of links between many paths
def calculate_path_size(links, links_lookup, path_data):
    Li = path_data["length"]
    path_size = 0
    for link in links.split(','): # Go through each link and add to the path size their contribution
        La = links_lookup[link]["length"] / 1000.0
        overlap = links_lookup[link]["paths"]
        path_size += (La / Li) * (1 / overlap)
    
    path_data["path_size"]=path_size
    return path_data

# Take links and paths file and append the aggregate link metrics to each path, as a seperate output file.
def csv_append(paths_file, links_file, output_file):
    links_lookup = load_links(links_file)
    
    # Open paths
    with open(paths_file, newline='', encoding='utf-8') as paths_csv, \
         open(output_file, "w", newline='', encoding='utf-8') as output_csv:
            
        paths_reader = csv.DictReader(paths_csv)
        fieldnames = paths_reader.fieldnames + ADDITIONAL_FIELDS
        writer = csv.DictWriter(output_csv, fieldnames)
        writer.writeheader()

        # Iterate through paths
        for row in paths_reader:
            data, links_lookup = calculate_links(row['links'],links_lookup)
            data = calculate_path_size(row['links'], links_lookup, data)
            row.update(data)
            writer.writerow(row)
    return 0

if __name__ == "__main__":
    _e = csv_append(FILE_PATHS, FILE_LINKS, FILE_OUTPUT)
    if(_e == 0):
        print("CSVs appended successfully")
    else:
        print(f"Error {_e} while appending CSVs")