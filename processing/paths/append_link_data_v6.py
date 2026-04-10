import csv
import numpy as math
from pathlib import Path

#DEFINES
FOLDER_PATH = Path(r'C:\Users\kioso\OneDrive - The University of Melbourne\Files\cycling_project\python') #Ideally only changing this top path
FILE_LINKS = FOLDER_PATH / r'data_links_updated.csv'
FILE_PATHS = FOLDER_PATH / r'shortest_paths.csv'
FILE_OUTPUT = FOLDER_PATH / r'shortest_paths_updated.csv'

ANGLE_MAX = math.deg2rad(30) # Maximum angle deviation at a link at which a path is considered straight
LONGITUDE_SCALAR = math.cos(math.deg2rad(-37.8136)) # Melbourne latitude to scale longitude for angle purposes.

ADDITIONAL_FIELDS = [
    'length',
    "prop_arterial_painted_lane", "prop_arterial_mixed_traffic", "prop_local_mixed_traffic", "prop_collector_mixed_traffic", "prop_protected_lane", "prop_off_road_path", "prop_no_type",
    'lts_1','lts_2','lts_3','lts_4',
    'scen_lts_1','scen_lts_2','scen_lts_3','scen_lts_4',
    'max_slope','POIs','turns','path_size', 'non_intersecting'
] # These are the fields for the aggregate link metrics being added to each path

ROAD_TYPE_MAP = {
    "arterial_painted_lane": "prop_arterial_painted_lane",
    "arterial_mixed_traffic": "prop_arterial_mixed_traffic",
    "local_mixed_traffic": "prop_local_mixed_traffic",
    "collector_mixed_traffic": "prop_collector_mixed_traffic",
    "protected_lane": "prop_protected_lane",
    "off_road_path": "prop_off_road_path",
    "no_type": "prop_no_type"
} # This is used to convert the road type into a more easily iterable variable - the could be changed to use the actual name with some effort

# Find shared endpoint of the two lines, line1, line2: ((lat1, lon1), (lat2, lon2))
def find_shared_endpoint(line1, line2, tol):
    for p1 in line1:
        for p2 in line2:
            if abs(p1[0] - p2[0]) < tol and abs(p1[1] - p2[1]) < tol:
                nonshared1 = line1[0] if p1 == line1[1] else line1[1]
                nonshared2 = line2[0] if p2 == line2[1] else line2[1]
                return 0, p1, nonshared1, nonshared2
    return 1, p1, line1[0], line2[0] # Non-intersection

#line1, line2: ((lat1, lon1), (lat2, lon2)) Returns: angle in degrees
def angle_between_lines(line1, line2, tol):

    # Step 1: enforce endpoint intersection
    status, shared, p1, p2 = find_shared_endpoint(line1, line2, tol)
    if status == 1: # Non-intersecting
        return 0, 1

    # Step 2: convert to local Cartesian coords
    sx, sy = shared[0], shared[1]*LONGITUDE_SCALAR
    x1, y1 = p1[0], p1[1]*LONGITUDE_SCALAR
    x2, y2 = p2[0], p2[1]*LONGITUDE_SCALAR

    # Step 3: vectors from shared point
    ux, uy = x1 - sx, y1 - sy
    vx, vy = x2 - sx, y2 - sy

    # Step 4: dot product angle
    dot = ux * vx + uy * vy
    mag_u = math.hypot(ux, uy)
    mag_v = math.hypot(vx, vy)

    if mag_u == 0 or mag_v == 0:
        raise ValueError("One of the lines has zero length")

    cos_theta = dot / (mag_u * mag_v)
    cos_theta = max(-1.0, min(1.0, cos_theta))  # numerical safety

    return math.degrees(math.acos(cos_theta)), 0

def angle_between_segments(line1, line2):

    # Get points
    x1, y1 = line1[0][0], line1[0][1]*LONGITUDE_SCALAR
    x2, y2 = line1[1][0], line1[1][1]*LONGITUDE_SCALAR
    x3, y3 = line2[0][0], line2[0][1]*LONGITUDE_SCALAR
    x4, y4 = line2[1][0], line2[1][1]*LONGITUDE_SCALAR

    # Direction vectors
    ux, uy = x2 - x1, y2 - y1
    vx, vy = x4 - x3, y4 - y3

    # Magnitudes
    mag_u = math.hypot(ux, uy)
    mag_v = math.hypot(vx, vy)

    if mag_u == 0 or mag_v == 0:
        raise ValueError("One of the lines has zero length")

    # Dot product
    dot = ux * vx + uy * vy
    cos_theta = dot / (mag_u * mag_v)

    # Clamp for safety
    cos_theta = max(-1.0, min(1.0, cos_theta))

    angle = math.degrees(math.acos(cos_theta))

    return angle

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
                "scen_LTS": int(row["Flag_Scenario_LTS"]),
                "paths" : 0,
                "x1" : float(row["x1 start long"]),
                "x2" : float(row["x2 end long"]),
                "y1" : float(row["y1 start lat"]),
                "y2" : float(row["y2 end lat"])
            }
            for row in reader
        }

    return links_lookup

# For a set of links, calculate aggregate metrics
def calculate_links(links, links_lookup):
    link_results = {k: 0.0 for k in ADDITIONAL_FIELDS} #Creates a dictionary for our link flagged properties.
    linkset = links.split(',')

    # Iterate through each link and add properties to dictionary
    for link in linkset:
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
        
        scen_lts = links_lookup[link]["scen_LTS"]
        if 1 <= scen_lts <= 4:
            link_results[f"scen_lts_{scen_lts}"] += length / 1000.0
        else:
            print(f"link {link} LTS not found")
        
        # Add road type lengths, will divide by length at the end
        road_prop = ROAD_TYPE_MAP.get(links_lookup[link]["road_type"])
        if road_prop:
            link_results[road_prop] += length
        else:
            print(f"link {link} roadtype not found")
    
    # Calculation of number of turns
    link_count = 0
    turns = 0
    non_intersecting = 0
    prev_link = 0

    for link in linkset:
        # skip first link
        link_count +=1
        if link_count == 1:
            prev_link = link
            continue

        # Generate lines
        
        line1 = (links_lookup[prev_link]["x1"], links_lookup[prev_link]["y1"]), (links_lookup[prev_link]["x2"], links_lookup[prev_link]["y2"])
        line2 = (links_lookup[link]["x1"], links_lookup[link]["y1"]), (links_lookup[link]["x2"], links_lookup[link]["y2"])

        prev_link = link
        angle_1, status = angle_between_lines(line1, line2, 1e-6)
        angle = angle_between_segments(line1, line2)
        if(status == 1): # Non-intersecting
            non_intersecting +=1
            continue
        if (angle < ANGLE_MAX): # Is there a turn?
            continue
        turns += 1 # Yes there is
    
    link_results["turns"] = turns
    link_results["non_intersecting"] = non_intersecting

    # Convert road type results into proportions
    total_length = link_results["length"]
    keys = ROAD_TYPE_MAP.values()
    if total_length > 0:
        for key in keys:
            link_results[key] = link_results[key] / total_length # Proportion conversion
        link_results["length"] = total_length / 1000.0 # Get final length into km
    else:
        print("Error non-positive length")
    
    return link_results, links_lookup

# Function to calculate the path size variable for each path, based on use of links between many paths
def calculate_path_size(links, links_lookup, path_data):
    try:
        Li = path_data["length"]
    except Exception:
        print(path_data)
    path_size = 0
    for link in links.split(','): # Go through each link and add to the path size their contribution
        La = links_lookup[link]["length"] / 1000.0
        overlap = links_lookup[link]["paths"]
        path_size += (La / Li) * (1 / overlap)
    
    path_data["path_size"]=path_size
    return path_data

# Small function to reset link-level path size variable
def reset_links(links, links_lookup):
    for link in links.split(','):
        links_lookup[link]["paths"] = 0
    return

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

        # Iterate through ODs and calculate path size
        paths_reader = list(paths_reader)
        ODs = list({row["OD_ID"] for row in paths_reader})
        
        pairs_done = 0
        for OD_pair in ODs:
            pairs_done += 1
            if (pairs_done > 99 and  pairs_done % 100 == 0):
                print(f"Processed {pairs_done} OD_Pairs")
            
            paths_dataset = {}
            for row in paths_reader: # Calculate statistics for path (except size)
                if row["OD_ID"] == OD_pair:       
                    data, links_lookup = calculate_links(row['links'],links_lookup)
                    paths_dataset[OD_pair + row["OD_Trip_Number"]] = data
                else:
                    continue
            
            for row in paths_reader: # Calculate path size
                if row["OD_ID"] == OD_pair:
                    data = paths_dataset.get(OD_pair + row["OD_Trip_Number"])
                    data = calculate_path_size(row['links'], links_lookup, data)
                    row.update(data)
                    writer.writerow(row)
                else:
                    continue
                    
            for row in paths_reader: # Reset paths count for path size before starting next OD pair.
                if row["OD_ID"] == OD_pair:  
                    reset_links(row['links'],links_lookup)
                else:
                    continue
    return 0

if __name__ == "__main__":
    _e = csv_append(FILE_PATHS, FILE_LINKS, FILE_OUTPUT)
    if(_e == 0):
        print("CSVs appended successfully")
    else:
        print(f"Error {_e} while appending CSVs")