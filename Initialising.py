## Initialise CRS for KSPA
import csv
import math
import time
import re
from datetime import datetime
from collections import defaultdict

## Flag categories
# Distance - information already procided within CRS - remove row where provided and place with other flagged categories for ease of reading

# Infrastructure

# LTS - Code snippet taken from other code set, may need to be reworked to fit this code set

# -------------------- CSV Loading Functions --------------------
def load_mrs_links_index(filename):
    index = {}
    print_timestamped(f"Loading MRS_Links index from '{filename}'")

    with open(filename, newline='', encoding='utf-8-sig') as f:
        next(f)
        reader = csv.DictReader(f)
        reader.fieldnames = [clean_string(h).lower() for h in reader.fieldnames]

        for row in reader:
            row = {clean_string(k).lower(): clean_string(v) for k, v in row.items()}
            link_lanes = row.get('link_lanes', '')
            sl_speed_limit = row.get('sl_speed_limit', '')
            accommodation_codes = set(row.keys()) - {'link_lanes', 'sl_speed_limit'}

            if not link_lanes or not sl_speed_limit:
                continue

            key = (link_lanes, sl_speed_limit)
            if key not in index:
                index[key] = {}

            for accom in accommodation_codes:
                index[key][accom] = row[accom]

    print_timestamped(f"Loaded MRS_Links index with {len(index)} keys")
    return index

def load_f_bikeaccom_row(filename):
    print_timestamped(f"Loading F_bikeaccom row from '{filename}'")

    with open(filename, newline='', encoding='utf-8-sig') as f:
        next(f)
        reader = csv.DictReader(f)
        reader.fieldnames = [clean_string(h).lower() for h in reader.fieldnames]

        for row in reader:
            row = {clean_string(k).lower(): clean_string(v) for k, v in row.items()}
            if row.get('link_lanes', '').lower() == 'f_bikeaccom':
                print_timestamped("Found F_bikeaccom row")
                return {k.lower(): v for k, v in row.items() if k not in ['link_lanes', 'sl_speed_limit']}

    print_timestamped("F_bikeaccom row not found in MRS_Links.csv")
    return {}

# -------------------- Main Processing --------------------
def process_lts(input_file, output_file, mrs_links_index, f_bikeaccom_row):
    print_timestamped(f"Processing '{input_file}' for LTS related outputs")

    with open(input_file, newline='', encoding='utf-8-sig') as fin, \
         open(output_file, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        reader.fieldnames = [clean_string(h) for h in reader.fieldnames]

        new_fields = ['PFI', 'Link_Lanes_final', 'SL_speed_limit_final',
                      'Cycling_Accommodation', 'F_stress', 'LTS_Final']
        writer = csv.DictWriter(fout, fieldnames=new_fields)
        writer.writeheader()

        for row in reader:
            row = {clean_string(k): clean_string(v) for k, v in row.items()}

            ezi_rdname = row.get('EZI_RDNAME', '').strip().lower()
            ftype_code_val = row.get('FTYPE_CODE', '').strip().lower()

            # --- Filter out tunnels and Wurundjeri Way ---
            if 'wurundjeri way' in ezi_rdname or 'tunnel' in ftype_code_val:
                continue

            class_code = row.get('CLASS_CODE', '').strip()

            # --- Skip rows with CLASS_CODE == '0' ---
            if class_code == '0':
                continue

            fw_penalty = FW_Penalty(class_code)
            os1_tags = row.get('OS1_other_tags', '')

            # --- Lanes ---
            match_lanes = re.search(r'"lanes"\s*=>\s*"(\d+)"', os1_tags)
            if match_lanes:
                link_lanes_final = match_lanes.group(1)
            else:
                link_lanes_final = class_code_to_link_lanes(class_code)
            if not link_lanes_final:
                link_lanes_final = '1'

            # --- Speed ---
            match_speed = re.search(r'"maxspeed"\s*=>\s*"(\d+)"', os1_tags)
            if match_speed:
                sl_speed_limit_final = match_speed.group(1)
            else:
                sl_speed_limit_final = row.get('SL_speed_limit', '').strip()
            if not sl_speed_limit_final:
                sl_speed_limit_final = '40'

            # --- Infrastructure type and overrides ---
            accommodation_raw = row.get('CY2_InfraType', '').strip()
            accommodation = accommodation_raw.lower() if accommodation_raw else 'intermittent/informal (on-road)'

            ftype_code = row.get('FTYPE_CODE', '').lower()
            key = (link_lanes_final, sl_speed_limit_final)

            f_roadway_value = mrs_links_index.get(key, {}).get(accommodation, '')
            f_bikeaccom_value = f_bikeaccom_row.get(accommodation, '')

            # --- Trail & off-road path overrides ---
            if accommodation_raw in ['Separated path (off-road)', 'Shared use path (off-road)'] or 'trail' in ftype_code:
                f_roadway_value = '3'
            if ftype_code == 'trail':
                f_bikeaccom_value = '75'

            # --- Convert and calculate F_stress ---
            try:
                f_roadway = float(f_roadway_value) / 100 if f_roadway_value else 0.0
            except:
                f_roadway = 0.0

            try:
                f_bikeaccom = float(f_bikeaccom_value) / 100 if f_bikeaccom_value else 0.0
            except:
                f_bikeaccom = 0.0

            # Base F_stress calculation
            f_stress = f_roadway * (1 - f_bikeaccom)

            # Apply only FW penalty (scaled)
            f_stress += fw_penalty / 1000.0

            # --- Discretise F_stress into LTS levels ---
            if f_stress <= 0.1:
                lts_final = 1
            elif f_stress <= 0.3:
                lts_final = 2
            elif f_stress <= 0.6:
                lts_final = 3
            else:
                lts_final = 4

            output_row = {
                'PFI': row.get('PFI', ''),
                'Link_Lanes_final': link_lanes_final,
                'SL_speed_limit_final': sl_speed_limit_final,
                'Cycling_Accommodation': accommodation_raw,
                'F_stress': f"{f_stress:.3f}",
                'LTS_Final': lts_final
            }

            writer.writerow(output_row)

    print_timestamped(f"Finished writing LTS outputs to '{output_file}'")

# -------------------- Entry Point --------------------
if __name__ == "__main__":
    mrs_links_index = load_mrs_links_index('MRS_Links.1.csv')
    f_bikeaccom_row = load_f_bikeaccom_row('MRS_Links.1.csv')

    process_lts('data_links.csv', 'LTS_Output.csv', mrs_links_index, f_bikeaccom_row)

# Slope

# POI


## Provide factor


## Calculate disutility

####
## CODE PROVIDED - NEEDS CHANGES: Initialise origin destination data
####

# SA1 origins
origin_nodes_sa1 = {}
with open('SA1s_30.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try: x, y = float(row['x']), float(row['y'])
        except: x, y = 0.0, 0.0
        origin_nodes_sa1[row['SA1_MAIN16']] = (x, y)

# SA2 origins
origin_nodes_sa2 = {}
with open('SA2s_30.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sa2_id = row['SA2_MAIN16']
        if sa2_id not in origin_nodes_sa2:
            try: x, y = float(row['x']), float(row['y'])
            except: x, y = 0.0, 0.0
            origin_nodes_sa2[sa2_id] = (x, y)

# Destinations
destination_nodes = {}
with open('DZNs_30.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try: x, y = float(row['x']), float(row['y'])
        except: x, y = 0.0, 0.0
        destination_nodes[row['DZN_CODE16']] = (x, y)


####
## CODE PROVIDED - NEEDS CHANGES: Trim and initialise CRS
####

def euclidean_distance(x1, y1, x2, y2):
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)

s = time.time()

## Note that this code has been taken from a different set of stand alone code that initialises data from CRS's.
## In this code these results should be taken from above. The rest should follow

# === Step 3a: Group links by FROM_UFI ===
def group_by_node_id(data, nodes):
    grouped = defaultdict(list)
    for entry in data:
        node_id = entry['FROM_UFI']
        link_id = entry['PFI']
        adjacent_node = entry['TO_UFI']
        try:
            link_weight = float(entry['length']) * float(entry['weight'])
        except ValueError:
            link_weight = 0.0
        x, y, node_weight = nodes.get(node_id, (0.0, 0.0, 0.0))
        adj_x, adj_y, _ = nodes.get(adjacent_node, (0.0, 0.0, 0.0))
        grouped[node_id].append([link_id, adjacent_node, link_weight, adj_x, adj_y])
    result = []
    for node_id, links in grouped.items():
        x, y, node_weight = nodes.get(node_id, (0.0, 0.0, 0.0))
        result.append([node_id, node_weight, x, y] + links)
    return result

# === Step 3b: Iterative simplification (prune dead-ends, collapse deg-2, remove duplicates) ===
def simplify_network(grouped_data):
    """
    Simplifies the network in the correct order:
    1. Prune dead-end nodes
    2. Collapse degree-2 nodes
    3. Remove duplicate adjacencies
    4. Ensure all adjacent nodes exist as starting nodes
    """
    collapsed_links_log = []
    link_origins = {}

    # Initialize link origins
    for node_entry in grouped_data:
        for link in node_entry[4:]:
            if isinstance(link, (list, tuple)):
                link_id = link[0]
                if link_id not in link_origins:
                    link_origins[link_id] = [link_id]

    changed = True
    while changed:
        changed = False
        node_dict = {n[0]: n for n in grouped_data}
        to_remove = set()

        # --- Step 1: prune dead ends ---
        for node_id, node_entry in node_dict.items():
            adjacencies = [l for l in node_entry[4:] if isinstance(l, (list, tuple))]
            if len(adjacencies) <= 1:
                to_remove.add(node_id)
                changed = True

        # Remove dead-end nodes
        if to_remove:
            grouped_data = [n for n in grouped_data if n[0] not in to_remove]
            for node_entry in grouped_data:
                node_entry[:] = node_entry[:4] + [l for l in node_entry[4:] if l[1] not in to_remove]

        # --- Step 2: collapse degree-2 nodes ---
        node_dict = {n[0]: n for n in grouped_data}  # refresh after pruning
        to_remove = set()
        for node_id, node_entry in list(node_dict.items()):
            adjacencies = [l for l in node_entry[4:] if isinstance(l, (list, tuple))]
            if len(adjacencies) == 2:
                link1, link2 = adjacencies
                n1, w1, n2, w2 = link1[1], link1[2], link2[1], link2[2]
                n1x, n1y = link1[3], link1[4]
                n2x, n2y = link2[3], link2[4]

                # Only collapse if both neighbors exist
                if n1 not in node_dict or n2 not in node_dict:
                    continue

                # Create new collapsed link
                new_weight = w1 + w2
                new_link_id = f"collapsed_{node_id}_{n1}_{n2}"
                ancestry = link_origins.get(link1[0], [link1[0]]) + link_origins.get(link2[0], [link2[0]])
                link_origins[new_link_id] = ancestry
                collapsed_links_log.append([new_link_id] + ancestry)

                # Update neighbors
                new_link_1 = [new_link_id, n2, new_weight, n2x, n2y]
                new_link_2 = [new_link_id, n1, new_weight, n1x, n1y]

                for neighbor_id, new_link in [(n1, new_link_1), (n2, new_link_2)]:
                    neighbor_entry = node_dict.get(neighbor_id)
                    if neighbor_entry:
                        updated_links = []
                        for l in [item for item in neighbor_entry[4:] if isinstance(item, (list, tuple))]:
                            if l[1] == node_id:
                                updated_links.append(new_link)
                            else:
                                updated_links.append(l)
                        neighbor_entry[:] = neighbor_entry[:4] + updated_links

                # Mark the collapsed node for removal
                to_remove.add(node_id)
                changed = True

        # Remove collapsed nodes
        if to_remove:
            grouped_data = [n for n in grouped_data if n[0] not in to_remove]

        # --- Step 3: remove duplicate adjacencies ---
        valid_nodes = set(entry[0] for entry in grouped_data)
        for node_entry in grouped_data:
            unique_links = {}
            for link in [l for l in node_entry[4:] if isinstance(l, (list, tuple))]:
                neighbor = link[1]
                weight = link[2]
                # Only keep links to valid nodes
                if neighbor not in valid_nodes:
                    continue
                if neighbor not in unique_links or weight < unique_links[neighbor][2]:
                    unique_links[neighbor] = link
            node_entry[:] = node_entry[:4] + list(unique_links.values())

    # --- Final Step: backfill collapsed link log to match o_nodes ---
    logged_ids = {entry[0] for entry in collapsed_links_log}
    for node_entry in grouped_data:
        for link in node_entry[4:]:
            if isinstance(link, (list, tuple)):
                link_id = link[0]
                if link_id.startswith("collapsed_") and link_id not in logged_ids:
                    ancestry = link_origins.get(link_id, [link_id])
                    collapsed_links_log.append([link_id] + ancestry)

    return grouped_data, collapsed_links_log

# === Step 4: Flag OD nodes ===
def flag_OD_nodes(grouped_data, od_nodes):
    simple_nodes = [(entry[0], entry[2], entry[3]) for entry in grouped_data]
    od_to_node = {}
    for od_id, (ox, oy) in od_nodes.items():
        best_id, best_d2 = None, float('inf')
        for node_id, gx, gy in simple_nodes:
            dx, dy = ox-gx, oy-gy
            d2 = dx*dx + dy*dy
            if d2 < best_d2:
                best_d2, best_id = d2, node_id
        od_to_node[od_id] = best_id
    return od_to_node

def add_od_flags_to_grouped(grouped_data, origin_map_sa1, origin_map_sa2, destination_map):
    reverse_origin_sa1 = {}
    for sa1, node in origin_map_sa1.items():
        if node: reverse_origin_sa1.setdefault(str(node), []).append(sa1)
    reverse_origin_sa2 = {}
    for sa2, node in origin_map_sa2.items():
        if node: reverse_origin_sa2.setdefault(str(node), []).append(sa2)
    reverse_dest = {}
    for dzn, node in destination_map.items():
        if node: reverse_dest.setdefault(str(node), []).append(dzn)

    updated = []
    for row in grouped_data:
        node_id = str(row[0])
        sa1_list = reverse_origin_sa1.get(node_id, [])
        sa2_list = reverse_origin_sa2.get(node_id, [])
        dzn_list = reverse_dest.get(node_id, [])
        new_row = row[:4] + [(";".join(sa1_list) if sa1_list else 0),
                              (";".join(sa2_list) if sa2_list else 0),
                              (";".join(dzn_list) if dzn_list else 0)] + row[4:]
        updated.append(new_row)
    return updated

# === Step 5: CSV output ===
def write_to_csv(filename, data):
    max_links = max(sum(1 for item in entry[6:] if isinstance(item,(list,tuple))) for entry in data)
    header = ['Node ID','Node Weight','X','Y','Origin_SA1','Origin_SA2','Destination_DZN']
    for _ in range(max_links):
        header += ['Link ID','Adjacent Node','Link Weight','Adjacent X','Adjacent Y']

    with open(filename,'w',newline='',encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in data:
            out = row[:7]
            links = [l for l in row[7:] if isinstance(l,(list,tuple))]
            for l in links: out += (list(l)+['','','','',''])[:5]
            for _ in range(max_links-len(links)): out += ['','','','','']
            writer.writerow(out)

# --- Execute full process ---
grouped_data = group_by_node_id(data, nodes)
grouped_data, collapsed_links_log = simplify_network(grouped_data)

origin_map_sa1 = flag_OD_nodes(grouped_data, origin_nodes_sa1)
origin_map_sa2 = flag_OD_nodes(grouped_data, origin_nodes_sa2)
destination_map = flag_OD_nodes(grouped_data, destination_nodes)
grouped_data = add_od_flags_to_grouped(grouped_data, origin_map_sa1, origin_map_sa2, destination_map)

write_to_csv('o_nodes_30_upgraded.csv', grouped_data)

with open('collapsed_links_log_upg.csv','w',newline='',encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["New Link ID","Original Links"])
    for entry in collapsed_links_log: writer.writerow(entry)

print("Total run time:", time.time()-s, "s")


