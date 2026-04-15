import csv
from pathlib import Path

#DEFINES
FOLDER_PATH = Path(r'C:\Users\kioso\OneDrive - The University of Melbourne\Files\cycling_project\python\14-04-2026')
FILE_DATA_LINKS = FOLDER_PATH / r'Data_Links_reupdated.csv'
FILE_PATHS = FOLDER_PATH / r'shortest_paths_updated.csv'
FILE_OUTPUT = FOLDER_PATH / r'Data_Links_labelled.csv'


with open(FILE_PATHS, newline='', encoding='utf-8') as paths_csv:
    
    paths_reader = csv.DictReader(paths_csv)

    paths_links = {}
    for row in paths_reader:
        if row.get('OD_ID') == 'Y14H0780137P04T01':
            path_id = int(row.get('OD_Trip_Number'))
            links = row.get('links').split(',')
            paths_links[path_id] = set(link for link in links)
            print(f"Got row {path_id}, with links {links}")
        else:
            continue

with open(FILE_DATA_LINKS, newline='', encoding='utf-8') as links_csv, \
     open(FILE_OUTPUT, "w", newline='', encoding='utf-8') as output_csv:

    links_reader = csv.DictReader(links_csv)

    # Create output fieldnames
    fieldnames = ['PFI'] + [f'Trip_{i}' for i in range(1, 16)] + ['Total']
    output_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
    output_writer.writeheader()

    for row in links_reader:
        pfi = row.get('PFI')

        output_row = {'PFI': pfi}

        # Check presence in each trip
        output_row = {'PFI': pfi}
        total = 0

        for i in range(1, 16):
            if pfi in paths_links[i]:
                output_row[f'Trip_{i}'] = 1
                total += 1
            else:
                output_row[f'Trip_{i}'] = 0

        output_row['Total'] = total
        output_writer.writerow(output_row)

print("Done")
