import csv


# ==========================================================
# 1. READ
# ==========================================================

def read_input_csv(input_path: str):
    """
    Reads the CSV and returns:
    - header (list) WITHOUT 'path_number'
    - rows (list of dictionaries) WITHOUT 'path_number'
    """

    with open(input_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        # Remove 'path_number' from header if present
        header = [col for col in reader.fieldnames if col != "path_number"]

        rows = []
        for row in reader:
            # Remove path_number from each row
            row.pop("path_number", None)
            rows.append(row)

    return header, rows



# ==========================================================
# 2. PROCESSING FUNCTIONS
# ==========================================================

def count_intersections(header, rows):
    """
    Adds 'number_of_intersections' field.
    Intersections = number of links - 1.
    """

    if "links" not in header:
        raise ValueError("Column 'links' not found in CSV.")

    for row in rows:
        link_string = row.get("links", "")

        if not link_string.strip():
            intersections = 0
        else:
            links_list = link_string.split(",")
            num_links = len(links_list)
            intersections = max(num_links - 1, 0)

        row["intersections"] = str(intersections)

    # Add new column to header if not already present
    if "intersections" not in header:
        header.append("intersections")

    return header, rows


# ==========================================================
# 3. WRITE (ALL STRUCTURE CONTROLLED HERE)
# ==========================================================

def write_output_csv(header, rows, output_path: str):
    """
    Writes all columns exactly as they exist in 'header'.
    No special treatment.
    """

    with open(output_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

# ==========================================================
# 4. MAIN
# ==========================================================

def main():
    input_file = "shortest_paths.csv"
    output_file = "paths_w_flagged_attributes.csv"

    header, rows = read_input_csv(input_file)

    header, rows = count_intersections(header, rows)

    write_output_csv(header, rows, output_file)

    print(f"Processing complete. Output saved to: {output_file}")


if __name__ == "__main__":
    main()
