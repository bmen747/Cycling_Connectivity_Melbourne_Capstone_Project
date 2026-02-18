import copy
import csv
import heapq
import math
import random
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# =========================
# SETTINGS
# =========================
k_paths = 20
max_allowed_cost = 100000
penalty_alpha = 1
global_num_procs = 1
num_orig = 5
num_dest = 5
s = time.time()

# =========================
# NODE LOADING
# =========================
def nodes_initialise(file_path="o_nodes.csv"):
    o_nodes = []

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header

        for row in reader:
            if not row or len(row) < 4:
                continue

            node_id = row[0].strip()

            sa1_id = row[1].strip() if row[1].strip() != "0" else None
            sa2_id = row[2].strip() if row[2].strip() != "0" else None
            dzn_id = row[3].strip() if row[3].strip() != "0" else None

            node = [node_id, sa1_id, sa2_id, dzn_id]
            edges = []

            # Edge block size is now 3
            for i in range(4, len(row), 3):
                if i + 2 >= len(row):
                    continue

                link_id = row[i].strip()
                neighbor_id = row[i + 1].strip()

                try:
                    link_weight = float(row[i + 2])
                except ValueError:
                    continue

                edges.append([link_id, neighbor_id, link_weight])

            node.extend(edges)
            o_nodes.append(node)

    return o_nodes

# =========================
# CONVERT TO INTEGER IDS
# =========================
def nodes_to_int_ids(raw_data):
    node_map = {}
    reverse_map = {}
    int_raw_data = []

    for idx, entry in enumerate(raw_data):
        node_map[entry[0]] = idx
        reverse_map[idx] = entry[0]

    for entry in raw_data:
        new_entry = entry[:4]
        new_entry[0] = node_map[entry[0]]

        edges_int = []
        for neighbor in entry[4:]:
            if isinstance(neighbor, list) and len(neighbor) >= 3:
                link_id, neighbor_id, weight = neighbor[:3]

                if neighbor_id not in node_map:
                    continue

                edges_int.append([link_id, node_map[neighbor_id], weight])

        new_entry.extend(edges_int)
        int_raw_data.append(new_entry)

    return int_raw_data, node_map, reverse_map

# =========================
# BUILD GRAPH
# =========================
def build_graph_weights_int(raw_data):
    graph = defaultdict(dict)
    edge_info = {}
    origin_nodes = []
    destination_nodes = []
    sa1_map = {}
    sa2_map = {}
    dzn_map = {}

    for entry in raw_data:
        node_id = entry[0]
        sa1_id, sa2_id, dzn_id = entry[1], entry[2], entry[3]

        if sa1_id or sa2_id:
            origin_nodes.append(node_id)
            if sa1_id:
                sa1_map[node_id] = sa1_id
            if sa2_id:
                sa2_map[node_id] = sa2_id

        if dzn_id:
            destination_nodes.append(node_id)
            dzn_map[node_id] = dzn_id

        for neighbor in entry[4:]:
            if isinstance(neighbor, list) and len(neighbor) >= 3:
                link_id, neighbor_id, weight = neighbor[:3]
                graph[node_id][neighbor_id] = weight
                edge_info[(node_id, neighbor_id)] = link_id

    return graph, edge_info, origin_nodes, destination_nodes, sa1_map, sa2_map, dzn_map


# =========================
# Dijkstra with integers
# =========================
def dijkstra_int(graph, edge_info, start, target,
                 banned_nodes=set(), banned_edges=set(),
                 link_usage=None, penalty_alpha=1.0, max_cost=None):

    heap = [(0, start)]
    visited = {}
    parent = {}
    link_used = {}

    while heap:
        cost, node = heapq.heappop(heap)

        if node in visited and visited[node] <= cost:
            continue
        visited[node] = cost

        if max_cost is not None and cost > max_cost:
            continue

        if node == target:
            path_nodes, path_links = [], []
            cur = node
            while cur is not None:
                path_nodes.append(cur)
                if cur in link_used:
                    path_links.append(link_used[cur])
                cur = parent.get(cur, None)
            return cost, list(reversed(path_nodes)), list(reversed(path_links))

        for neighbor, edge_weight in graph.get(node, {}).items():
            if neighbor in banned_nodes or (node, neighbor) in banned_edges:
                continue

            link_id = edge_info[(node, neighbor)]
            new_cost = cost + edge_weight

            if max_cost is not None and new_cost > max_cost:
                continue

            if neighbor not in visited or new_cost < visited[neighbor]:
                parent[neighbor] = node
                link_used[neighbor] = link_id
                heapq.heappush(heap, (new_cost, neighbor))

    return None, None, None

# =========================
# APPLY PENALTIES
# =========================
def apply_penalties_to_graph(graph, edge_info, link_usage, alpha):
    penalized_graph = defaultdict(dict)

    for u, neighbors in graph.items():
        for v, w in neighbors.items():
            link_id = edge_info[(u, v)]
            penalized_graph[u][v] = w * (alpha * link_usage.get((u, v, link_id), 0) + 1)

    return penalized_graph


# =========================
# GENERATE PATHS
# =========================
_global_graph = None
_global_edge_info = None
_global_k = None
_global_max_cost = None
_global_penalty_alpha = None
_global_origin_nodes = None
_global_destination_nodes = None
_global_sa1_map = None
_global_sa2_map = None
_global_dzn_map = None
_global_reverse_map = None

def _init_worker(graph, edge_info, k, max_cost, penalty_alpha,
                 origin_nodes, destination_nodes, sa1_map, sa2_map, dzn_map, reverse_map):
    global _global_graph, _global_edge_info
    global _global_k, _global_max_cost, _global_penalty_alpha
    global _global_origin_nodes, _global_destination_nodes
    global _global_sa1_map, _global_sa2_map, _global_dzn_map
    global _global_reverse_map

    _global_graph = graph
    _global_edge_info = edge_info
    _global_k = k
    _global_max_cost = max_cost
    _global_penalty_alpha = penalty_alpha
    _global_origin_nodes = origin_nodes
    _global_destination_nodes = destination_nodes
    _global_sa1_map = sa1_map
    _global_sa2_map = sa2_map
    _global_dzn_map = dzn_map
    _global_reverse_map = reverse_map

def kspa_pen(args):
    (
        start_subset,
        graph,
        edge_info,
        k,
        max_cost,
        penalty_alpha,
        destination_nodes,
        sa1_map,
        sa2_map,
        dzn_map
    ) = args

    local_routes = []

    for start in start_subset[:num_orig]:
        print("\nOrigin node:", start, "Run time", time.time() - s, "s")

        for target in destination_nodes[:num_dest]:
            print("Destination node:", target, "Run time", time.time() - s, "s")

            if start == target:
                continue

            A = []
            B = []
            link_usage = defaultdict(int)

            cost, path_nodes, path_links = dijkstra_int(
                graph, edge_info, start, target, max_cost=max_cost
            )

            if path_nodes is None or cost > max_cost:
                continue

            A.append((cost, path_nodes, path_links))

            for u, v, link_id in zip(path_nodes[:-1], path_nodes[1:], path_links):
                link_usage[(u, v, link_id)] += 1

            for kth in range(1, k):

                for i in range(len(A[-1][1]) - 1):
                    spur_node = A[-1][1][i]
                    root_nodes = A[-1][1][:i+1]
                    root_links = A[-1][2][:i]

                    banned_nodes = set(root_nodes[:-1])
                    banned_edges = set()

                    for cost_p, path_p, links_p in A:
                        if len(path_p) > i and path_p[:i+1] == root_nodes:
                            banned_edges.add((path_p[i], path_p[i+1]))

                    penalized_graph = apply_penalties_to_graph(
                        graph, edge_info, link_usage, alpha=penalty_alpha
                    )

                    spur_cost, spur_nodes, spur_links = dijkstra_int(
                        penalized_graph,
                        edge_info,
                        spur_node,
                        target,
                        banned_nodes=banned_nodes,
                        banned_edges=banned_edges,
                        max_cost=max_cost
                    )

                    if spur_nodes:
                        total_nodes = root_nodes[:-1] + spur_nodes
                        total_links = root_links + spur_links

                        total_cost = 0

                        for j in range(len(total_nodes)-1):
                            u = total_nodes[j]
                            v = total_nodes[j+1]
                            w = penalized_graph[u][v]

                            total_cost += w

                        candidate = (total_cost, total_nodes, total_links)

                        if total_cost <= max_cost and candidate not in B:
                            heapq.heappush(B, candidate)

                if not B:
                    break

                next_path = heapq.heappop(B)
                A.append(next_path)

                for u, v, link_id in zip(
                    next_path[1][:-1],
                    next_path[1][1:],
                    next_path[2]
                ):
                    link_usage[(u, v, link_id)] += 1

            local_routes.append({
                "origin": start,
                "origin_sa1": sa1_map.get(start, ""),
                "origin_sa2": sa2_map.get(start, ""),
                "destination": target,
                "destination_dzn": dzn_map.get(target, ""),
                "paths": [
                    {
                        "nodes": p[1],
                        "links": p[2],
                        "method": "penalty"
                    }
                    for p in A
                ]
            })
    
    print("\nPenalty KSPA completed.")

    return local_routes

def kspa_link_elim(args):
    (
        start_subset,
        graph,
        edge_info,
        k,
        max_cost,
        penalty_alpha,
        destination_nodes,
        sa1_map,
        sa2_map,
        dzn_map
    ) = args

    local_routes = []

    for start in start_subset[:num_orig]:
        print("\nOrigin node:", start, "Run time", time.time() - s, "s")

        for target in destination_nodes[:num_dest]:
            print("Destination node:", target, "Run time", time.time() - s, "s")

            if start == target:
                continue

            A = []  # shortest paths found
            B = []  # candidate paths (heap)

            # First shortest path
            cost, path_nodes, path_links = dijkstra_int(
                graph, edge_info, start, target, max_cost=max_cost
            )

            if path_nodes is None or cost > max_cost:
                continue

            A.append((cost, path_nodes, path_links))

            # Generate up to k paths
            for kth in range(1, k):

                for i in range(len(A[-1][1]) - 1):

                    spur_node = A[-1][1][i]
                    root_nodes = A[-1][1][:i+1]
                    root_links = A[-1][2][:i]

                    banned_nodes = set(root_nodes[:-1])
                    banned_edges = set()

                    # Eliminate links that would recreate previous paths
                    for cost_p, path_p, links_p in A:
                        if len(path_p) > i and path_p[:i+1] == root_nodes:
                            banned_edges.add((path_p[i], path_p[i+1]))

                    spur_cost, spur_nodes, spur_links = dijkstra_int(
                        graph,
                        edge_info,
                        spur_node,
                        target,
                        banned_nodes=banned_nodes,
                        banned_edges=banned_edges,
                        max_cost=max_cost
                    )

                    if spur_nodes:

                        total_nodes = root_nodes[:-1] + spur_nodes
                        total_links = root_links + spur_links

                        total_cost = 0
                        for j in range(len(total_nodes)-1):
                            u = total_nodes[j]
                            v = total_nodes[j+1]
                            total_cost += graph[u][v]

                        candidate = (total_cost, total_nodes, total_links)

                        if total_cost <= max_cost and candidate not in B:
                            heapq.heappush(B, candidate)

                if not B:
                    break

                next_path = heapq.heappop(B)
                A.append(next_path)

            local_routes.append({
                "origin": start,
                "origin_sa1": sa1_map.get(start, ""),
                "origin_sa2": sa2_map.get(start, ""),
                "destination": target,
                "destination_dzn": dzn_map.get(target, ""),
                "paths": [
                    {
                        "nodes": p[1],
                        "links": p[2],
                        "method": "link_elimination"
                    }
                    for p in A
                ]
            })

    print("\nLink elimination KSPA completed.")

    return local_routes

def random_paths(args):
    (
        start_subset,
        graph,
        edge_info,
        k,
        max_cost,
        penalty_alpha,   # unused but kept for consistent signature
        destination_nodes,
        sa1_map,
        sa2_map,
        dzn_map
    ) = args

    local_routes = []

    for start in start_subset[:num_orig]:
        print("\nOrigin node:", start, "Run time", time.time() - s, "s")

        for target in destination_nodes[:num_dest]:
            print("Destination node:", target, "Run time", time.time() - s, "s")

            if start == target:
                continue

            A = []

            # Run k times
            for _ in range(k):

                # Create fresh randomized graph each iteration
                randomized_graph = defaultdict(dict)

                for u, neighbors in graph.items():
                    for v, w in neighbors.items():
                        multiplier = random.choice([1, 2, 3])
                        randomized_graph[u][v] = w * multiplier

                cost, path_nodes, path_links = dijkstra_int(
                    randomized_graph,
                    edge_info,
                    start,
                    target,
                    max_cost=max_cost
                )

                if path_nodes is None or cost > max_cost:
                    continue

                A.append((cost, path_nodes, path_links))

            if A:
                local_routes.append({
                    "origin": start,
                    "origin_sa1": sa1_map.get(start, ""),
                    "origin_sa2": sa2_map.get(start, ""),
                    "destination": target,
                    "destination_dzn": dzn_map.get(target, ""),
                    "paths": [
                        {
                            "nodes": p[1],
                            "links": p[2],
                            "method": "random"
                        }
                        for p in A
                    ]
                })

    print("\nRandom path generation completed.")

    return local_routes

# ==============================
# POST PROCESSING
# ==============================

def read_collapsed_links(input_file):
    """
    Reads collapsed link mapping where:
    - Column 1 = New Link ID
    - Remaining columns = Original Links (1..n)
    File is TAB separated.
    """
    collapsed_dict = {}

    with open(input_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header

        for row in reader:
            if not row:
                continue

            new_link_id = row[0].strip()

            # Remaining columns may contain 1..n original links
            original_links = [
                x.strip() for x in row[1:]
                if x and x.strip()
            ]

            collapsed_dict[new_link_id] = original_links

    return collapsed_dict

def expand_link_recursive(link, collapsed_links_map, visited=None):
    """
    Recursively expands a link until only original links remain.
    Handles nested collapsed links safely.
    """

    if visited is None:
        visited = set()

    # Prevent infinite loops (safety guard)
    if link in visited:
        return [link]

    visited.add(link)

    if link in collapsed_links_map:
        expanded = []
        for sub_link in collapsed_links_map[link]:
            expanded.extend(
                expand_link_recursive(sub_link, collapsed_links_map, visited.copy())
            )
        return expanded
    else:
        return [link]


def restore_original_links_in_results(results, collapsed_links_map):
    """
    Fully expands all nested collapsed links and removes _rev suffix.
    """

    for route in results:
        for path in route.get("paths", []):
            fully_expanded = []

            for link in path.get("links", []):
                expanded_links = expand_link_recursive(link, collapsed_links_map)
                fully_expanded.extend(expanded_links)

            # Remove _rev after full expansion
            fully_expanded = [
                l.replace("_rev", "") if isinstance(l, str) else l
                for l in fully_expanded
            ]

            path["links"] = fully_expanded

    return results

def write_paths_to_csv(all_routes, output_file, reverse_map):
    """
    Write computed paths to a CSV file.

    Each row represents one path between an origin and destination pair.
    """
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # CSV header
        writer.writerow([
            'method',
            'origin_node',
            'origin_sa1',
            'destination_node',
            'destination_dzn',
            'path_number',
            'nodes',
            'links',
        ])

        for route in all_routes:
            origin = reverse_map.get(route['origin'], route['origin'])
            destination = reverse_map.get(route['destination'], route['destination'])

            origin_sa1 = route.get('origin_sa1', '')
            origin_sa2 = route.get('origin_sa2', '')
            destination_dzn = route.get('destination_dzn', '')

            for idx, path in enumerate(route.get('paths', []), start=1):
                nodes_str = ",".join(
                    reverse_map.get(n, str(n)) for n in path.get('nodes', [])
                )
                links_str = ",".join(path.get('links', []))

                writer.writerow([
                    path.get("method", ""),
                    origin,
                    origin_sa1,
                    destination,
                    destination_dzn,
                    idx,
                    nodes_str,
                    links_str,
                ])


# =========================
# MAIN
# =========================
def main():
    raw_data = nodes_initialise()
    int_raw_data, node_map, reverse_map = nodes_to_int_ids(raw_data)

    graph, edge_info, origin_nodes, destination_nodes, \
    sa1_map, sa2_map, dzn_map = build_graph_weights_int(int_raw_data)

    # Split origin nodes across processes
    num_procs = global_num_procs
    chunk_size = math.ceil(len(origin_nodes) / num_procs)
    origin_chunks = [
        origin_nodes[i:i + chunk_size]
        for i in range(0, len(origin_nodes), chunk_size)
    ]

    args_list = [
        (
            chunk,
            graph,
            edge_info,
            k_paths,
            max_allowed_cost,
            penalty_alpha,
            destination_nodes,
            sa1_map,
            sa2_map,
            dzn_map
        )
        for chunk in origin_chunks
    ]

    results = []

    results_pen = []
    results_elim = []
    results_rand = []

    if num_procs == 1:
        for args in args_list:
            results_pen.extend(kspa_pen(args))
            results_elim.extend(kspa_link_elim(args))
            results_rand.extend(random_paths(args))
    else:
        with ProcessPoolExecutor(max_workers=num_procs) as executor:
            futures_pen = [executor.submit(kspa_pen, args) for args in args_list]
            futures_elim = [executor.submit(kspa_link_elim, args) for args in args_list]
            futures_rand = [executor.submit(random_paths, args) for args in args_list]

            for future in as_completed(futures_pen):
                results_pen.extend(future.result())

            for future in as_completed(futures_elim):
                results_elim.extend(future.result())

            for future in as_completed(futures_rand):
                results_rand.extend(future.result())


    # Merge both result sets by OD pair
    merged_results = {}

    for route in results_pen + results_elim + results_rand:
        key = (route["origin"], route["destination"])

        if key not in merged_results:
            merged_results[key] = route
        else:
            existing_nodes = {
                tuple(p["nodes"]) for p in merged_results[key]["paths"]
            }

            for p in route["paths"]:
                if tuple(p["nodes"]) not in existing_nodes:
                    merged_results[key]["paths"].append(p)

    results = list(merged_results.values())

    # Load collapsed link mapping
    collapsed_links_map = read_collapsed_links("collapsed_links_log.csv")

    # Restore links before writing output
    results = restore_original_links_in_results(results, collapsed_links_map)

    write_paths_to_csv(results, "shortest_paths.csv", reverse_map)

    print("\nTotal run time:", time.time() - s, "s")


if __name__=="__main__":
    multiprocessing.freeze_support()
    main()
