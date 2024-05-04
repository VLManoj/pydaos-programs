import subprocess

def get_pool_with_max_targets():
    # Listing all pools
    output_bytes = subprocess.check_output(["dmg", "pool", "list"])
    output_string = output_bytes.decode("utf-8")
    lines = output_string.strip().split("\n")[2:]

    # Variables to keep track of the pool with the highest number of targets
    max_targets = 0
    pool_with_max_targets = ""

    # Iterate over each and extract pool name and number of targets
    for line in lines:
        pool_name = line.split()[0]
        # Command to get the number of targets in the pool
        query_output_bytes = subprocess.check_output(["dmg", "pool", "query", pool_name])
        query_output_string = query_output_bytes.decode("utf-8")
        query_lines = query_output_string.strip().split("\n")
        containers = []

        for query_line in query_lines:
            if 'Target(VOS) count' in query_line:
                # Extract the target count 
                num_targets = int(query_line.split(':')[1])
                if num_targets > max_targets:
                    max_targets = num_targets
                    pool_with_max_targets = pool_name
                break  # Stop searching further once the target count is found

    # Return the pool with the highest number of targets
    return pool_with_max_targets



def list_containers_in_pool_with_max_targets():
    # Get the pool with the maximum number of targets
    pool_name = get_pool_with_max_targets()
    
    # Command to list containers in the pool with the maximum number of targets
    output_bytes = subprocess.check_output(["daos", "cont", "list", pool_name])
    output_string = output_bytes.decode("utf-8")
    lines = output_string.strip().split("\n")[2:]

    # Initialize a list to store container names
    containers_in_pool = []

    # Iterate over each line and extract container name
    for line in lines:
        container_name = line.split()[1]
        containers_in_pool.append(container_name)

    # Return the list of container names in the pool with the maximum number of targets
    return pool_name,containers_in_pool
