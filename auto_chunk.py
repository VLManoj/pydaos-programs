import os
import json
import time
from pydaos import DCont, DDict
import pool_test

# Create a DAOS container
def get_daos_container():
    pool, containers = pool_test.list_containers_in_pool_with_max_targets()
    for container in containers:
        try:
            return DCont(pool, container, None), pool, container
        except Exception as e:
            print(f"Error accessing container {container}: {e}")
            continue
upload_dir = "uploads"
os.makedirs(upload_dir, exist_ok=True)

n = int(input("Enter size of chunks (in MB): "))
CHUNK_SIZE = n * 1024 * 1024
 #Function to print  the data associated with a given key from t
def print_help():
    print("?\t- Print this help")
    print("r\t- Read a key")
    print("u\t- Upload file for a new key")
    print("p\t- delete a key")
    print("d\t- Display keys")
    print("q\t- Quit")
def save_value_as_file(key,value):
    filename = os.path.join(upload_dir, f"{key}.dat")
    with open(filename, "wb") as f:
        f.write(value)
    print(f"Value saved as file: {filename}")
def print_keys():
    try:
        with open("metadata.json", 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print("Error: 'metadata.json' file not found.")
        return None

    for i, item in enumerate(data):
        print(item["key"])
def delete_by_key(key):
    try:
        with open("metadata.json", 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print("Error: 'metadata.json' file not found.")
        return None

    new_data = [item for item in data if item["key"] != key]
    if len(new_data) < len(data):
        with open("metadata.json", 'w') as file:
            json.dump(new_data, file, indent=4)
        print(f"All data associated with key '{key}' deleted.")
    else:
        print(f"Key '{key}' not found in the data.")


def get_pool_and_container(key):
    """
    This function retrieves the pool and container names from "metadata.json" based on a given key.
    Args:
        key: The key to search for in the data.
    Returns:
        A tuple containing the pool name and container name if found, or None if not found.
    """
    try:
        with open("metadata.json", 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print("Error: 'metadata.json' file not found.")
        return None

    for item in data:
        if item["key"] == key:
            return item["pool"], item["container"]  # Directly return pool and container names
    return None

def read_key():
    try:
        key = input("Enter key to read: ")
        chunk_count = 0
        assembled_data = b""
        pool, container = get_pool_and_container(key)
        daos_cont = DCont(pool, container, None)
        try:
            daos_dict = daos_cont.get("pydaos_kvstore_dict")
        except:
            daos_dict = daos_cont.dict("pydaos_kvstore_dict")
        # Directory to store uploaded fi


        start_time = time.time()
        # Fetch all chunks using bget
        chunk_keys = {f"{key}chunk{i}": None for i in range(len(daos_dict))}

        chunks = daos_dict.bget(chunk_keys)

        for chunk_key, chunk in chunks.items():
            if chunk is not None:
                assembled_data += chunk
                chunk_count += 1

        end_time = time.time()
        retrieval_time = end_time - start_time

        if assembled_data:
            save_value_as_file(key, assembled_data)
            print(f"Value retrieved successfully. Total chunks: {chunk_count}. Time taken: {retrieval_time} seconds")
        else:
            print("Key not found.")
    except Exception as e:
        print(f"Error reading key: {e}")
def delete_key1():
    try:
        key = input("Enter key to delete: ")
        chunk_count = 0
        assembled_data = b""
        pool, container = get_pool_and_container(key)
        daos_cont = DCont(pool, container, None)
        try:
            daos_dict = daos_cont.get("pydaos_kvstore_dict")
        except:
            daos_dict = daos_cont.dict("pydaos_kvstore_dict")
        # Directory to store uploaded fi
        delete_by_key(key)

        
        chunk_keys = {f"{key}chunk{i}": None for i in range(len(daos_dict))}
        if chunk_keys:
            for chunk_key in chunk_keys:
                daos_dict.pop(chunk_key) 
        
            print("key delleted succesfully")
        else:
            print("key not deleted")
                          
    except Exception as e:
        print(f"Error delete  key: {e}")

#Function to print all keys



def delete_key():
    """Deletes a key and all its chunks from the DAOS container."""
    # Check if key exists
    key = input("Enter new key: ")
    pool, container = get_pool_and_container(key)
    daos_cont = DCont(pool, container, None)
    try:
        daos_dict = daos_cont.get("pydaos_kvstore_dict")
    except:
        daos_dict = daos_cont.dict("pydaos_kvstore_dict")

    if key in daos_dict:
        daos_dict.pop(key)# Remove metadata chunk

      # Iterate through keys and delete chunks
        for chunk_key in list(daos_dict.keys()):
            if chunk_key.startswith(f"{key}chunk"):
                daos_dict.pop(chunk_key)

        print(f"Key '{key}' and its chunks deleted (if they existed).")
    else:
        print("no key found")
def append_to_json_file(data, filename):
    try:
        # Load existing JSON data from file
        with open(filename, 'r') as file:
            existing_data = json.load(file)
    except FileNotFoundError:
        # If the file doesn't exist, initialize with an empty list
        existing_data = []

    # Append the new data to the existing list
    existing_data.append(data)

    # Write the updated data back to the file
    with open(filename, 'w') as file:
        json.dump(existing_data, file, indent=4)

def upload_file():
    daos_cont, pool, container = get_daos_container()

    # Create a DAOS dictionary or get it if it already exists
    try:
        daos_dict = daos_cont.get("pydaos_kvstore_dict")
    except:
        daos_dict = daos_cont.dict("pydaos_kvstore_dict")

    # Directory to store uploaded files

    key = input("Enter new key: ")
    file_path = input("Enter path to file: ")

    if os.path.exists(file_path):
        chunk_dict = {}
        try:
            metadata = {
                "pool": pool,
                "container": container,
                "key": key,
                "filename": os.path.basename(file_path),
                "size": os.path.getsize(file_path),
                "upload_time": time.time(),
            }
            append_to_json_file(metadata, "metadata.json")
            with open(file_path, "rb") as f:
                chunk_count = 0
                while True:
                    data = f.read(CHUNK_SIZE)
                    if not data:
                        break
                    chunk_key = f"{key}chunk{chunk_count}"
                    chunk_dict[chunk_key] = data
                    chunk_count += 1
            # Measure time only for the bput operation
            bput_start_time = time.time()
            daos_dict.bput(chunk_dict)
            bput_end_time = time.time()
            upload_time = bput_end_time - bput_start_time

            print(f"File uploaded in {chunk_count} chunks successfully. Time taken: {upload_time} seconds")
        except Exception as e:
            print(f"Error uploading file: {e}")
    else:
        print("File not found.")

while True:
    print("\nCommands:")
    print_help()
    cmd = input("Enter command (? for help): ")

    if cmd == "?":
        print_help()
    elif cmd == "r":
        read_key()
    elif cmd == "u":
        upload_file()
    elif cmd == "p":
        delete_key1() 
    elif cmd == "d":
        print_keys()
    elif  cmd == "q":
        break
    else:
        print("Invalid command. Enter '?' for help.")

print("Program ended.")
