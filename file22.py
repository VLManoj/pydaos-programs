import os
import time
from pydaos import DCont, DDict

# Create a DAOS container
daos_cont = DCont("pydaos1", "kvstore", None)

# Create a DAOS dictionary or get it if it already exists
try:
    daos_dict = daos_cont.get("pydaos_kvstore_dict")
except:
    daos_dict = daos_cont.dict("pydaos_kvstore_dict")

# Directory to store uploaded files
upload_dir = "uploads"
os.makedirs(upload_dir, exist_ok=True)

# Function to print help
def print_help():
    print("?\t- Print this help")
    print("r\t- Read a key")
    print("u\t- Upload file for a new key")
    print("ub\t- Upload files for new keys in bulk")
    print("d\t- Delete key")
    print("p\t- Display keys")
    print("q\t- Quit")


# Function to read a key with time measurement
def read_key():
    try:
        key = input("Enter key to read: ")
        
        start_time = time.time()  # Start time measurement
        value = daos_dict[key]
        end_time = time.time()    # End time measurement
        
        retrieval_time = end_time - start_time
        if value:
            save_value_as_file(key, value)
            print(f"Value retrieved successfully. Time taken: {retrieval_time} seconds")
        else:
            print("Key not found.")

    except KeyError:
        print("\tError! Key not found")

# Function to save value as a file
def save_value_as_file(key, value):
    filename = os.path.join(upload_dir, f"{key}.dat")
    with open(filename, "wb") as f:
        f.write(value)
    print(f"Value saved as file: {filename}")

#Function to print all keys
def print_keys():
    unique_keys = set()
    for key in daos_dict:
        key_prefix = key.split("chunk")[0]
        unique_keys.add(key_prefix)
    for key in unique_keys:
        print(key)

# Function to upload file for a new key with time measurement
def upload_file():
    key = input("Enter new key: ")
    file_path = input("Enter path to file: ")
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            value = f.read()

        start_time = time.time()  # Start time measurement
        daos_dict.put(key, value)
        end_time = time.time()    # End time measurement

        upload_time = end_time - start_time
        print(f"File uploaded successfully. Time taken: {upload_time} seconds")
    else:
        print("File not found.")

# Function to delete a key
def delete_key():
    key = input("Enter key to delete: ")
    if daos_dict.pop(key):
        print("Key deleted successfully.")
    else:
        print("Key not found.")

# Function to upload files for new keys in bulk with time measurement
def upload_bulk():
    num_keys = int(input("Enter the number of keys to insert: "))

    keys = []
    file_paths = []

    # Prompt user to enter keys and file paths
    for i in range(num_keys):
        key = input(f"Enter key {i + 1}: ")
        file_path = input(f"Enter path to file for key {key}: ")

        keys.append(key)
        file_paths.append(file_path)

    # Measure time for each file upload
    total_upload_time = 0
    
    for key, file_path in zip(keys, file_paths):

        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                value = f.read()

            start_time = time.time()

            daos_dict.put(key, value)
             
            end_time = time.time()
            upload_time = end_time - start_time
            total_upload_time += upload_time

            print(f"File uploaded for key {key}. Time taken: {upload_time} seconds")
        else:
            print(f"File not found for key {key}.")

    print(f"Total time taken for all uploads: {total_upload_time} seconds")

    # Prompt user to enter ke
# Main loop
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
    elif cmd == "d":
        delete_key()
    elif cmd == "ub":
        upload_bulk()
    elif cmd=='p':
        print_keys()
    elif cmd == "q":
        break
    else:
        print("Invalid command. Enter '?' for help.")

print("Program ended.")
