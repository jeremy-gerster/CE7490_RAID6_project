import os
import time
from src.raid6.RAID6_bin import RAID6

current_dir = os.path.dirname(os.path.abspath(__file__))
main_dir = os.path.dirname(current_dir)
data_dir = os.path.join(main_dir, 'CE7490_RAID6_project/data')
raid_dirs = os.path.join(data_dir, 'test_dirs')

total_time = 0

# Check if there are existing RAID directories
existing_dirs = []
if os.path.exists(raid_dirs):
    existing_dirs = [d for d in os.listdir(raid_dirs) if os.path.isdir(os.path.join(raid_dirs, d))]
    existing_dirs.sort(key=lambda d: int(d.split('_')[-1]))
    

if existing_dirs:
    chunk_size = 0
    num_disk = 0

    load_choice = input("\nDo you want to load an existing RAID configuration? (y/n): ").strip().lower()

    # Load existing directories
    if load_choice == 'y':
        print("\nExisting RAID configurations found:")
        for i, d in enumerate(existing_dirs):
            print(f"{i + 1}. {d}")

        dir_choice = int(input("\nChoose a directory to load (enter number): ")) - 1
        existing_dir = os.path.join(raid_dirs, existing_dirs[dir_choice])
        # Initialize RAID6 instance
        raid = RAID6(chunk_size=chunk_size,
                     num_disk=num_disk,
                     dir=existing_dir,
                     existing_dir=existing_dir)
        
        option = int(input("Choose between these options: \n1. Add files \n2. Delete files \n3. Delete disks and recover\n"))

        # Add files
        if option == 1:
            start_time = time.time()
            
            files_dir = os.path.join(existing_dir, 'files')
            print(f"\nPlease upload your files (jpg, pdf, mp3) into the following directory:\n{files_dir}")
            input("\nPress Enter after you have uploaded the files...")

            uploaded_files = os.listdir(files_dir)

            # Collect file extensions and store them
            for file in uploaded_files:
                file_extension = file.split('.')[-1].lower()
                if file_extension not in ["jpg", "pdf", "mp3"]:
                    print(f"Unsupported format detected: {file}. Skipping this file.")

            raid.distribute_data(existing_dir)
            end_time = time.time()
            print(f"Time elapsed while adding files: {end_time-start_time}")

        # Delete files
        elif option == 2:
            start_time = time.time()

            files_dir = os.path.join(existing_dir, 'files')
            uploaded_files = os.listdir(files_dir)

            # Check for validity
            for file in uploaded_files:
                file_extension = file.split('.')[-1].lower()
                if file_extension not in ["jpg", "pdf", "mp3"]:
                    ValueError(f"Unsupported format detected: {file}.")

            existing_files = [d for d in uploaded_files if d != '.DS_Store']

            for i, d in enumerate(existing_files):
                print(f"{i + 1}. {d}")

            while True:
                dir_choice = input("\nChoose a file to delete (enter number). If done, type 'ok': ")

                if dir_choice.lower() == 'ok':
                    break

                try:
                    dir_choice = int(dir_choice) - 1
                    delete_file = os.path.join(files_dir, existing_files[dir_choice])
                    os.remove(delete_file)
                    print(f"Deleted file: {existing_files[dir_choice]}")

                except ValueError:
                    print("Invalid input")

                except IndexError:
                    print("Invalid number")

            raid.distribute_data(existing_dir)  # Re-distribute data after deletion
            end_time = time.time()
            print(f"Time elapsed while deleting files: {end_time-start_time}")

        # Delete disks and recover
        elif option == 3:

            raid.distribute_data(existing_dir)

    else:
        existing_dir = None
else:
    print("\nNo existing RAID configurations found.")
    existing_dir = None

# Create new RAID setup
if not existing_dir:
    # Find the highest numbered existing "raid_system_X" directory
    raid_system_dirs = [d for d in existing_dirs if d.startswith('raid_system_')]
    if raid_system_dirs:
        # Extract numbers from existing directory names and find the highest
        max_num = max([int(d.split('_')[-1]) for d in raid_system_dirs])
        next_num = max_num + 1
    else:
        next_num = 1

    new_dir_name = f'raid_system_{next_num}'
    dir = os.path.join(raid_dirs, new_dir_name)
    os.makedirs(dir)
    files_dir = os.path.join(dir, 'files')
    os.makedirs(files_dir)
    disks_dir = os.path.join(dir, 'disks')
    os.makedirs(disks_dir)
    pre_dir = os.path.join(dir, 'Initial_distributed_files')
    os.makedirs(pre_dir)
    rec_dir = os.path.join(dir, 'Recovered_files')
    os.makedirs(rec_dir)
    reload_dir = os.path.join(dir, 'Reloaded_Initial_distributed_files')
    os.makedirs(reload_dir)

    print(f"Please upload your files (jpg, pdf, mp3) into the following directory: {files_dir}")
    input("Press Enter after you have uploaded the files...")

    uploaded_files = os.listdir(files_dir)

    file_list = []
    # Collect file extensions
    for file in uploaded_files:
        file_extension = file.split('.')[-1].lower()
        if file_extension in ["jpg", "pdf", "mp3"]:
            file_list.append(file)
        else:
            print(f"Unsupported format: {file}. Skipping this file.")

    if not file_list:
        raise ValueError("No valid files uploaded")

    while True:
        num_disk = int(input("\nChoose number of disks (up to 7): "))
        if num_disk <= 7:
            break
    chunk_size = int(input("\nChoose chunk size: "))
    is_local_input = input("\nDo you want to store the data locally? (y/n) ")
    is_local = True
    if is_local_input == "n":
        is_local = False

    # Initialize RAID6
    raid = RAID6(chunk_size=chunk_size,
                 num_disk=num_disk,
                 is_local=is_local,
                 dir=dir,
                 existing_dir=None)
    raid.uploaded_files = uploaded_files

    start_time = time.time()
    raid.distribute_data(existing_dir)
    end_time = time.time()
    total_time += end_time - start_time

# Choose disks to delete
deleted_disks = [int(x) for x in input("\nChoose 1 or 2 disks to delete (split by space): ").split()]
start_time = time.time()
raid.delete_disk(deleted_disks)

# Rebuild lost data
raid.rebuild_data(deleted_disks)

end_time = time.time()
total_time += end_time - start_time
print(f"Total time elapsed: {total_time}")