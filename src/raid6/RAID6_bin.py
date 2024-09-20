import os
import json
from src.raid6.GaloisField import GF
import src.cloud_implementation.api_client as client

class RAID6:
    def __init__(self, chunk_size=0, num_disk=0, is_local=True, dir=None, existing_dir=None):
        """Initializes the RAID 6 environment or loads an existing configuration."""
        self.chunk_size = chunk_size
        self.num_disk = num_disk
        self.dir = dir
        self.existing_dir = existing_dir
        self.gf = GF()
        self.test_directory = dir
        self.matrix = []
        self.disk_data = None
        self.P_loc = []
        self.Q_loc = []
        self.total_stripes = 0
        self.file_metadata = {}
        self.old_files = []
        self.disks_dir = os.path.join(self.dir, 'disks')
        self.num_data_disk = num_disk - 2
        self.files_dir = os.path.join(self.dir, 'files')
        self.disks_dir = os.path.join(self.dir, 'disks')
        self.file_dict = {i: "" for i in range(num_disk)}
        self.is_local = is_local
        print(self.file_dict)
        if existing_dir and os.path.exists(existing_dir):
            self._load_metadata(existing_dir)
        elif chunk_size is None or num_disk is None:
            raise ValueError("chunk_size and num_disk are not provided")

    def _load_metadata(self, existing_dir):
        """Loads RAID system configuration from metadata."""
        metadata_file = os.path.join(existing_dir, 'metadata.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                raid = json.load(f)
                self.chunk_size = raid['chunk_size']
                self.num_disk = raid['num_disk']
                self.num_data_disk = self.num_disk - 2
                self.test_directory = existing_dir
                self.file_metadata = raid.get('file_metadata', {})
                self.total_stripes = raid.get('total_stripes', 0)
                self.old_files = raid.get('old_files', [])
                self.file_dict = raid.get('file_ids', {})
                self.is_local = raid.get('is_local', True)
        else:
            raise FileNotFoundError(f"No metadata found in {existing_dir}")


    def save_metadata(self):
        """Saves the entire RAID structure to a metadata file."""
        metadata = {
            'chunk_size': self.chunk_size,
            'num_disk': self.num_disk,
            'file_metadata': self.file_metadata,
            'total_stripes': self.total_stripes,
            'old_files': self.old_files,
            'file_ids': self.file_dict,
            'is_local': self.is_local
        }
        metadata_file = os.path.join(self.test_directory, 'metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)


    def read_data(self, filename, mode='rb'):
        with open(filename, mode) as f:
            return list(f.read())

    
    def update_file_metadata(self):
        current_stripe = 0
        for file, metadata in self.file_metadata.items():
            num_stripes = metadata['num_stripes']
            self.file_metadata[file]['start_stripe'] = current_stripe
            self.file_metadata[file]['end_stripe'] = current_stripe + num_stripes - 1
            current_stripe += num_stripes
        return self.file_metadata

    
    def compute_parity(self, matrix):
        """Computes the P and Q parity for the distributed data in the matrix."""
        # Initialize parity arrays
        num_stripes = len(matrix)
        P_parity = [[0] * self.chunk_size for _ in range(num_stripes)]
        Q_parity = [[0] * self.chunk_size for _ in range(num_stripes)]

        # Compute P and Q parity for each stripe
        for stripe_index in range(num_stripes):
            for disk_index in range(self.num_disk):
                # Skip the P and Q locations during data processing
                if (stripe_index, disk_index) not in self.P_loc and (stripe_index, disk_index) not in self.Q_loc:
                    data_val = matrix[stripe_index][disk_index]
                    if data_val is not None and len(data_val) == self.chunk_size:
                        for i in range(self.chunk_size):
                            P_parity[stripe_index][i] ^= data_val[i]
                            Q_parity[stripe_index][i] ^= self.gf.mul(data_val[i], self.gf.exp(disk_index))

        return P_parity, Q_parity


    def recalculate_parity_locations(self):
        """Recalculate P and Q locations after the matrix is compacted due to file deletion."""
        P_loc = []
        Q_loc = []

        for filename, metadata in self.file_metadata.items():
            start_stripe = metadata['start_stripe']
            end_stripe = metadata['end_stripe']
            num_stripes = metadata['num_stripes']

            # Recalculate parity locations for each stripe
            for stripe_index in range(self.total_stripes):
                p_disk = (self.num_disk - 2 - stripe_index) % self.num_disk
                q_disk = (self.num_disk - 1 - stripe_index) % self.num_disk
                P_loc.append((start_stripe + stripe_index, p_disk))
                Q_loc.append((start_stripe + stripe_index, q_disk))

        return P_loc, Q_loc


    def load_existing_data(self):
        """Reconstructs data from existing RAID configuration for multiple files, supporting P and Q parity recomputation."""
    
        # Rebuild matrix from disk files
        reloaded_matrix = [[None for _ in range(self.num_disk)] for _ in range(self.total_stripes)]
        for disk_index in range(self.num_disk):
            if self.is_local:
                disk_file = os.path.join(self.disks_dir, f'disk_{disk_index}')
                if os.path.exists(disk_file):
                    with open(disk_file, 'rb') as f:
                        disk_content = f.read()
            else:
                file_id = self.file_dict[str(disk_index)]
                disk_content = client.get_disk_data(disk_index, file_id)
            
            if disk_content:
            # Split disk data into chunks
                disk_content = list(disk_content)
                disk_chunks = [disk_content[i:i + self.chunk_size] for i in range(0, len(disk_content), self.chunk_size)]

                # Rebuild matrix for this disk
                for stripe_index in range(self.total_stripes):
                    if stripe_index < len(disk_chunks):
                        reloaded_matrix[stripe_index][disk_index] = disk_chunks[stripe_index]
                    else:
                        reloaded_matrix[stripe_index][disk_index] = [0] * self.chunk_size  # Pad remaining stripes

        # Recalculate P_loc and Q_loc based on the rebuild
        reloaded_P_loc = []
        reloaded_Q_loc = []
        current_stripe_index = 0
    
        # Process each file in the file_metadata
        for filename, metadata in self.file_metadata.items():

    
            P_loc_file = []
            Q_loc_file = []
    
            # Determine P and Q parities positions in matrix
            num_stripes = metadata['num_stripes']
            for stripe_index in range(num_stripes):
                p_disk = (self.num_disk - 2 - stripe_index) % self.num_disk
                q_disk = (self.num_disk - 1 - stripe_index) % self.num_disk
                P_loc_file.append((current_stripe_index + stripe_index, p_disk))  
                Q_loc_file.append((current_stripe_index + stripe_index, q_disk))  
    
            # Append file-specific P_loc and Q_loc to the global lists
            reloaded_P_loc.extend(P_loc_file)
            reloaded_Q_loc.extend(Q_loc_file)
    
            current_stripe_index += num_stripes
    
        
        # Initialize lists to accumulate data for each disk
        disk_data = [bytearray() for _ in range(self.num_disk)]
        
        # Save data for each disk
        for i in range(self.num_disk):
            for stripe_index in range(len(reloaded_matrix)):
                # Always extend the disk data, even if the stripe is zero-padded
                disk_data[i].extend(reloaded_matrix[stripe_index][i])
    
        # Save individual pre files for each format
        for filename, metadata in self.file_metadata.items():
            start_stripe = metadata['start_stripe']
            end_stripe = metadata['end_stripe']
            pre_data = bytearray()
    
            # Accumulate data for the file from corresponding stripes
            for stripe_index in range(start_stripe, end_stripe + 1):
                for disk_index in range(self.num_disk):
                    if (stripe_index, disk_index) not in reloaded_P_loc and (stripe_index, disk_index) not in reloaded_Q_loc:
                        if reloaded_matrix[stripe_index][disk_index] != [0] * self.chunk_size:
                            pre_data.extend(reloaded_matrix[stripe_index][disk_index])
    
            # Save accumulated non-parity data as pre.<format>
            reload_dir = os.path.join(self.dir, 'Reloaded_Initial_distributed_files')
            pre_filename = f'pre_reloaded.{filename}'
            print(f'created {pre_filename}')
            with open(os.path.join(reload_dir, pre_filename), 'wb') as f:
                f.write(pre_data)

                
        self.matrix = reloaded_matrix
        self.P_loc = reloaded_P_loc
        self.Q_loc = reloaded_Q_loc
        self.disk_data = disk_data

        return reloaded_matrix


    def distribute_data(self, existing_dir=None):
        """Distributes data across the data disks for multiple formats, creating separate matrices for each file and then merging them."""

        # Initialize lists to accumulate data for each disk
        disk_data = [bytearray() for _ in range(self.num_disk)]

        files = os.listdir(self.files_dir)

        new_files_added = [item for item in files if item not in self.old_files]
        new_files_deleted = [item for item in self.old_files if item not in files]
        
        if existing_dir:
            
            self.load_existing_data()
            all_matrices = self.matrix
            
            #if files added
            if len(new_files_added) > 0:
                print('adding new files')
                current_stripe_index = len(all_matrices)
            #if files deleted
            elif len(new_files_deleted) > 0:
                print('Deleting files')
                print(f'total: {self.total_stripes}')
                for deleted_file in new_files_deleted:
                    # Find the stripe range of the deleted file from metadata
                    if deleted_file in self.file_metadata:
                        start_stripe = self.file_metadata[deleted_file]['start_stripe']
                        end_stripe = self.file_metadata[deleted_file]['end_stripe']
                        num_stripes = self.file_metadata[deleted_file]['num_stripes']

                        # Remove corresponding stripes from the matrix
                        for stripe_index in range(start_stripe, end_stripe + 1):
                            for disk_index in range(self.num_disk):
                                all_matrices[stripe_index][disk_index] = None

                        # Remove the deleted file's metadata
                        del self.file_metadata[deleted_file]

                        self.total_stripes -= num_stripes

                # After deletion, recompute the matrix by removing the empty rows
                all_matrices = [row for row in all_matrices if any(chunk is not None for chunk in row)]

                # Reset stripe index and recalculate P/Q locations
                current_stripe_index = len(all_matrices)
                self.old_files = files
                self.matrix = all_matrices
                self.update_file_metadata()
                self.P_loc, self.Q_loc = self.recalculate_parity_locations()
                # Save data for each disk
                for i in range(self.num_disk):
                    for stripe_index in range(len(all_matrices)):
                        disk_data[i].extend(all_matrices[stripe_index][i])

                    if self.is_local:
                        with open(os.path.join(self.disks_dir, f'disk_{i}'), 'wb') as f:
                            f.write(disk_data[i])
                    else:
                        file_id = client.upload_to_disk(i, disk_data[i])
                        self.file_dict[str(i)] = file_id
                return

            # If files are unchanged
            else:
                print('no new files')
                self.old_files = files
                return
        else:
            all_matrices = []
            current_stripe_index = 0
            self.P_loc = []
            self.Q_loc = []


        # Process each file in the 'files' directory
        for file_index, filename in enumerate(new_files_added):
            filepath = os.path.join(self.files_dir, filename)
            print(f'adding {new_files_added}')
            file_extension = filename.split('.')[-1].lower()

            # Only process supported formats
            if file_extension not in ["jpg", "pdf", "mp3"]:
                print(f"Skipping unsupported file format: {filename}")
                continue

            # Read the original data for this file
            data = self.read_data(filepath)
            chunks = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
            max_chunks = len(chunks)

            # Calculate the number of stripes (rows in the matrix)
            num_stripes = (max_chunks + self.num_data_disk - 1) // self.num_data_disk
            self.total_stripes += num_stripes

            # Save file metadata with start, end, and number of stripes
            self.file_metadata[filename] = {
                'start_stripe': current_stripe_index,
                'end_stripe': current_stripe_index + num_stripes - 1,
                'num_stripes': num_stripes
            }
 
            file_matrix = [[bytearray([0] * self.chunk_size) for _ in range(self.num_disk)] for _ in range(num_stripes)]


            P_loc_file = []
            Q_loc_file = []

            # Determine P and Q parities positions in the matrix for this file
            for stripe_index in range(num_stripes):
                p_disk = (self.num_disk - 2 - stripe_index) % self.num_disk
                q_disk = (self.num_disk - 1 - stripe_index) % self.num_disk
                P_loc_file.append((stripe_index, p_disk))
                Q_loc_file.append((stripe_index, q_disk))

            # Fill in the data chunks row by row for this file
            chunk_index = 0
            for stripe_index in range(num_stripes):
                for disk_index in range(self.num_disk):
                    if (stripe_index, disk_index) in P_loc_file or (stripe_index, disk_index) in Q_loc_file:
                        continue
                    if chunk_index < max_chunks:
                        file_matrix[stripe_index][disk_index] = chunks[chunk_index]
                        while len(file_matrix[stripe_index][disk_index]) < self.chunk_size:
                            file_matrix[stripe_index][disk_index].append(0)
                        chunk_index += 1
                    else:
                        # Pad with zeros if no data left
                        file_matrix[stripe_index][disk_index] = [0] * self.chunk_size

            # Store file's P_loc and Q_loc globally
            self.P_loc.extend([(current_stripe_index + stripe_index, disk) for stripe_index, disk in P_loc_file])
            self.Q_loc.extend([(current_stripe_index + stripe_index, disk) for stripe_index, disk in Q_loc_file])

            
            # Append this file's matrix to the overall matrix
            all_matrices.extend(file_matrix)

            # Update the stripe index for the next file
            current_stripe_index += num_stripes

        # Compute P and Q parity after merging the matrices
        P, Q = self.compute_parity(all_matrices)

        # Assign computed P and Q parities to their respective locations in the global matrix
        for stripe_index in range(self.total_stripes):
            for disk_index in range(self.num_disk):
                if (stripe_index, disk_index) in self.P_loc:
                    all_matrices[stripe_index][disk_index] = P[stripe_index]
                if (stripe_index, disk_index) in self.Q_loc:
                    all_matrices[stripe_index][disk_index] = Q[stripe_index]



        # Save individual pre files for each format based on start and end stripes
        for filename, metadata in self.file_metadata.items():
            start_stripe = metadata['start_stripe']
            end_stripe = metadata['end_stripe']
            pre_data = bytearray()

            for stripe_index in range(start_stripe, end_stripe + 1):
                for disk_index in range(self.num_disk):
                    if (stripe_index, disk_index) not in self.P_loc and (stripe_index, disk_index) not in self.Q_loc:
                        if all_matrices[stripe_index][disk_index] != [0] * self.chunk_size:
                            pre_data.extend(all_matrices[stripe_index][disk_index])

            
            pre_filename = f'pre_initial_{filename}'
            print(f'created {pre_filename}')
            init_dir = os.path.join(self.dir, 'Initial_distributed_files')
            with open(os.path.join(init_dir, pre_filename), 'wb') as f:
                f.write(pre_data)
        

        self.matrix = all_matrices

        

        self.old_files = files

        # Save data for each disk
        for i in range(self.num_disk):
            for stripe_index in range(len(all_matrices)):
                disk_data[i].extend(all_matrices[stripe_index][i])

            if self.is_local:
                with open(os.path.join(self.disks_dir, f'disk_{i}'), 'wb') as f:
                    f.write(disk_data[i])
            else:
                file_id = client.upload_to_disk(i, disk_data[i])
                self.file_dict[str(i)] = file_id


        self.disk_data = disk_data

        return all_matrices, disk_data

    def delete_disk(self, deleted_disks):
        """Delete specified disks."""
        for i in deleted_disks:
            if self.is_local:
                os.remove(os.path.join(self.disks_dir, f'disk_{i}'))
            else:
                file_id = self.file_dict[str(i)]
                client.delete_file(i, file_id)
            print(f"Disk {i} was deleted")

        self.matrix = []


    def rebuild_data(self, deleted_disks):
        """Rebuilds data from the available disks using the matrix to XOR the correct chunks together and writes the reconstructed data back to disk."""

        # Rebuild the matrix from disk files directly
        self.matrix = [[None for _ in range(self.num_disk)] for _ in range(self.total_stripes)]
    
        for disk_index in range(self.num_disk):

            if self.is_local:
                disk_file = os.path.join(self.disks_dir, f'disk_{disk_index}')
                if os.path.exists(disk_file):
                    with open(disk_file, 'rb') as f:
                        disk_content = list(f.read())
            else:
                file_id = self.file_dict[str(disk_index)]
                disk_content = client.get_disk_data(disk_index, file_id)
                
            if disk_content:
                # Split the disk data into chunks of size chunk_size
                disk_content = list(disk_content)
                disk_chunks = [disk_content[i:i + self.chunk_size] for i in range(0, len(disk_content), self.chunk_size)]
            
                # Rebuild the matrix for this disk
                for stripe_index in range(self.total_stripes):
                    if stripe_index < len(disk_chunks):
                        self.matrix[stripe_index][disk_index] = disk_chunks[stripe_index]
                    else:
                        # Pad remaining stripes
                        self.matrix[stripe_index][disk_index] = [0] * self.chunk_size
            else:
                for stripe_index in range(self.total_stripes):
                    for disk_index in range(self.num_disk):
                        if disk_index in deleted_disks:
                            self.matrix[stripe_index][disk_index] = [0] * self.chunk_size


        # Reconstruct missing data
        for stripe_index in range(self.total_stripes):
            # Initialize the sums for this stripe, as arrays to handle each byte in the chunk
            p_sum = [0] * self.chunk_size
            q_sum = [0] * self.chunk_size
            missing_disk1 = deleted_disks[0]
            missing_disk2 = deleted_disks[1] if len(deleted_disks) == 2 else None
            # Retrieve the P and Q parity locations
            p_disk = self.P_loc[stripe_index][1]
            q_disk = self.Q_loc[stripe_index][1]

            for disk_index in range(self.num_disk):
                data_val = self.matrix[stripe_index][disk_index]

                # Accumulate known data values into p_sum and q_sum
                if disk_index != missing_disk1 and (missing_disk2 is None or disk_index != missing_disk2):
                    if disk_index != p_disk and disk_index != q_disk:
                        for i in range(self.chunk_size):
                            p_sum[i] ^= data_val[i]
                            q_sum[i] ^= self.gf.mul(data_val[i], self.gf.exp(disk_index))

            # If one disk is missing
            if len(deleted_disks) == 1:
                # Add the P parity value to p_sum for each byte
                if deleted_disks[0] != p_disk:
                    for i in range(self.chunk_size):
                        p_sum[i] ^= self.matrix[stripe_index][p_disk][i]

                if deleted_disks[0] != q_disk:
                    # Recover the missing data using p_sum if Q parity was not deleted
                    self.matrix[stripe_index][missing_disk1] = p_sum[:]
                else:
                    # Recover the missing data using q_sum if Q parity was deleted
                    self.matrix[stripe_index][missing_disk1] = q_sum[:]

            # If two disks are missing
            elif len(deleted_disks) == 2:
                # Add the P and Q parity values for each byte
                if p_disk not in deleted_disks:
                    for i in range(self.chunk_size):
                        p_sum[i] ^= self.matrix[stripe_index][p_disk][i]
                if q_disk not in deleted_disks:
                    for i in range(self.chunk_size):
                        q_sum[i] ^= self.matrix[stripe_index][q_disk][i]

                dx = [0] * self.chunk_size
                dy = [0] * self.chunk_size

                for i in range(self.chunk_size):
                    # If both P and Q are missing, just use p_sum and q_sum
                    if p_disk in deleted_disks and q_disk in deleted_disks:
                        if missing_disk1 == p_disk:
                            dx[i] = p_sum[i]
                            dy[i] = q_sum[i]
                        else:
                            dx[i] = q_sum[i]
                            dy[i] = p_sum[i]
                    # If P is missing, reconstruct using q_sum
                    elif p_disk in deleted_disks:
                        if missing_disk1 == p_disk:
                            dy[i] = self.gf.div(q_sum[i], self.gf.exp(missing_disk2))
                            p_sum[i] ^= dy[i]
                            dx[i] = p_sum[i]
                        if missing_disk2 == p_disk:
                            dx[i] = self.gf.div(q_sum[i], self.gf.exp(missing_disk1))
                            p_sum[i] ^= dx[i]
                            dy[i] = p_sum[i]
                    # If Q is missing, reconstruct using p_sum
                    elif q_disk in deleted_disks:
                        if missing_disk1 == q_disk:
                            dy[i] = p_sum[i]
                            q_sum[i] ^= self.gf.mul(dy[i], self.gf.exp(missing_disk2))
                            dx[i] = q_sum[i]
                        else:
                            dx[i] = p_sum[i]
                            q_sum[i] ^= self.gf.mul(dx[i], self.gf.exp(missing_disk2))
                            dy[i] = q_sum[i]
                    else:
                        # General case where both missing disks are data disks
                        dy[i] = self.gf.div(q_sum[i] ^ self.gf.mul(p_sum[i], self.gf.exp(missing_disk1)), self.gf.add(self.gf.exp(missing_disk1), self.gf.exp(missing_disk2)))
                        dx[i] = p_sum[i] ^ dy[i]

                # Store the reconstructed data in the matrix
                self.matrix[stripe_index][missing_disk1] = dx
                self.matrix[stripe_index][missing_disk2] = dy

        disk_data = [bytearray() for _ in range(self.num_disk)]
        
        for filename, metadata in self.file_metadata.items():
            start_stripe = metadata['start_stripe']
            end_stripe = metadata['end_stripe']
            recovered_data = bytearray()

            for stripe_index in range(start_stripe, end_stripe + 1):
                for disk_index in range(self.num_disk):
                    if (stripe_index, disk_index) not in self.P_loc and (stripe_index, disk_index) not in self.Q_loc:
                        if self.matrix[stripe_index][disk_index] != [0] * self.chunk_size:
                            recovered_data.extend(self.matrix[stripe_index][disk_index])


            recovered_filename = f'recovered_{filename}'
            print(f'recovered {filename}')
            rec_dir = os.path.join(self.dir, 'Recovered_files')
            with open(os.path.join(rec_dir, recovered_filename), 'wb') as f:
                f.write(recovered_data)

        # Save data for each disk
        for i in range(self.num_disk):
            for stripe_index in range(len(self.matrix)):
                disk_data[i].extend(self.matrix[stripe_index][i])

            if self.is_local:
                with open(os.path.join(self.disks_dir, f'disk_{i}'), 'wb') as f:
                    f.write(disk_data[i])
            else:
                file_id = client.upload_to_disk(i, disk_data[i])
                self.file_dict[str(i)] = file_id
        print(f"Data reconstruction successful for disks {deleted_disks}.")
        self.save_metadata()
