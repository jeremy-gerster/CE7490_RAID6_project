import requests
import base64

# URL of the FastAPI server
BASE_URL = "http://35.198.208.178:8000"

def upload_to_disk(disk_number, chunk_data):
    """Uploads a binary chunk to the specified disk number"""
    # Encode the binary chunk to base64
    encoded_chunk = base64.b64encode(chunk_data).decode('utf-8')
    
    # Prepare the data payload
    body = {
        "disk_number": disk_number,
        "chunk": encoded_chunk
    }
    
    # Send the POST request
    response = requests.post(f"{BASE_URL}/upload", json=body)
    
    # Check the response status
    if response.status_code == 200:
        response_data = response.json()
        file_id = response_data.get("file_id")
        print("Upload successful. File ID:", file_id)
        return file_id
    else:
        print("Upload failed:", response.json())

def get_disk_data(disk_number, file_id):
    """Retrieves a binary chunk from the specified disk number"""
    # Prepare the data payload
    body = {
        "disk_number": disk_number,
        "file_id": file_id
    }
    
    # Send the GET request
    response = requests.get(f"{BASE_URL}/data", json=body)
    
    # Check the response status
    if response.status_code == 200:
        # Save the binary data to a file
        print("File retrieved successfully and saved as 'retrieved_file'")
        return response.content
    else:
        print("Failed to retrieve file:", response.json())
        return None


def delete_file(disk_number, file_id):
    """Resets the specified disk by deleting all files"""

    body = {
        "disk_number": disk_number,
        "file_id": file_id
    }
    # Send the POST request with the disk number as a query parameter
    response = requests.post(f"{BASE_URL}/reset", json=body)
    
    # Check the response status
    if response.status_code == 200:
        print(response.json()["message"])
    else:
        print("Failed to reset disk:", response.json())


# Example usage:

# DISK = 3

# # Prepare binary data for upload
# binary_data = b'This is a test binary chunk to be uploaded to disk.'

# # Upload the binary data to disk number 1
# file_id = upload_to_disk(DISK, binary_data)

# # Retrieve the file data using the disk number and file_id returned from the upload
# # file_id = "YOUR_FILE_ID_HERE"  # Replace with the actual file_id returned from the upload
# get_disk_data(DISK, file_id)

# delete_file(DISK, file_id)