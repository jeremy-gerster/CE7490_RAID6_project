from typing import Union
from pydantic import BaseModel
import gridfs
from pymongo import MongoClient
from bson import ObjectId
from fastapi.responses import StreamingResponse
from io import BytesIO
import base64

from fastapi import FastAPI, HTTPException


app = FastAPI()

# Request model
class ChunkData(BaseModel):
    disk_number: int
    chunk: bytes


class ChunkRequest(BaseModel):
    disk_number: int
    file_id: str


class DeleteRequest(BaseModel):
    disk_number: int
    file_id: str


@app.post("/upload")
def upload_to_disk(chunk_data: ChunkData):
    """Uploads file chunks to specified disk"""
    try:
        # Extract data from the request
        disk_number = chunk_data.disk_number
        encoded_chunk = chunk_data.chunk

        chunk = base64.b64decode(encoded_chunk)

        client = MongoClient(f'mongodb://localhost:2700{disk_number+1}')
        db = client.chunks
        fs = gridfs.GridFS(db)

        # Store the binary data in GridFS with custom metadata
        file_id = fs.put(chunk)

        return {"message": "Chunk stored successfully", "file_id": str(file_id)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data")
def get_disk_data(chunk_request: ChunkRequest):
    """Gets data from specified disk"""
    try:
        # Convert file_id from string to ObjectId
        disk_number = chunk_request.disk_number
        file_id = ObjectId(chunk_request.file_id)

        client = MongoClient(f'mongodb://localhost:2700{disk_number+1}')
        db = client.chunks
        fs = gridfs.GridFS(db)
        
        # Retrieve the file from GridFS by its ObjectId
        stored_file = fs.get(file_id)
        
        # Read the binary data from the stored file
        file_data = stored_file.read()
        
        # Use StreamingResponse to send the file data to the client
        return StreamingResponse(BytesIO(file_data), media_type="application/octet-stream", headers={"Content-Disposition": f"attachment; filename={stored_file.filename}"})

    except gridfs.errors.NoFile:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset")
def delete_file(delete_request: DeleteRequest):
    """Resets the specified disk and deletes all files"""
    try:

        disk_number = delete_request.disk_number
        file_id = ObjectId(delete_request.file_id)
        # Connect to the corresponding MongoDB instance
        client = MongoClient(f'mongodb://localhost:2700{disk_number}')
        db = client.chunks
        fs = gridfs.GridFS(db)
        fs.delete(file_id)
        # Drop the entire GridFS collection to delete all files

        return {"message": f"File {file_id} on disk {disk_number} has been deleted"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))