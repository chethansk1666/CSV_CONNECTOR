import os
import logging
from typing import Dict, Any
from fastapi import UploadFile, HTTPException,Form # type: ignore
from pymongo import MongoClient # type: ignore
from motor.motor_asyncio import AsyncIOMotorClient,AsyncIOMotorCollection
import pandas as pd
import tempfile
from tempfile import NamedTemporaryFile
from models.block import Block
from utils.utils import  get_airbyte_schema,remove_object_id

# MongoDB connection settings
MONGO_URI = "mongodb+srv://chethansk1666:U4ch4fOfl0JBqUoL@cluster1.dgiu2bg.mongodb.net/"
DATABASE_NAME = "canvas_db"
BLOCKS_COLLECTION = "blocks"
CANVAS_COLLECTION = "canvases"


# Temporary Storage directory for upload iles
TEMP_STORAGE_DIR = tempfile.gettempdir()

# Ensure the existence of the temporary directory
if not os.path.exists(TEMP_STORAGE_DIR):
    os.makedirs(TEMP_STORAGE_DIR)
            

try:
    # Connect to MongoDB
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DATABASE_NAME]
    blocks_collection = db[BLOCKS_COLLECTION]
    canvas_collection = db[CANVAS_COLLECTION]

    # Log a success message
    logging.info("Successfully connected to MongoDB!.")
except Exception as e:
    # Log any errors that occur during the connection attempt
    logging.error(f"Error connecting to MongoDB: {str(e)}")





#---------------------------------------------------------------------------------------------------------------------#
async def create_canvas(canvas_name: str,canvas_uid : str =Form(None)):
    try:
        
        # Insert the canvas into MongoDB
        canvas_dict = {"canvas_name": canvas_name, "canvas_uid": canvas_uid}
        canvas = await canvas_collection.insert_one(canvas_dict)
        
        # Retrieve the inserted document without _id
        new_canvas =await canvas_collection.find_one({"_id": canvas.inserted_id}, {"_id": 0})
        if new_canvas:
            # Check if '_id' key exists before removing it
            if '_id' in new_canvas:
                new_canvas.pop('_id')  # Remove _id from the dictionary
            print(new_canvas)
            logging.info("Canvas created: %s", new_canvas)
        return new_canvas
        
    except Exception as e:
        logging.error("Failed to create canvas: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to create canvas: {str(e)}")

async def list_canvas():
     canvases = []
     # Retrieve all canvases from MongoDB
     async for canvas in canvas_collection.find({}, {"_id": 0}):  # Exclude _id field
        canvas_dict = {
                "canvas_name": canvas["canvas_name"],
                "canvas_uid": canvas["canvas_uid"]
            }
        canvases.append(canvas_dict)
     return canvases


'''async def create_block(canvas_uid: str, sample_file: UploadFile,header_row: bool = False, separator: str = ",") -> Dict[str, Any]:
    """
    Create a block in the database.

    Args:
        canvas_uid (str): The canvas UID.
        sample_file (UploadFile): The uploaded CSV file.
        separator (str, optional): The CSV separator. Defaults to ",".

    Returns:
        Dict[str, Any]: The created block details.
    """
    logging.info("Received request to create a block")

    # Check if the file is CSV and size is less than 5MB
    if not sample_file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file is not a CSV.")

    file_size = os.stat(sample_file.filename).st_size
    if file_size > 5 * 1024 * 1024:  # 5MB
        raise HTTPException(status_code=400, detail="File size exceeds 5MB limit.")

    # Get the full path to the uploaded file
    uploaded_file_path = sample_file.filename
    
    # Generate a unique filename
    unique_filename = f"{canvas_uid}_{os.path.basename(uploaded_file_path)}"
    
    # Store uploaded file in temporary storage
    file_path = os.path.join(TEMP_STORAGE_DIR, unique_filename)
    print(file_path)
    with open(file_path, "wb") as f:
        f.write(sample_file.file.read())

    logging.info("Uploaded file stored at: %s", file_path)

    # Retrieve the schema from Airbyte
    try:
        airbyte_schema = get_airbyte_schema(file_path, separator)
        logging.info("Schema retrieved from Airbyte: %s", airbyte_schema)
    except Exception as e:
        logging.error("Error retrieving schema from Airbyte: %s", e)
        raise HTTPException(status_code=500, detail=f"Error retrieving schema from Airbyte: {str(e)}")

    # Store the block details in MongoDB
    block = Block(
        schema_=airbyte_schema,
        canvas_uid=canvas_uid
    )

    blocks_collection.insert_one(block.dict())
    logging.info("Block details stored in MongoDB: %s", block.dict())

    return block.dict()'''
    
async def create_block(canvas_uid: str, sample_file: UploadFile, header_row: bool = False, separator: str = ",") -> Dict[str, Any]:
    logging.info("Received request to create a block")

    # Check if the file is CSV and size is less than 5MB
    if not sample_file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file is not a CSV.")

    file_size = len(sample_file.file.read())  # Read file content to get its size
    sample_file.file.seek(0)  # Reset file pointer to the beginning
    if file_size > 5 * 1024 * 1024:  # 5MB
        raise HTTPException(status_code=400, detail="File size exceeds 5MB limit.")

    try:
        # Create a NamedTemporaryFile to store the uploaded file content
        with NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            # Write the contents of the uploaded file to the temporary file
            temp_file.write(sample_file.file.read())

            # Retrieve the schema from Airbyte using the temporary file
            airbyte_schema = get_airbyte_schema(temp_file.name, separator)

            # Store the block details in MongoDB
            block = Block(
                schema_=airbyte_schema,
                canvas_uid=canvas_uid
            )
            blocks_collection.insert_one(block.dict())

            logging.info("Block details stored in MongoDB: %s", block.dict())

            return block.dict()

    except Exception as e:
        logging.error("Failed to create block: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to create block: {str(e)}")


async def create_temporary_block(canvas_uid: str) -> Block:
    try:
        # Create the temporary block with default values
        temporary_block = Block(
            schema_={},
            canvas_uid=canvas_uid,
            type="temporary",
        )

        # Store block details in MongoDB
        await blocks_collection.insert_one(temporary_block.dict())

        logging.info("Temporary block created: %s", temporary_block.dict())

        return temporary_block
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create temporary block: {str(e)}")

async def connect_csv_temporary(canvas_uid: str) -> dict:
    try:
        # Find CSV connector block associated with the provided canvas_uid
        csv_block =await blocks_collection.find_one({"canvas_uid": canvas_uid, "type": "connector", "connector_type": "csv"})
        if not csv_block:
            raise HTTPException(status_code=404, detail=f"No CSV connector block found for canvas UID: {canvas_uid}")

        # Create or find the temporary block associated with the provided canvas_uid
        temporary_block =await blocks_collection.find_one({"canvas_uid": canvas_uid, "type": "temporary"})
        if not temporary_block:
            # If temporary block does not exist, create it
            temporary_block = await create_temporary_block(canvas_uid)

        # Check if canvas IDs match
        if csv_block.get("canvas_uid") == temporary_block.get("canvas_uid"):
            # Define left block as CSV connector block and right block as temporary block
            left_block = csv_block
            right_block = temporary_block

        # Return information about left and right blocks
        return {
            "left_block": remove_object_id(left_block),
            "right_block": remove_object_id(right_block)
        }
    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Failed to connect CSV connector and temporary block: {str(e)}")
    
#------------------------------------------------------------------------------------------------------------------#

async def drop_block( canvas_uid: str) -> dict:
    try:
        # Find the block associated with the provided canvas_uid
        block = await blocks_collection.find_one({"canvas_uid": canvas_uid})
        
        # If block not found, raise an HTTPException with status code 404
        if not block:
            raise HTTPException(status_code=404, detail=f"No block found for canvas UID: {canvas_uid}")
        
        # Delete the block from the database
        await blocks_collection.delete_one({"canvas_uid": canvas_uid})
        
        # Return a success message
        return {"message": f"Block for canvas UID {canvas_uid} has been dropped successfully."}
        
    except HTTPException as e:
        raise e
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop block: {str(e)}")
    
#-------------------------------------------------------------------------#


       