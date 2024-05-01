
from fastapi import APIRouter, HTTPException, UploadFile, Depends
from models.block import Block
from motor.motor_asyncio import AsyncIOMotorClient
from db_operations import create_block as create_block_db,create_temporary_block as create_temporary_block_db, connect_csv_temporary as connect_csv_temporary_db,drop_block as drop_block_db
from typing import Any
from db_operations import DATABASE_NAME


router = APIRouter()

#Creating Block Endpoint for csv file
@router.post("/block/", response_model=Block, tags=["CANVAS"])
async def create_block_endpoint(canvas_uid: str, sample_file: UploadFile, header_row: bool = False, separator: str = ","):
    try:
        return await create_block_db(canvas_uid, sample_file, header_row, separator)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create block: {str(e)}")

#Creating temporary block which accepts canvas_uid and others are None
@router.post("/block/temporary/", response_model=Block, tags=["CANVAS"])
async def create_temporary_block_endpoint(canvas_uid: str):
    try:
        return await create_temporary_block_db(canvas_uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create temporary block: {str(e)}")

@router.post("/block/connect/csv_temporary/", response_model=dict, tags=["CANVAS"])
async def connect_csv_temporary_endpoint(canvas_uid: str):
    try:
        return await connect_csv_temporary_db(canvas_uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect CSV connector and temporary block: {str(e)}")

# Endpoint to drop a block
@router.delete("/block/drop/{canvas_uid}", tags=["CANVAS"])
async def drop_block_endpoint(canvas_uid: str):
    try:
        result = await drop_block_db( canvas_uid)
        return result
    except HTTPException as e:
        return e
    
    
#----------------------------------------------------------------------

