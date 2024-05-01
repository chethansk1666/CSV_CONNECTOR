from fastapi import APIRouter, HTTPException,Form
from typing import List
from models.canvas import Canvas
from db_operations import create_canvas as create_canvas_db, list_canvas as list_canvas_db

router = APIRouter()

@router.post("/canvas/", response_model=Canvas,tags=["CANVAS"])
async def create_canvas(canvas_name: str=Form(...,example="Enter Canvas Name")
                        , canvas_uid: str = Form(...,example="12")):
    try:
        return await create_canvas_db(canvas_name, canvas_uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create canvas: {str(e)}")

@router.get("/get_canvas/", response_model=List[Canvas], tags=["CANVAS"])
async def get_all_canvas():
    try:
        return await list_canvas_db()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get canvases: {str(e)}")