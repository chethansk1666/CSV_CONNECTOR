# models/canvas.py

from pydantic import BaseModel
from typing import Optional

class Canvas(BaseModel):
    canvas_name: str
    canvas_uid: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "canvas_name": "Example Canvas Name",
                "canvas_uid": "1234567890"
            }
        }
