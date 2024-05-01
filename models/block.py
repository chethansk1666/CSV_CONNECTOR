# models/block.py

from pydantic import BaseModel

class Block(BaseModel):
    canvas_uid: str
    type: str = "connector"
    connector_type: str = "csv"
    schema_: dict


