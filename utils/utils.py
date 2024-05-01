import os
import airbyte as ab
import logging
from fastapi import HTTPException
from fastapi import HTTPException


# Helper functions
def canvas_helper(canvas_data:dict)->dict:
    """
    Helper function to format canvas data.
    """
    formatted_canvas_list = []
    for canvas in canvas_data:
        formatted_canvas = {
            "canvas_name": canvas.get("canvas_name"),
            "canvas_uid": canvas.get("canvas_uid")
        }
        formatted_canvas_list.append(formatted_canvas)
    return formatted_canvas_list


def get_dataset_name(file_path: str) -> str:
    """
    Extracts the dataset name from the file path.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at path: {file_path}")

    file_name = os.path.basename(file_path)
    dataset_name, _ = os.path.splitext(file_name)
    return dataset_name

def get_airbyte_schema(file_path: str, separator: str):
    dataset_name = get_dataset_name(file_path)

    source = ab.get_source("source-file"
                          ,local_executable="source-file.exe"
                          ,install_if_missing= False
                        )
    source.set_config(
        config={
            "dataset_name": dataset_name,
            "format": "csv",
            "url": file_path,
            "provider": {"storage": "local"},
            "reader_options": "{}"
        }
    )

    try:
        #source.check()
        source.select_all_streams()
        #read_result = source.read()
        data = source.read()[dataset_name]
        data_df = data.to_pandas()
        column_types = {col: str(data_df[col].dtype) for col in data_df.columns}
        print(column_types)
        return column_types
    except Exception as e:
        logging.error("Error retrieving schema from Airbyte: %s", e)
        raise HTTPException(status_code=500, detail=f"Error retrieving schema from Airbyte: {str(e)}")
  
def remove_object_id(block_dict):
    if "_id" in block_dict:
        del block_dict["_id"]
    return block_dict
