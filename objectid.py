from bson import ObjectId
from pydantic import BaseModel
from typing import Any

class PydanticObjectId(str):
    @classmethod
    def validate(cls, value: Any) -> str:
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")
        return str(value)

class Config:
    arbitrary_types_allowed = True
    json_encoders = {
        ObjectId: str
    }
