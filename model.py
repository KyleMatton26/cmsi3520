from pydantic import BaseModel, Field, validator
from typing import Optional
from objectid import PydanticObjectId
from datetime import datetime

class HockeyTeam(BaseModel):
    id: Optional[PydanticObjectId] = Field(None, alias="_id")
    Team_Name: str = Field(..., alias="Team_Name")
    Year: int = Field(..., alias="Year", ge=1900, le=2100)
    Wins: int = Field(..., alias="Wins", ge=0)
    Losses: int = Field(..., alias="Losses", ge=0)
    OT_Losses: Optional[int] = Field(None, alias="OT_Losses", ge=0)
    Win_Percentage: float = Field(..., alias="Win_Percentage", ge=0.0, le=1.0)
    Goals_For_GF: int = Field(..., alias="Goals_For_GF", ge=0)
    Goals_Against_GA: int = Field(..., alias="Goals_Against_GA", ge=0)
    Plus_Minus: int = Field(..., alias="+____")
    date_added: Optional[datetime] = None
    date_updated: Optional[datetime] = None

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {
            PydanticObjectId: str,
            datetime: lambda v: v.isoformat() if v else None
        }

    @validator('Year')
    def validate_year(cls, v):
        if v < 1900 or v > 2100:
            raise ValueError('Year must be between 1900 and 2100')
        return v
