from pydantic import BaseModel, ConfigDict


class StrictBase(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class AllowExtraBase(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=True)
