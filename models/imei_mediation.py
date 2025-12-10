from datetime import date, datetime
from typing import Optional, Dict
from pydantic import BaseModel


class IMEIDataModel(BaseModel):
    # Remove _id from the model - let MongoDB generate it
    # _id: Optional[str] = None  # REMOVE THIS
    uf204: Optional[str] = None
    uf102: int
    uf201: str
    uf211: str
    uf301: date
    uf300: str
    uf400: int
    uf202: Optional[str] = None
    uf203: str
    imei_14: Optional[str] = None
    ProcID: Optional[str] = None
    uf212: Optional[str] = None
    uf213: Optional[str] = None
    uf214: Optional[str] = None
    imei_14_destino: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict) -> "IMEIDataModel":
        # Type conversions for string fields
        if "uf211" in data and isinstance(data["uf211"], int):
            data["uf211"] = str(data["uf211"])
        if "uf202" in data and isinstance(data["uf202"], int):
            data["uf202"] = str(data["uf202"])
        if "uf201" in data and isinstance(data["uf201"], int):
            data["uf201"] = str(data["uf201"])

        # Date conversion
        if "uf301" in data:
            if isinstance(data["uf301"], str) or isinstance(data["uf301"], int):
                data["uf301"] = datetime.strptime(str(data["uf301"]), "%Y%m%d").date()

        # Don't generate _id - let MongoDB do it
        # Remove any existing _id or SequenceID from the data
        data.pop("_id", None)
        data.pop("SequenceID", None)

        return cls(**data)

    def model_dump(self, **kwargs):
        """Override to ensure _id is included in output."""
        data = super().model_dump(**kwargs)
        # Don't include SequenceID in the output anymore
        return data