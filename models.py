from typing import List, Optional, Literal, Union
from typing_extensions import Annotated
from pydantic import BaseModel, StringConstraints
import yaml

StrReq = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
StrAny = Annotated[str, StringConstraints(strip_whitespace=True)]

# -------------------------
# Secciones de entrada
# -------------------------
class TablaUC(BaseModel):
    path: StrReq

class SourceItem(BaseModel):
    tipo_fuente: Literal["DL", "RDBMS", "API", "FILE", "STREAM"]
    nombre_tecnico_origen: StrReq
    unity_catalog_fuente: StrReq
    tabla_origen: StrReq

class SchemaCol(BaseModel):
    name: StrReq
    type: StrReq
    nullable: bool
    is_required: bool
    description: Optional[StrAny] = None  # <— NUEVO (opcional)

class Constraints(BaseModel):
    primary_key: List[StrReq]
    unique: Optional[List[StrReq]] = None
    required_fields: Optional[List[StrReq]] = None

# -------------------------
# Validations (tipadas)
# -------------------------
class VNullCheck(BaseModel):
    type: Literal["null_check"]
    columns: List[StrReq]
    thresholds: Optional[List[float]] = None

class VDuplicateCheck(BaseModel):
    type: Literal["duplicate_check"]
    columns: List[StrReq]

class VRangeCheck(BaseModel):
    type: Literal["range_check"]
    column: StrReq
    min_value: Optional[float] = None
    max_value: Optional[float] = None

class VDateRangeCheck(BaseModel):
    type: Literal["date_range_check"]
    column: StrReq
    start_date: Optional[StrAny] = None
    end_date: Optional[StrAny] = None

class VCompleteness(BaseModel):
    type: Literal["completeness"]
    expected_min_records: int

class VConsistencyCross(BaseModel):
    type: Literal["consistency_cross"]
    df_reference: StrReq
    foreign_key: StrReq
    reference_key: StrReq

class VConsistencyInclude(BaseModel):
    type: Literal["consistency_Include"]
    column: StrReq
    expected_value: Union[str, int, float, bool]
    threshold: Optional[float] = 0.0

class VStatsOutlier(BaseModel):
    type: Literal["stats_outlier"]
    column: StrReq
    method: Optional[Literal["zscore", "iqr"]] = "zscore"
    zscore_threshold: Optional[float] = 3.0

class VRowsCountChange(BaseModel):
    type: Literal["rows_count_change"]
    previous_count: int
    max_percent_change: Optional[float] = 0.1

class VPatternMatch(BaseModel):
    type: Literal["pattern_match"]
    column: StrReq
    pattern: StrReq
    expected_match_rate: Optional[float] = 1.0

class VMonotonicity(BaseModel):
    type: Literal["monotonicity"]
    order_by: StrReq
    direction: Literal["increasing", "decreasing"]

class VDistValueCount(BaseModel):
    type: Literal["dist_value_count"]
    column: StrReq
    min_distinct: Optional[int] = None
    max_distinct: Optional[int] = None

class VColDependency(BaseModel):
    type: Literal["col_dependency"]
    column: StrReq
    condition_column: StrReq
    condition_value: Union[str, int, float, bool, Literal["Any"]]

class VColCorrelation(BaseModel):
    type: Literal["col_correlation"]
    column_1: StrReq
    column_2: StrReq
    max_correlation: float

class VFreshness(BaseModel):
    type: Literal["freshness"]
    timestamp_column: StrReq
    max_age_hours: int

ValidationItem = Union[
    VNullCheck, VDuplicateCheck, VRangeCheck, VDateRangeCheck, VCompleteness,
    VConsistencyCross, VConsistencyInclude, VStatsOutlier, VRowsCountChange,
    VPatternMatch, VMonotonicity, VDistValueCount, VColDependency,
    VColCorrelation, VFreshness,
]

class Ownership(BaseModel):
    owner_analitico: StrReq
    owner_funcional: Optional[StrAny] = None
    steward_tecnico: Optional[StrAny] = None
    notification_channel: Optional[StrAny] = None
    notification_group: Optional[StrAny] = None

# -------------------------
# Request completo
# -------------------------
class DataContractBody(BaseModel):
    tabla_uc: TablaUC
    source: List[SourceItem]
    schema: List[SchemaCol]
    constraints: Constraints
    validations: List[ValidationItem]
    ownership: Ownership
    description: Optional[StrAny] = None   # <— NUEVO (opcional)

class DataContractRequest(BaseModel):
    data_contract: DataContractBody

# -------------------------
# Construir YAML
# -------------------------
def build_yaml(request: DataContractRequest) -> str:
    return yaml.safe_dump(
        data=request.model_dump(mode="python"),
        sort_keys=False,
        allow_unicode=True,
        width=1000,
        default_flow_style=False,
    )
