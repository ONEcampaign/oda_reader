"""Column names and Arrow types for the bulk files `oda_reader.scan` converts.

The DAC1, DAC2A, DAC2B and Multisystem bulk files are zipped CSVs whose headers
pair an upper-case code column with a label column (``DONOR`` and ``Donor``) or
use CamelCase (``DonorCode``). Names that differ only by case collide in
case-insensitive engines such as DuckDB, so `scan` writes the snake_case names
below. They reuse the names `download_dac1`, `download_dac2a` and the raw CRS
Parquet already use (``donor_code``, ``aidtype_code``, ``purpose_code``).

A header missing from these maps is kept as a string column under its
`snake_case` name, so a column OECD adds still converts.
"""

import re
import typing

import pyarrow as pa


class BulkColumn(typing.NamedTuple):
    """The converted name and Arrow type of one bulk-file column."""

    name: str
    type: pa.DataType


_CODE = pa.int32()
_TEXT = pa.string()
_AMOUNT = pa.float64()

# DAC1, DAC2A and DAC2B share the legacy .Stat table layout. `Year` repeats `TIME`
# on every row and is dropped (mapped to None).
DOTSTAT_TABLE_COLUMNS: dict[str, BulkColumn | None] = {
    "DONOR": BulkColumn("donor_code", _CODE),
    "Donor": BulkColumn("donor_name", _TEXT),
    "RECIPIENT": BulkColumn("recipient_code", _CODE),
    "Recipient": BulkColumn("recipient_name", _TEXT),
    "PART": BulkColumn("part_code", _CODE),
    "Part": BulkColumn("part_name", _TEXT),
    "AIDTYPE": BulkColumn("aidtype_code", _CODE),
    "Aid type": BulkColumn("aid_type", _TEXT),
    "FLOWS": BulkColumn("flows_code", _CODE),
    "Fund flows": BulkColumn("fund_flows", _TEXT),
    "AMOUNTTYPE": BulkColumn("amounttype_code", _TEXT),
    "DATATYPE": BulkColumn("data_type_code", _TEXT),
    "Amount type": BulkColumn("amount_type", _TEXT),
    "TIME": BulkColumn("year", _CODE),
    "Year": None,
    "Value": BulkColumn("value", _AMOUNT),
    "Flags": BulkColumn("flags", _TEXT),
}

MULTISYSTEM_COLUMNS: dict[str, BulkColumn | None] = {
    "Year": BulkColumn("year", _CODE),
    "DonorCode": BulkColumn("donor_code", _CODE),
    "DonorNameE": BulkColumn("donor_name", _TEXT),
    "AgencyNameE": BulkColumn("agency_name", _TEXT),
    "Amount": BulkColumn("amount", _AMOUNT),
    "FlowType": BulkColumn("flow_type", _TEXT),
    "AmountType": BulkColumn("amount_type", _TEXT),
    "AidToOrThru": BulkColumn("aid_to_or_thru", _TEXT),
    "ChannelCode": BulkColumn("channel_code", _CODE),
    "ChannelNameE": BulkColumn("channel_name", _TEXT),
    "ChannelReportedNameE": BulkColumn("channel_reported_name", _TEXT),
    "ChannelCategory": BulkColumn("channel_category_code", _CODE),
    "ChannelCategoryName": BulkColumn("channel_category_name", _TEXT),
    "RecipientCode": BulkColumn("recipient_code", _CODE),
    "RecipientNameE": BulkColumn("recipient_name", _TEXT),
    "Purposecode": BulkColumn("purpose_code", _CODE),
    "Purposename_e": BulkColumn("purpose_name", _TEXT),
    "ShortDescription": BulkColumn("short_description", _TEXT),
    "Gender": BulkColumn("gender", _CODE),
    "Environment": BulkColumn("environment", _CODE),
    "Free-standing Technical Co-operation": BulkColumn("ftc", _CODE),
    "Programme Based Approaches": BulkColumn("pba", _CODE),
    "InvestmentProject": BulkColumn("investment_project", _CODE),
    "Biodiversity": BulkColumn("biodiversity", _CODE),
    "ClimateMitigation": BulkColumn("climate_mitigation", _CODE),
    "ClimateAdaptation": BulkColumn("climate_adaptation", _CODE),
    "Desertification": BulkColumn("desertification", _CODE),
    "RegionNameE": BulkColumn("region_name", _TEXT),
    "IncomeGroupNameE": BulkColumn("incomegroup_name", _TEXT),
    "FlowCode": BulkColumn("flow_code", _CODE),
    "FlowName_e": BulkColumn("flow_name", _TEXT),
    "Aid_T": BulkColumn("aid_t", _TEXT),
    "Aid_T_Description_E": BulkColumn("aid_t_name", _TEXT),
    "Sector": BulkColumn("sector_code", _CODE),
    "SectorNameE": BulkColumn("sector_name", _TEXT),
    "Geography": BulkColumn("geography", _TEXT),
    "ProjectTitle": BulkColumn("project_title", _TEXT),
    "LongDescription": BulkColumn("long_description", _TEXT),
    "CRSid": BulkColumn("crs_id", _TEXT),
    "ProjectNumber": BulkColumn("project_number", _TEXT),
}


def snake_case(name: str) -> str:
    """Convert a header such as ``DonorNameE`` or ``Aid type`` to snake_case."""
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    return re.sub(r"[^0-9A-Za-z]+", "_", spaced).strip("_").lower()


def converted_columns(
    header: list[str], columns: typing.Mapping[str, BulkColumn | None]
) -> dict[str, BulkColumn]:
    """Map each kept header to its converted column, in header order.

    Args:
        header: The raw column names, as the file lists them.
        columns: The dataset's map from raw name to converted column, with
            ``None`` for a column to drop.

    Returns:
        dict[str, BulkColumn]: Raw name -> converted column, for every kept
            column. A raw name missing from *columns* becomes a string column
            named `snake_case(raw)`.

    Raises:
        ValueError: If two converted names are equal case-insensitively.
    """
    converted: dict[str, BulkColumn] = {}
    for raw in header:
        if raw in columns:
            column = columns[raw]
            if column is None:
                continue
        else:
            column = BulkColumn(snake_case(raw), _TEXT)
        converted[raw] = column

    seen: dict[str, str] = {}
    for raw, column in converted.items():
        folded = column.name.casefold()
        if folded in seen:
            raise ValueError(
                f"Bulk columns {seen[folded]!r} and {raw!r} both convert to "
                f"{column.name!r}."
            )
        seen[folded] = raw
    return converted
