"""A module for constructing SDMX API queries for the OECD data."""

import logging
import warnings
from collections.abc import Mapping, Sequence

from oda_reader.download.version_discovery import sdmx_rest_host

logger = logging.getLogger(__name__)

type FilterValue = str | int | list[str] | list[int] | None

# Dimension order of each table's latest data structure, used when the caller
# does not pass the live order.
DAC1_DIMENSIONS: tuple[str, ...] = (
    "DONOR",
    "SECTOR",
    "MEASURE",
    "TYING_STATUS",
    "FLOW_TYPE",
    "UNIT_MEASURE",
    "PRICE_BASE",
)
DAC2_DIMENSIONS: tuple[str, ...] = (
    "DONOR",
    "RECIPIENT",
    "MEASURE",
    "UNIT_MEASURE",
    "PRICE_BASE",
)
CRS_DIMENSIONS: tuple[str, ...] = (
    "DONOR",
    "RECIPIENT",
    "SECTOR",
    "MEASURE",
    "CHANNEL",
    "MODALITY",
    "FLOW_TYPE",
    "PRICE_BASE",
    "MD_DIM",
    "MD_ID",
    "UNIT_MEASURE",
)
MULTISYSTEM_DIMENSIONS: tuple[str, ...] = (
    "DONOR",
    "RECIPIENT",
    "SECTOR",
    "MEASURE",
    "CHANNEL",
    "FLOW_TYPE",
    "PRICE_BASE",
    "MD_DIM",
    "MD_ID",
    "UNIT_MEASURE",
)

V1_DATA_PATH: str = "data/"
V2_DATA_PATH: str = "v2/data/dataflow/"
AGENCY_ID: str = "OECD.DCD.FSD"
SHAPE: str = "dimensionAtObservation=AllDimensions"
FORMAT: str = "csvfilewithlabels"


class QueryBuilder:
    """
    A builder class for constructing SDMX API queries for the OECD data.

    The `build_*_filter` methods place each filter value by dimension ID. They
    follow `dimensions` when given, and `download` passes the order of the
    resolved dataflow version's data structure. Without it they follow the
    order of each table's latest data structure at release time
    (`DAC1_DIMENSIONS`, `DAC2_DIMENSIONS`, `CRS_DIMENSIONS`,
    `MULTISYSTEM_DIMENSIONS`).

    Attributes:
        agency_id (str): The agency ID used in the query.
        base_url (str): The base URL for the query, dynamically determined by the API version.
        params (dict): A dictionary of query parameters, initialized with default format.
        api_version (int): The version of the API to use.
        dimensions (tuple[str, ...] | None): The dimension IDs of the data
            structure in key order, or None to use each builder's default order.
    """

    def __init__(
        self,
        dataflow_id: str,
        dataflow_version: str | None = None,
        api_version: int = 1,
        dimensions: Sequence[str] | None = None,
    ) -> None:
        """
        Initialize the QueryBuilder with specific settings for the API and data flow.

        Args:
            dataflow_id (str): The identifier for the dataflow.
            dataflow_version (str): The version of the dataflow
            api_version (int): The version of the API to use, default is 2.
            dimensions (Sequence[str] | None): The dimension IDs of the
                dataflow version's data structure, in key order. When None,
                each filter builder uses the order of the table's latest data
                structure at release time.
        """
        self.dataflow_id = dataflow_id
        self.dataflow_version = dataflow_version
        self.dimensions = tuple(dimensions) if dimensions is not None else None

        # If dataflow_version is not provided, use the latest version
        if dataflow_version is None:
            dataflow_version = "+" if api_version == 2 and not dataflow_version else ""

        # Set the base URL and separator based on the dataflow and API version
        data_path = V2_DATA_PATH if api_version == 2 else V1_DATA_PATH
        base_url = f"{sdmx_rest_host(dataflow_id)}{data_path}"

        self._separator = "/" if api_version == 2 else ","

        # Set the agency ID
        self.agency_id = AGENCY_ID

        # Set the dimensions filter to all
        self.filter = "*" if api_version == 2 else "all"

        # Construct the base URL
        self.base_url = (
            f"{base_url}{self.agency_id}"
            f"{self._separator}{dataflow_id}"
            f"{self._separator}{dataflow_version}/"
        )

        # Initialize the query parameters with the default format
        self.params: dict[str, str | int] = {"format": FORMAT}

        # Store the API version
        self.api_version = api_version

    def _to_filter_str(self, param: str | int | list[str] | list[int] | None) -> str:
        """Convert a parameter to a filter string.

        Args:
            param (str | int | list[str] | list[int] | None): The parameter to convert.

        Returns:
            str: The parameter as a filter string.
        """

        if param is None:
            return "*" if self.api_version == 2 else ""

        items: list[str | int] = (
            [param] if isinstance(param, (str, int)) else list(param)
        )
        str_param = [str(p) for p in items]

        if (self.api_version == 2) & (len(str_param) > 1):
            logger.info(
                f"API version 2 does not support filtering on multiple values:"
                f"\n{(', '.join(str_param))} \n"
                "Returning all values."
            )
            return "*"

        return "+".join(str_param)

    def _build_key(
        self, values: Mapping[str, FilterValue], default_order: tuple[str, ...]
    ) -> str:
        """Join filter values in the data structure's dimension order.

        Args:
            values: Filter value per dimension ID the builder maps. None leaves
                the dimension unfiltered.
            default_order: The dimension order used when `dimensions` is None.

        Returns:
            str: The key, one segment per dimension.

        Raises:
            ValueError: If a value is set on a dimension the structure lacks.
        """
        order = default_order if self.dimensions is None else self.dimensions

        missing = [d for d, v in values.items() if v is not None and d not in order]
        if missing:
            raise ValueError(
                f"Dataflow {self.dataflow_id} version "
                f"{self.dataflow_version or 'latest'} has no dimension "
                f"{', '.join(missing)}, so the filter on it cannot be applied."
            )

        # An unmapped dimension stays a wildcard, which also returns any total
        # code it carries, so sums over the result may double-count.
        unmapped = [d for d in order if d not in values]
        if unmapped:
            logger.warning(
                f"Dataflow {self.dataflow_id} version "
                f"{self.dataflow_version or 'latest'} has dimensions oda-reader "
                f"does not filter: {', '.join(unmapped)}. The query returns "
                "every code on them."
            )

        return ".".join(self._to_filter_str(values.get(d)) for d in order)

    def set_time_period(
        self, start: int | str | None, end: int | str | None
    ) -> "QueryBuilder":
        """Set the time period for the query. The time period is inclusive.

        Args:
            start (int | str): The start year or date.
            end (int | str): The end year or date.

        Returns:
            Self: Returns self to allow for method chaining.
        """
        if self.api_version == 2:
            if start and end:
                self.params["c[TIME_PERIOD]"] = f"ge:{start}+le:{end}"
                return self
            if start:
                self.params["c[TIME_PERIOD]"] = f"ge:{start}"
            if end:
                self.params["c[TIME_PERIOD]"] = f"ge:1950+le:{end}"

        else:
            if start:
                self.params["startPeriod"] = start
            if end:
                self.params["endPeriod"] = end

        return self

    def build_dac1_filter(
        self,
        donor: str | list[str] | None = None,
        sector: str | list[str] | None = None,
        measure: str | list[str] | None = None,
        flow_type: str | list[str] | None = None,
        unit_measure: str | list[str] | None = None,
        price_base: str | list[str] | None = None,
    ) -> str:
        """Build the filter string for the DAC1 dataflow.

        Values are placed by dimension ID. The default order is
        DONOR.SECTOR.MEASURE.TYING_STATUS.FLOW_TYPE.UNIT_MEASURE.PRICE_BASE.

        Args:
            donor (str | list[str] | None): The donor country code(s).
            sector (str | list[str] | None): The sector code(s).
            measure (str | list[str] | None): The measure code(s).
            flow_type (str | list[str] | None): The flow type code(s).
            unit_measure (str | list[str] | None): The unit of measure code(s).
            price_base (str | list[str] | None): The price base code(s).

        Returns:
            str: The filter string for the query.

        Raises:
            ValueError: If a value is set on a dimension the structure lacks.
        """
        return self._build_key(
            {
                "DONOR": donor,
                "SECTOR": sector,
                "MEASURE": measure,
                "TYING_STATUS": None,
                "FLOW_TYPE": flow_type,
                "UNIT_MEASURE": unit_measure,
                "PRICE_BASE": price_base,
            },
            DAC1_DIMENSIONS,
        )

    def build_dac2_filter(
        self,
        donor: str | list[str] | None = None,
        recipient: str | list[str] | None = None,
        measure: str | int | list[str] | list[int] | None = None,
        unit_measure: str | list[str] | None = None,
        price_base: str | list[str] | None = None,
    ) -> str:
        """Build the filter string for the DAC2 dataflow (DAC2A and DAC2B).

        Both tables live under `DSD_DAC2`, so one filter builder serves both.

        Values are placed by dimension ID. The default order is
        DONOR.RECIPIENT.MEASURE.UNIT_MEASURE.PRICE_BASE.

        Args:
            donor (str | list[str] | None): The donor country code(s).
            recipient (str | list[str] | None): The recipient country code(s).
            measure (int | list[int] | None): The measure code(s).
            unit_measure (str | list[str] | None): The unit of measure code(s).
            price_base (str | list[str] | None): The price base code(s).

        Returns:
            str: The filter string for the query.

        Raises:
            ValueError: If a value is set on a dimension the structure lacks.
        """
        return self._build_key(
            {
                "DONOR": donor,
                "RECIPIENT": recipient,
                "MEASURE": measure,
                "UNIT_MEASURE": unit_measure,
                "PRICE_BASE": price_base,
            },
            DAC2_DIMENSIONS,
        )

    def build_dac2a_filter(
        self,
        donor: str | list[str] | None = None,
        recipient: str | list[str] | None = None,
        measure: str | int | list[str] | list[int] | None = None,
        unit_measure: str | list[str] | None = None,
        price_base: str | list[str] | None = None,
    ) -> str:
        """Deprecated alias for `build_dac2_filter`.

        Kept so existing code built against the DAC2A-only name keeps
        working. `build_dac2_filter` is the DAC2-generic name and also
        serves DAC2B.
        """
        warnings.warn(
            "build_dac2a_filter is deprecated. Use build_dac2_filter instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.build_dac2_filter(
            donor=donor,
            recipient=recipient,
            measure=measure,
            unit_measure=unit_measure,
            price_base=price_base,
        )

    def build_crs_filter(
        self,
        donor: str | list[str] | None = None,
        recipient: str | list[str] | None = None,
        sector: str | int | list[str] | list[int] | None = None,
        measure: str | int | list[str] | list[int] | None = None,
        channel: str | int | list[str] | list[int] | None = None,
        modality: str | list[str] | None = None,
        flow_type: str | list[str] | None = None,
        price_base: str | list[str] | None = None,
        unit_measure: str | list[str] | None = None,
        microdata: bool = True,
    ) -> str:
        """Build the filter string for the CRS dataflow.

        Values are placed by dimension ID. The default order is
        DONOR.RECIPIENT.SECTOR.MEASURE.CHANNEL.MODALITY.FLOW_TYPE.PRICE_BASE
        .MD_DIM.MD_ID.UNIT_MEASURE. `microdata` sets MD_DIM to "DD" (True) or
        "_T" (False).

        Args:
            donor (str | list[str] | None): The donor country code(s).
            recipient (str | list[str] | None): The recipient country code(s).
            sector (int | list[int] | None): The sector code(s).
            measure (int | list[int] | None): The measure code(s).
            channel (str | list[str] | None): The channel code(s).
            modality (str | list[str] | None): The modality code(s).
            flow_type (str | list[str] | None): The flow type code(s).
            unit_measure (str | list[str] | None): The unit of measure code(s).
            price_base (str | list[str] | None): The price base code(s).
            microdata (bool): Whether to get microdata or summary data.

        Returns:
            str: The filter string for the query.

        Raises:
            ValueError: If a value is set on a dimension the structure lacks,
                including MD_DIM, which `microdata` always sets.
        """
        return self._build_key(
            {
                "DONOR": donor,
                "RECIPIENT": recipient,
                "SECTOR": sector,
                "MEASURE": measure,
                "CHANNEL": channel,
                "MODALITY": modality,
                "FLOW_TYPE": flow_type,
                "PRICE_BASE": price_base,
                "MD_DIM": "DD" if microdata else "_T",
                "MD_ID": None,
                "UNIT_MEASURE": unit_measure,
            },
            CRS_DIMENSIONS,
        )

    def build_multisystem_filter(
        self,
        donor: str | list[str] | None = None,
        recipient: str | list[str] | None = None,
        sector: str | int | list[str] | list[int] | None = None,
        measure: str | int | list[str] | list[int] | None = None,
        channel: str | int | list[str] | list[int] | None = None,
        flow_type: str | list[str] | None = None,
        price_base: str | list[str] | None = None,
    ) -> str:
        """Build the filter string for the Multisystem dataflow.

        Values are placed by dimension ID. The default order is
        DONOR.RECIPIENT.SECTOR.MEASURE.CHANNEL.FLOW_TYPE.PRICE_BASE.MD_DIM
        .MD_ID.UNIT_MEASURE. MD_DIM is always "_T".

        Args:
            donor (str | list[str] | None): The donor country code(s).
            recipient (str | list[str] | None): The recipient country code(s).
            sector (int | list[int] | None): The sector code(s).
            measure (int | list[int] | None): The measure code(s).
            channel (int | list[int] | None): The channel code(s).
            flow_type (str | list[str] | None): The flow type code(s).
            price_base (str | list[str] | None): The price base code(s).

        Returns:
            str: The filter string for the query.

        Raises:
            ValueError: If a value is set on a dimension the structure lacks,
                including MD_DIM.
        """
        return self._build_key(
            {
                "DONOR": donor,
                "RECIPIENT": recipient,
                "SECTOR": sector,
                "MEASURE": measure,
                "CHANNEL": channel,
                "FLOW_TYPE": flow_type,
                "PRICE_BASE": price_base,
                "MD_DIM": "_T",
                "MD_ID": None,
                "UNIT_MEASURE": None,
            },
            MULTISYSTEM_DIMENSIONS,
        )

    def set_filter(self, filter_string: str) -> "QueryBuilder":
        """Set the dimensions parameter for the query.

        Args:
            filter_string (str): The filter string for the query.

        Returns:
            Self: Returns self to allow for method chaining.
        """

        self.filter = filter_string
        return self

    def set_last_n_observations(self, n: int) -> "QueryBuilder":
        """Set the number of most recent observations to return.

        Args:
            n (int): The number of most recent observations to return.

        Returns:
            Self: Returns self to allow for method chaining.
        """
        self.params["lastNObservations"] = n
        return self

    def set_format(self, file_format: str) -> "QueryBuilder":
        """Set the format of the output file.

        Args:
            file_format (str): The file format for the output.

        Returns:
            Self: Returns self to allow for method chaining.
        """
        self.params["format"] = file_format
        return self

    def build_query(self) -> str:
        """Construct and return the full query URL.

        Returns:
            str: The fully constructed URL.
        """
        # Create list to contain query parts
        query_parts = [self.base_url + self.filter + "?"]

        # Add each parameter to the query
        query_parts.extend(f"{key}={value}&" for key, value in self.params.items())

        # Return the full query URL, removing the trailing "&"
        return "".join(query_parts).rstrip("&")
