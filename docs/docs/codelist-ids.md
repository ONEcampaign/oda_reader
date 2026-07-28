# Codelist ID Reference

OECD's codelist dropdown has 27 options: id `0` ("All codes list", which returns every codelist
in one response) plus 26 individual codelists. This page maps each of those 26 ids to its name and
to the [`oda_reader.codelists`](codelists.md) contract that covers it.

| ID   | Name                       | Contract | Notes                                                                                                                                                                   |
| ---- | -------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `1`  | Bi_Multi                   | Category |                                                                                                                                                                         |
| `2`  | Type of flow               | Category |                                                                                                                                                                         |
| `3`  | Channel of delivery        | Category | [one row per reporting standard](codelists.md#category-one-row-per-standard-and-period)                                                                                 |
| `4`  | Currency                   | Category |                                                                                                                                                                         |
| `5`  | Provider                   | Area     |                                                                                                                                                                         |
| `6`  | Nature of submission       | Category |                                                                                                                                                                         |
| `7`  | Markers                    | Category |                                                                                                                                                                         |
| `8`  | Mobilisation leveraging    | Category |                                                                                                                                                                         |
| `9`  | Mobilisation origin        | Category |                                                                                                                                                                         |
| `10` | Purpose code               | Category | [one row per reporting standard](codelists.md#category-one-row-per-standard-and-period); the only codelist carrying [`parent_code`](codelists-reference.md#parent_code) |
| `11` | PSI additionality          | Category |                                                                                                                                                                         |
| `12` | PSI flag                   | Category |                                                                                                                                                                         |
| `13` | Recipient                  | Area     |                                                                                                                                                                         |
| `14` | Co-operation modality      | Category | [one row per reporting standard](codelists.md#category-one-row-per-standard-and-period)                                                                                 |
| `15` | Type of finance            | Category | [one row per reporting standard](codelists.md#category-one-row-per-standard-and-period)                                                                                 |
| `16` | Provider agency            | Agency   | Keyed additionally on `donor_code`; see [one row per donor](codelists.md#agency-one-row-per-donor)                                                                      |
| `17` | Financing Arrangement      | Category |                                                                                                                                                                         |
| `18` | Framework of collaboration | Category |                                                                                                                                                                         |
| `19` | TOSSD Pillar               | Category |                                                                                                                                                                         |
| `20` | Agency Type                | Category |                                                                                                                                                                         |
| `21` | Concessionality            | Category |                                                                                                                                                                         |
| `22` | Keyword                    | Category |                                                                                                                                                                         |
| `23` | Markers value              | Category |                                                                                                                                                                         |
| `24` | LDC Flag                   | Category |                                                                                                                                                                         |
| `25` | Type of Blended Finance    | Category |                                                                                                                                                                         |
| `26` | Type of repayment          | Category |                                                                                                                                                                         |

"Area" is `fetch_codelists` / `parse_codelists`. "Category" is `fetch_code_categories` /
`parse_code_categories`. "Agency" is `fetch_provider_agencies` / `parse_provider_agencies`. All three
are documented in [Codelists](codelists.md) and [Codelists Reference](codelists-reference.md).

## Next Steps

- **[Codelists](codelists.md)** - The `oda_reader.codelists` API these ids feed into
- **[Codelists Reference](codelists-reference.md)** - Signatures, frame columns, and the exception
  contract
