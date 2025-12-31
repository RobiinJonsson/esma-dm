# RTS 23 – Financial Instrument Reference Data Schema

This document provides a clean, developer-friendly data dictionary derived from **RTS 23 (MiFIR)** – Commission Delegated Regulation supplementing Regulation (EU) No 600/2014.

It is intended for use in:
- Market data platforms
- Reference data databases
- FIRDS ingestion pipelines
- Validation and schema enforcement
- RAG / knowledge-base projects

---

## 1. Common Data Types & Formats

### Text & Identifiers
- **ALPHANUM-n**: Free text, up to *n* alphanumeric characters
- **ISIN**: 12-character ISO 6166 instrument identifier
- **LEI**: 20-character ISO 17442 Legal Entity Identifier
- **MIC**: 4-character ISO 10383 Market Identifier Code
- **FISN**: 35-character ISO 18774 Financial Instrument Short Name
- **CFI_CODE**: 6-character ISO 10962 Classification of Financial Instruments

### Codes
- **COUNTRYCODE_2**: ISO 3166-1 alpha-2 country code
- **CURRENCYCODE_3**: ISO 4217 currency code

### Date & Time
- **DATEFORMAT**: ISO 8601 date (`YYYY-MM-DD`)
- **DATE_TIME_FORMAT**: ISO 8601 UTC timestamp (`YYYY-MM-DDThh:mm:ss.ddddddZ`)

### Numeric
- **DECIMAL-n/m**: Decimal number with up to *n* digits total and *m* fractional digits
- **INTEGER-n**: Signed integer with up to *n* digits

---

## 2. Commodity & Emission Derivatives Classification

### Base Products
- AGRI – Agricultural
- NRGY – Energy
- ENVR – Environmental
- EMIS – Emissions
- FRGT – Freight
- FRTL – Fertilizer
- INDP – Industrial Products
- METL – Metals
- POLY – Polypropylene / Plastics
- INFL – Inflation
- OEST – Official Economic Statistics
- OTHR – Other

### Example Sub Products
- Grains & Oil Seeds
- Softs
- Electricity
- Natural Gas
- Oil
- Coal
- Renewable Energy
- Freight (Wet / Dry)
- Precious Metals
- Non-Precious Metals

> **Note**: Only valid combinations defined by ESMA are permitted.

---

## 3. Financial Instrument Reference Data Fields

### General Instrument Fields
| # | Field | Description | Format |
|---|------|------------|--------|
| 1 | Instrument identification code | Unique identifier of the instrument | ISIN |
| 2 | Instrument full name | Full legal name | ALPHANUM-350 |
| 3 | Instrument classification | CFI code | CFI_CODE |
| 4 | Commodity / emission derivative indicator | Is commodity or emission allowance derivative | Boolean |

---

### Issuer Fields
| # | Field | Description | Format |
|---|------|------------|--------|
| 5 | Issuer / Trading venue operator identifier | LEI of issuer or venue operator | LEI |

---

### Venue & Admission Fields
| # | Field | Description | Format |
|---|------|------------|--------|
| 6 | Trading venue MIC | MIC of trading venue or SI | MIC |
| 7 | Instrument short name | ISO 18774 short name | FISN |
| 8 | Issuer approval indicator | Issuer approved admission | Boolean |
| 9 | Approval date/time | Issuer approval timestamp | DATE_TIME_FORMAT |
|10 | Request date/time | Admission request timestamp | DATE_TIME_FORMAT |
|11 | Admission / first trade date-time | First admission or trade | DATE_TIME_FORMAT |
|12 | Termination date-time | End of trading | DATE_TIME_FORMAT |

---

### Notional Information
| # | Field | Description | Format |
|---|------|------------|--------|
|13 | Notional currency 1 | Primary notional currency | CURRENCYCODE_3 |

---

### Bonds & Securitised Debt
| # | Field | Description | Format |
|---|------|------------|--------|
|14 | Total issued nominal amount | Total issued nominal value | DECIMAL-18/5 |
|15 | Maturity date | Instrument maturity date | DATEFORMAT |
|16 | Currency of nominal value | Nominal currency | CURRENCYCODE_3 |
|17 | Nominal value per unit | Nominal or minimum trade value | DECIMAL-18/5 |
|18 | Fixed rate | Fixed interest rate (%) | DECIMAL-11/10 |
|19 | Floating rate benchmark ISIN | Benchmark identifier | ISIN |
|20 | Floating rate benchmark name | Benchmark name | INDEX / ALPHANUM-25 |
|21 | Benchmark term | Tenor of benchmark | INTEGER + unit |
|22 | Basis point spread | Spread over benchmark | INTEGER-5 |
|23 | Bond seniority | Seniority classification | ENUM |

---

### Derivatives & Securitised Derivatives
| # | Field | Description | Format |
|---|------|------------|--------|
|24 | Expiry date | Derivative expiry | DATEFORMAT |
|25 | Price multiplier | Contract multiplier | DECIMAL-18/17 |
|26 | Underlying instrument ISIN | Underlying instrument | ISIN |
|27 | Underlying issuer | Issuer of underlying | LEI |
|28 | Underlying index name | Index name | INDEX / ALPHANUM-25 |
|29 | Underlying index term | Index tenor | INTEGER + unit |

---

### Options
| # | Field | Description | Format |
|---|------|------------|--------|
|30 | Option type | Call / Put / Other | ENUM |
|31 | Strike price | Strike price or PNDG | DECIMAL / PNDG |
|32 | Strike price currency | Currency of strike | CURRENCYCODE_3 |
|33 | Exercise style | European, American, etc. | ENUM |

---

### Settlement
| # | Field | Description | Format |
|---|------|------------|--------|
|34 | Delivery type | Physical / Cash / Optional | ENUM |

---

### Commodity & Emission Derivatives
| # | Field | Description | Format |
|---|------|------------|--------|
|35 | Base product | Commodity base product | ENUM |
|36 | Sub product | Commodity sub product | ENUM |
|37 | Further sub product | Commodity detail | ENUM |
|38 | Transaction type | Futures, Options, Swaps, etc. | ENUM |
|39 | Final price type | Pricing source | ENUM |

---

### Interest Rate Derivatives
| # | Field | Description | Format |
|---|------|------------|--------|
|40 | Reference rate | Interest rate index | INDEX / ALPHANUM-25 |
|41 | IR contract term | Contract tenor | INTEGER + unit |
|42 | Notional currency 2 | Second leg currency | CURRENCYCODE_3 |
|43 | Fixed rate leg 1 | Fixed rate (%) | DECIMAL-11/10 |
|44 | Fixed rate leg 2 | Fixed rate (%) | DECIMAL-11/10 |
|45 | Floating rate leg 2 | Floating index | INDEX / ALPHANUM-25 |
|46 | Floating rate term leg 2 | Floating rate tenor | INTEGER + unit |

---

### Foreign Exchange Derivatives
| # | Field | Description | Format |
|---|------|------------|--------|
|47 | Notional currency 2 | Second currency of pair | CURRENCYCODE_3 |
|48 | FX type | FX Majors, Crosses, EM | ENUM |

---

## 4. Notes
- All date-times must be reported in **UTC**
- Controlled vocabularies must strictly follow ESMA definitions
- Repeatable fields (e.g. underlyings in baskets) must be modeled as child entities

---

**Source**: RTS 23 – Commission Delegated Regulation (EU) supplementing MiFIR

