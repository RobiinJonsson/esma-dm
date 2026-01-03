# FIRDS – Instructions for Download and Use of Full, Delta and Cancellations Files

**Document reference**: ESMA65-8-5014 rev.3 (09 February 2022)

This document is a developer-oriented Markdown version of ESMA guidance on how to **download, process, and store FIRDS reference data files**. It is intended to be used directly in technical projects (ETL pipelines, databases, market data platforms).

---

## 1. Purpose & Scope

### Purpose
- Describe FIRDS reference data files published by ESMA
- Explain how to access them (manual and automated)
- Provide guidance on how to use them, especially for historical databases

### Intended Audience
- EU market participants subject to MiFIR
- National Competent Authorities (NCAs)
- Market data and regulatory reporting technology teams

### Scope
- Financial Instrument Reference Data (FIRDS) only

---

## 2. FIRDS Reference Data Files

FIRDS publishes **machine-readable XML files** representing financial instrument reference data.

### 2.1 File Types

#### Full File (FULINS)
- Complete snapshot of all *active* instruments
- Includes instruments:
  - Admitted to trading on an RM
  - Requested for admission
  - Traded on MTF, OTF, or SI
- Generated **weekly**

#### Delta File (DLTINS)
- Incremental changes since the last publication
- Generated **daily**

Record types in Delta files:
- `<NewRcrd>` – new instrument or new ISIN/MIC
- `<ModfdRcrd>` – modification of reference data
- `<TermntdRcrd>` – termination of an instrument
- `<CancRcrd>` – cancellation of an instrument

> Delta files may contain late or corrective records that never appear in the Full file.

#### Invalid Records File (INVINS)
- Records no longer valid or superseded
- Includes historical versions and late-reported instruments

#### Cancelled Records File (FULCAN)
- Consolidated list of cancelled reference data
- Submitted by venues, SIs, and NCAs
- Generated **daily**

---

## 3. Reference Data Content

Each FIRDS record includes:
- All fields defined in **RTS 23 Tables 1–3**
- Country of the Relevant Competent Authority (RCA)

### NCA-only Technical Attributes (Full File)
- `LastUpd` – date/time record was last received
- `IncnsstncyInd` – inconsistency indicator

> Large files may be split (e.g. >500k records or by first letter of CFI code).

---

## 4. Publication Schedule

| File Type | Frequency | Time (CET) |
|---------|----------|------------|
| Full (FULINS) | Weekly (Sunday) | By 09:00 |
| Delta (DLTINS) | Daily | By 09:00 |
| Cancellations (FULCAN) | Daily | By 09:00 |

---

## 5. XML Structure & Schemas

### Structure
- Business Application Header (BAH)
- Payload (Reference Data)

### Schemas
| File Type | XML Schema |
|---------|-----------|
| Full | `auth.017.001.02_ESMAUG_FULINS_1.1.0.xsd` |
| Delta | `auth.036.001.03_ESMAUG_DLTINS_1.2.0.xsd` |
| Cancellations | `auth.102.001.01_ESMAUG_CANINS_1.2.0.xsd` |

---

## 6. File Naming Conventions

### Full Files
```
FULINS_<CFI-1st-letter>_<YYYYMMDD>_<Key1>of<Key2>.zip
```
Example:
```
FULINS_D_20170625_02of02.zip
```

### Delta Files
```
DLTINS_<YYYYMMDD>_<Key1>of<Key2>.zip
```

### Cancellations Files
```
FULCAN_<YYYYMMDD>_<Key1>of<Key2>.zip
```

---

## 7. Accessing FIRDS Files

### 7.1 Human Interface
- ESMA Registers → Financial Instrument Reference Data System
- Filter by publication date
- Download ZIP files

### 7.2 Machine-to-Machine Access

FIRDS supports automated download using HTTP queries.

#### Example Query
```
?q=*&fq=publication_date:[YYYY-MM-DDT00:00:00Z TO YYYY-MM-DDT23:59:59Z]&wt=xml&start=0&rows=100
```

Key parameters:
- `fq=publication_date[...]` – date filter
- `wt` – response format (`xml`, `json`)
- `start`, `rows` – pagination

XPath for download URLs:
```
/response/result/doc/str[@name='download_link']
```

---

## 8. Building a Historical Database

To support historical queries, each record should include:
- `ValidFromDate`
- `ValidToDate`
- `LatestRecordFlag`

### 8.1 Initial Load (Day T)
- Download Full file
- Insert all records
- Set:
  - `ValidFromDate = PublicationPeriod/FromDate`
  - `ValidToDate = NULL`
  - `LatestRecordFlag = TRUE`

### 8.2 Daily Updates (Day T+1)

#### New Records (`<NewRcrd>`)
- Insert record
- Set `ValidFromDate`
- Mark as latest

#### Modified / Cancelled Records (`<ModfdRcrd>`, `<CancRcrd>`)
- Close previous record (`ValidToDate = new.ValidFromDate - 1`)
- Insert new version

#### Terminated Records (`<TermntdRcrd>`)
- Handle late reports and corrections
- Either replace or version existing records depending on dates

---

## 9. Query Patterns

### Latest Version of All Instruments
```
LatestRecordFlag = true
```

### Instruments Active on Date T
```
Field11 <= T AND (Field12 IS NULL OR Field12 >= T)
```

### Historical State on Date T
```
ValidFromDate <= T AND (ValidToDate IS NULL OR ValidToDate >= T)
```

---

## 10. Other Reference Data Files

### CFI, MIC, Currency, Country, Index
- Single file per type
- Includes active and inactive records
- Weekly (daily for Index)
- Historical database mirrors file content

### LEI Reference Data
- Daily file (GLEIF-sourced)
- No full history → historical tracking required
- Special validation rules for transaction reporting

---

## 11. Implementation Notes
- FIRDS data is **append-only with corrections**
- Delta files are mandatory for historical accuracy
- Never rebuild history using Full files alone

---

**Source**: ESMA FIRDS – Instructions for download and use of full, delta and cancellations reference data files

