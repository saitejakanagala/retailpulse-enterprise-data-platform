# RetailPulse — Phase 2: AWS S3 Raw Ingestion

## 1. Phase 2 Objective

Phase 2 establishes the first cloud ingestion path for the RetailPulse enterprise data platform.

The goal is to move data from the local operational source systems into an Amazon S3 raw data lake using Python.

### High-Level Architecture

```text
┌──────────────────────────┐
│ Local Source Systems     │
│                          │
│ PostgreSQL               │
│ - customers              │
│ - orders                 │
│ - order_items            │
│ - payments               │
│                          │
│ MySQL                    │
│ - suppliers              │
│ - products               │
│ - stores                 │
│ - inventory              │
└─────────────┬────────────┘
              │
              │ Python extraction
              │ psycopg / mysql-connector-python
              ▼
┌──────────────────────────┐
│ Local CSV Extracts       │
│ data/extracts/...        │
└─────────────┬────────────┘
              │
              │ boto3
              ▼
┌──────────────────────────┐
│ Amazon S3 Data Lake      │
│                          │
│ raw/postgres/...         │
│ raw/mysql/...            │
│                          │
│ load_date=YYYY-MM-DD/    │
└──────────────────────────┘
```

### Current Phase 2 Boundary

Phase 2 currently covers:

- AWS S3 bucket setup
- AWS CLI installation and authentication
- boto3 installation and authentication
- PostgreSQL full-table extraction to CSV
- MySQL full-table extraction to CSV
- upload of raw CSV files to S3
- S3 path organization by source, table, and load date
- manual validation of representative files

PySpark is **not used yet**. PySpark/AWS Glue will be introduced in a later transformation phase.

---


## 1.1 Quick One-Line Definitions

| Term / Tool | One-line definition | Why it is used in Phase 2 |
|---|---|---|
| **Amazon S3** | AWS object-storage service used to store files as objects inside buckets. | Stores the raw PostgreSQL and MySQL extracts in the cloud data lake. |
| **S3 Bucket** | Top-level container that holds S3 objects. | Holds all RetailPulse raw data files. |
| **S3 Prefix** | Folder-like portion of an S3 object key. | Organizes data by source, table, and load date. |
| **S3 Object** | Actual file/data stored in S3. | Each generated CSV becomes an S3 object. |
| **S3 Key** | Full name/path of an object inside a bucket. | Uniquely identifies where each CSV is stored. |
| **boto3** | Official AWS SDK library for Python. | Lets Python upload files to and interact with AWS S3 programmatically. |
| **AWS CLI** | Command-line tool for interacting with AWS services. | Used to authenticate, list buckets, inspect S3 paths, and validate uploaded files. |
| **AWS STS** | AWS Security Token Service used for temporary credentials and identity checks. | `get-caller-identity` verifies which AWS identity the CLI is using. |
| **aws login** | Browser-based AWS CLI sign-in that provides temporary credentials. | Authenticates the local machine without hardcoding long-lived access keys. |
| **botocore** | Low-level core library used internally by boto3. | Handles AWS API requests, credentials, and service communication for boto3. |
| **AWS CRT** | AWS Common Runtime libraries used by modern AWS credential/networking features. | Required so boto3/botocore can use the AWS CLI login credential provider. |
| **psycopg** | Python PostgreSQL database driver. | Connects the ingestion script to PostgreSQL. |
| **mysql-connector-python** | Python driver for connecting to MySQL. | Connects the ingestion script to MySQL. |
| **python-dotenv** | Python library that loads values from a `.env` file. | Reads local database settings without hardcoding passwords in Python. |
| **`.venv`** | Project-specific Python virtual environment. | Keeps Python dependencies isolated from other projects and system Python. |
| **CSV** | Comma-separated text format for tabular data. | Used as the raw extract format before upload to S3. |
| **SSE-S3** | Server-side encryption using keys managed by Amazon S3. | Encrypts RetailPulse S3 objects at rest. |
| **S3 Versioning** | S3 feature that keeps multiple versions of the same object key. | Helps recover from accidental overwrites or deletions. |
| **`load_date=YYYY-MM-DD`** | Partition-style S3 prefix containing the ingestion date. | Organizes ingestion runs and prepares the layout for later Glue/Athena processing. |
| **Git branch** | Independent line of Git development. | Keeps Phase 2 ingestion work isolated until it is reviewed and pushed. |


# 2. S3 Data Lake Setup

## 2.1 S3 Bucket Created

Bucket:

```text
retailpulse-data-lake-saiteja
```

This bucket is the cloud storage location for the RetailPulse data lake.

### S3 Concepts

**Bucket** — top-level S3 storage container.

**Object** — an actual file stored in S3, for example:

```text
customers_20260814T143645Z.csv
```

**Key** — the full object name/path inside the bucket, for example:

```text
raw/postgres/customers/load_date=2026-08-14/customers_20260814T143645Z.csv
```

**Prefix** — a folder-like portion of an S3 key, for example:

```text
raw/postgres/customers/
```

S3 does not use traditional filesystem directories internally. The AWS Console presents prefixes in a folder-like interface.

## 2.2 Bucket Versioning

Configured:

```text
Bucket Versioning: Enabled
```

### Why

Versioning helps preserve multiple versions of an S3 object if the same object key is overwritten or deleted. This provides an additional recovery mechanism during development and later production operation.

## 2.3 Default Encryption

Configured:

```text
Server-side encryption with Amazon S3 managed keys (SSE-S3)
```

### Why

SSE-S3 automatically encrypts objects stored in the bucket at rest. For the current project stage, SSE-S3 provides a simple secure default.

Later enterprise-security work can introduce AWS KMS and customer-managed KMS keys if stronger control, auditability, or compliance requirements are needed.

## 2.4 Public Access

The bucket was kept private. Public access should remain blocked because this bucket contains internal data-platform files and should not be publicly accessible.

---

# 3. Python AWS SDK Setup

## 3.1 boto3

`boto3` is the AWS SDK for Python. It allows Python applications to call AWS APIs programmatically.

```text
Python
  ↓
boto3
  ↓
AWS API
  ↓
Amazon S3
```

### Installation

The project virtual environment was activated first:

```powershell
.\.venv\Scripts\Activate.ps1
```

The terminal prompt should show:

```text
(.venv)
```

Then boto3 was installed:

```powershell
pip install boto3
```

### Verification

```powershell
pip show boto3
```

The output confirmed that boto3 was installed inside:

```text
.venv\Lib\site-packages
```

This keeps the dependency isolated to the RetailPulse project.

---

# 4. AWS CLI Setup

## 4.1 What AWS CLI Is

AWS CLI means **AWS Command Line Interface**. It allows AWS services to be accessed from PowerShell or another shell without using the AWS Console.

Examples:

```powershell
aws s3 ls
aws sts get-caller-identity
```

### AWS Console vs AWS CLI vs boto3

```text
AWS Console = browser-based graphical interface
AWS CLI     = command-line interaction with AWS
boto3       = Python code interacting with AWS
```

## 4.2 Initial AWS CLI Check

Command:

```powershell
aws --version
```

### Error

```text
aws : The term 'aws' is not recognized...
CommandNotFoundException
```

### Cause

AWS CLI was not installed on the Windows machine.

## 4.3 AWS CLI v2 Installation

The recommended AWS Windows installation script was used:

```powershell
irm https://awscli.amazonaws.com/v2/install.ps1 | iex
```

Observed install location:

```text
C:\Users\saite\AppData\Local\Programs\Amazon\AWSCLIV2
```

Installed AWS CLI version during setup:

```text
2.36.22
```

---

# 5. AWS CLI PATH Issue

After AWS CLI installation, running:

```powershell
aws --version
```

still returned `CommandNotFoundException`.

## 5.1 Cause

The AWS CLI executable existed, but the current PowerShell session did not have its installation directory in the `PATH`.

## 5.2 Direct Executable Test

```powershell
& "C:\Users\saite\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" --version
```

This verifies the installation even when the `aws` command is not yet available through `PATH`.

## 5.3 Temporary PATH Fix

```powershell
$env:Path += ";C:\Users\saite\AppData\Local\Programs\Amazon\AWSCLIV2"
```

Then:

```powershell
aws --version
```

worked successfully.

## 5.4 Permanent User PATH Fix

```powershell
[Environment]::SetEnvironmentVariable(
  "Path",
  [Environment]::GetEnvironmentVariable("Path", "User") + ";C:\Users\saite\AppData\Local\Programs\Amazon\AWSCLIV2",
  "User"
)
```

After opening a fresh terminal, `aws --version` worked normally.

---

# 6. AWS Authentication

## 6.1 Initial Identity Test

```powershell
aws sts get-caller-identity
```

### Error

```text
NoCredentials: Unable to locate credentials
```

### Meaning

AWS CLI was installed correctly, but no AWS authentication session was available yet.

## 6.2 Login

Authentication was performed using:

```powershell
aws login
```

A browser-based AWS sign-in flow opened. During the flow, the CLI presented a default region prompt:

```text
AWS Region [us-east-1]:
```

The browser then confirmed that credentials had been shared successfully with the CLI session.

### Security Benefit

This avoids putting AWS access keys directly into Python source code, `.env`, Git, or GitHub.

---

# 7. AWS CLI → S3 Verification

Command:

```powershell
aws s3 ls
```

The command successfully returned S3 buckets available to the authenticated AWS identity, including:

```text
retailpulse-data-lake-saiteja
```

This proved:

```text
Windows / VS Code Terminal
        ↓
AWS CLI
        ↓
Authenticated AWS session
        ↓
Amazon S3
```

---

# 8. boto3 Authentication Testing

## 8.1 First boto3 Test

```powershell
py -c "import boto3; s3=boto3.client('s3'); print([b['Name'] for b in s3.list_buckets()['Buckets']])"
```

### Error

```text
ModuleNotFoundError: No module named 'boto3'
```

### Cause

The new terminal was not using the project virtual environment. The prompt did not show `(.venv)`.

### Fix

```powershell
.\.venv\Scripts\Activate.ps1
```

Then the test was run again.

## 8.2 Second boto3 Authentication Error

After activating `.venv`, boto3 was found, but authentication produced:

```text
botocore.exceptions.MissingDependencyException
```

The AWS login credential provider required an additional dependency.

### Fix

```powershell
pip install "botocore[crt]"
```

### Why

The AWS login credential provider requires AWS Common Runtime support for boto3/botocore to use the login-based credentials.

## 8.3 Successful boto3 Verification

The S3 bucket listing test was run again:

```powershell
py -c "import boto3; s3=boto3.client('s3'); print([b['Name'] for b in s3.list_buckets()['Buckets']])"
```

Python successfully returned the S3 bucket list, including `retailpulse-data-lake-saiteja`.

```text
Python
  ↓
boto3
  ↓
botocore + CRT
  ↓
AWS login credentials
  ↓
Amazon S3
```

---

# 9. Raw S3 Prefix Design

```text
s3://retailpulse-data-lake-saiteja/
└── raw/
    ├── postgres/
    │   ├── customers/
    │   │   └── load_date=YYYY-MM-DD/
    │   ├── orders/
    │   │   └── load_date=YYYY-MM-DD/
    │   ├── order_items/
    │   │   └── load_date=YYYY-MM-DD/
    │   └── payments/
    │       └── load_date=YYYY-MM-DD/
    │
    └── mysql/
        ├── suppliers/
        │   └── load_date=YYYY-MM-DD/
        ├── products/
        │   └── load_date=YYYY-MM-DD/
        ├── stores/
        │   └── load_date=YYYY-MM-DD/
        └── inventory/
            └── load_date=YYYY-MM-DD/
```

### Why `load_date=YYYY-MM-DD`

This creates a partition-style directory convention.

Example:

```text
raw/postgres/orders/load_date=2026-08-14/
```

Benefits:

- separates ingestion runs by date
- provides historical organization
- prepares the layout for later Glue/Athena partitioning
- avoids putting every extract into one flat prefix

---

# 10. PostgreSQL → S3 Ingestion

## 10.1 Script

Created:

```text
src/ingestion/postgres_to_s3.py
```

### Libraries Used

```text
os
csv
datetime
boto3
psycopg
python-dotenv
```

### Process

```text
PostgreSQL
   ↓
psycopg
   ↓
Python
   ↓
CSV
   ↓
boto3
   ↓
Amazon S3 raw layer
```

## 10.2 Source Tables

```text
customers
orders
order_items
payments
```

The implementation loops through a table list instead of using four separate ingestion programs.

## 10.3 Database Connection

The script connects through the host-mapped PostgreSQL port:

```text
127.0.0.1:<POSTGRES_PORT>
```

Database credentials are read from the local `.env` file with `python-dotenv`. They are not hardcoded in the script.

## 10.4 Extraction

For each table, Python executes a query such as:

```sql
SELECT * FROM retail.customers;
```

Rows and column names are written to CSV.

## 10.5 Local Extract Directory

```text
data/extracts/postgres/
```

Example:

```text
customers_20260814T143645Z.csv
```

The UTC timestamp identifies the ingestion run and avoids filename collisions.

## 10.6 S3 Upload

Example key:

```text
raw/postgres/customers/load_date=2026-08-14/customers_20260814T143645Z.csv
```

## 10.7 PostgreSQL Results

```text
customers      1000
orders         1987
order_items    3988
payments       1466
```

All four PostgreSQL tables were generated and uploaded successfully.

---

# 11. MySQL → S3 Ingestion

## 11.1 Script

Created:

```text
src/ingestion/mysql_to_s3.py
```

### Libraries Used

```text
os
csv
datetime
boto3
mysql.connector
python-dotenv
```

### Process

```text
MySQL
  ↓
mysql-connector-python
  ↓
Python
  ↓
CSV
  ↓
boto3
  ↓
Amazon S3 raw layer
```

## 11.2 Source Tables

```text
suppliers
products
stores
inventory
```

## 11.3 Local Extract Directory

```text
data/extracts/mysql/
```

Example:

```text
products_20260814T144418Z.csv
```

## 11.4 S3 Upload

Example key:

```text
raw/mysql/products/load_date=2026-08-14/products_20260814T144418Z.csv
```

## 11.5 MySQL Results

```text
suppliers      10
products       50
stores         10
inventory      500
```

All four MySQL tables were generated and uploaded successfully.

---

# 12. S3 Operational Commands

List all buckets:

```powershell
aws s3 ls
```

List bucket root:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/
```

List PostgreSQL raw prefix:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/postgres/
```

List MySQL raw prefix:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/mysql/
```

Recursively list PostgreSQL objects:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/postgres/ --recursive
```

Recursively list MySQL objects:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/mysql/ --recursive
```

---

# 13. S3 Navigation Clarification

During testing, there was temporary confusion between local filesystem navigation and S3 navigation.

An incorrect command was entered:

```powershell
cs ..
```

PowerShell returned `CommandNotFoundException`.

The correct local filesystem command is:

```powershell
cd ..
```

However, `cd` only navigates the Windows filesystem. S3 is not navigated using `cd`.

Instead, use:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/postgres/
```

---

# 14. S3 Object Validation

## 14.1 `cp` Meaning

In:

```powershell
aws s3 cp
```

`cp` means **copy**.

Copy from S3 to a local directory:

```powershell
aws s3 cp s3://bucket/path/file.csv .
```

Copy from local machine to S3:

```powershell
aws s3 cp localfile.csv s3://bucket/path/
```

Print an S3 object directly to the terminal:

```powershell
aws s3 cp s3://bucket/path/file.csv -
```

The final `-` means write the copied object to standard output instead of creating a local file.

## 14.2 Initial Validation Failure

A validation attempt used a placeholder filename such as:

```text
<customers-file>.csv
```

This produced a failed copy/download operation.

### Cause

AWS CLI requires the exact S3 object key.

### Fix

First list the prefix:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/postgres/customers/load_date=2026-08-14/
```

Then use the exact returned object name.

## 14.3 PostgreSQL Validation

The PostgreSQL `customers` CSV was successfully printed/read from S3. The output contained the expected CSV header and customer records, confirming that the uploaded object was not empty and preserved the source data structure.

## 14.4 MySQL Validation

The MySQL `products` CSV was also successfully inspected from S3. The output included product IDs through `1050` and fields such as supplier ID, product name, category, brand, cost, price, status, and timestamps.

---

# 15. Phase 2 Troubleshooting Summary

| Problem | Cause | Resolution |
|---|---|---|
| `aws` command not found | AWS CLI not installed | Installed AWS CLI v2 |
| `aws` still not found after installation | CLI install directory missing from current PATH | Added AWS CLI path to `PATH` |
| Future terminals needed CLI access | Initial PATH change was session-only | Added permanent user PATH entry |
| `aws sts get-caller-identity` returned `NoCredentials` | No AWS authentication session | Used `aws login` |
| `ModuleNotFoundError: boto3` | `.venv` was not active | Activated `.venv` |
| `MissingDependencyException` | AWS login provider required CRT support | Installed `botocore[crt]` |
| S3 copy/download failed during validation | Placeholder filename used | Listed prefix and used exact object name |
| `cs ..` failed | Invalid PowerShell command / typo | Used `cd ..` |
| Confusion about `cd` into S3 | S3 is object storage, not local filesystem | Used `aws s3 ls s3://...` |

---

# 16. Security Decisions

## 16.1 Database Credentials

Database credentials remain in:

```text
.env
```

Python reads them at runtime. The actual `.env` file must remain ignored by Git.

## 16.2 AWS Credentials

AWS credentials are not hardcoded in Python. Authentication is obtained through:

```powershell
aws login
```

boto3 then uses the available AWS credential provider chain.

## 16.3 Generated Raw Extracts

The local extract directory contains generated CSV data:

```text
data/extracts/
```

These files should **not** be committed to GitHub.

Recommended `.gitignore` entry:

```text
data/extracts/
```

## 16.4 S3 Security

```text
Versioning: Enabled
Default encryption: SSE-S3
Public access: Blocked
```

---

# 17. Python Dependencies Added in Phase 2

Phase 2 introduced:

```text
boto3
botocore[crt]
```

Existing source-system dependencies continue to be used:

```text
psycopg[binary]
mysql-connector-python
python-dotenv
```

Before committing, verify that `requirements.txt` contains the dependencies required to recreate the environment.

---

# 18. Running the Phase 2 Ingestion

Activate virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Confirm source containers:

```powershell
docker ps
```

Confirm AWS authentication:

```powershell
aws sts get-caller-identity
```

If the login session has expired:

```powershell
aws login
```

Run PostgreSQL ingestion:

```powershell
py src\ingestion\postgres_to_s3.py
```

Run MySQL ingestion:

```powershell
py src\ingestion\mysql_to_s3.py
```

Verify S3:

```powershell
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/postgres/ --recursive
aws s3 ls s3://retailpulse-data-lake-saiteja/raw/mysql/ --recursive
```

---

# 19. Git Actions for Phase 2

Git work was intentionally performed **after the local-to-S3 migration was tested successfully**.

## 19.1 Phase 2 Branch Created Locally

After successful PostgreSQL and MySQL S3 testing, the local branch was created:

```powershell
git switch -c feature/aws-ingestion
```

Verification:

```powershell
git branch
```

Observed:

```text
* feature/aws-ingestion
  feature/source-systems
  main
```

The `*` indicates the active local branch.

At this point, the branch exists locally until it is pushed to GitHub.

## 19.2 Current Untracked Phase 2 Files

`git status` showed:

```text
data/
src/ingestion/
```

Interpretation:

```text
src/ingestion/ = source code that should be version controlled
data/extracts/ = generated data that should NOT be version controlled
```

## 19.3 Git Preparation Before Commit

Add generated extracts to `.gitignore`:

```text
data/extracts/
```

Then verify:

```powershell
git status
```

`data/extracts/` should no longer appear as an untracked item.

## 19.4 Files Intended for the Phase 2 Commit

Include:

```text
.gitignore
requirements.txt
docs/phase-2-aws-s3-ingestion.md
src/ingestion/postgres_to_s3.py
src/ingestion/mysql_to_s3.py
```

Do not include:

```text
.env
.venv/
data/extracts/
```

## 19.5 Stage the Tested Phase 2 Work

```powershell
git add .gitignore requirements.txt docs/phase-2-aws-s3-ingestion.md src/ingestion
```

Then verify:

```powershell
git status
```

## 19.6 Commit

Recommended commit:

```powershell
git commit -m "feat: add PostgreSQL and MySQL ingestion to S3"
```

## 19.7 First Push of the New Branch

Because `feature/aws-ingestion` is initially local only:

```powershell
git push -u origin feature/aws-ingestion
```

Meaning:

```text
git push             = send committed changes to remote repository
-u                   = set upstream relationship
origin               = GitHub remote repository
feature/aws-ingestion = branch being pushed
```

After the upstream is configured, later pushes can normally use:

```powershell
git push
```

---

# 20. Phase 2 Current Status

```text
S3 data lake bucket created                    ✅
Bucket versioning enabled                      ✅
SSE-S3 default encryption configured           ✅
AWS CLI v2 installed                           ✅
AWS CLI PATH issue resolved                    ✅
AWS CLI authenticated                          ✅
boto3 installed                                ✅
boto3 login dependency issue resolved          ✅
Python → S3 authentication validated           ✅
PostgreSQL customers → S3                      ✅
PostgreSQL orders → S3                         ✅
PostgreSQL order_items → S3                    ✅
PostgreSQL payments → S3                       ✅
MySQL suppliers → S3                           ✅
MySQL products → S3                            ✅
MySQL stores → S3                              ✅
MySQL inventory → S3                           ✅
Representative PostgreSQL CSV validated        ✅
Representative MySQL CSV validated             ✅
Phase 2 Git branch created locally             ✅
```

Pending Git repository actions at the time of this document:

```text
Ignore generated data extracts
Verify requirements.txt
Add this Phase 2 documentation
Stage Phase 2 files
Commit
Push feature/aws-ingestion to GitHub
```

---

# 21. Next Engineering Step

The current scripts perform **full-table batch extraction**.

The next engineering improvements should focus on:

```text
incremental extraction
watermark tracking
idempotency
logging
retry/error handling
row-count reconciliation
metadata capture
data-quality checks
schema evolution handling
quarantine/reprocessing
```

After the raw ingestion layer is reliable, the project can move into:

```text
Amazon S3 raw
      ↓
AWS Glue / PySpark
      ↓
standardized data
      ↓
curated data
      ↓
Snowflake
```

---

# 22. Phase 2 Summary

Phase 2 successfully established the first working cloud data path for RetailPulse.

Two local operational systems now feed an AWS S3 raw data lake using Python:

```text
PostgreSQL ─┐
            ├── Python → CSV → boto3 → S3 raw
MySQL ──────┘
```

The ingestion was tested end to end, AWS CLI and boto3 authentication issues were resolved, representative raw files were validated in S3, and the work is ready to be staged, committed, and pushed from the local `feature/aws-ingestion` branch.
