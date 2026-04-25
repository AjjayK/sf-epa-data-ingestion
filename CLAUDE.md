# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EPA Data Pipeline - A data ingestion and processing system that:
1. Fetches EPA pesticide product data from the EPA API
2. Stores structured data in Snowflake
3. Downloads and processes PDF label files
4. Chunks PDFs for RAG (Retrieval-Augmented Generation) using Snowflake Cortex
5. Creates a searchable index using Cortex Search Service

## Core Architecture

### Environment-based Configuration
The application uses an environment-based multi-tenant architecture with three environments (DEV, TEST, PROD). The `DatabaseConfig` class (lines 34-55 in epa_ingestion.py) dynamically constructs database names by prefixing them with the environment (e.g., `DEV_SRC_INGEST`, `TEST_SRC_INGEST`, `PROD_SRC_INGEST`).

The environment is set via the `ENVIRONMENT` environment variable, which is injected during Docker build and CI/CD deployment. This allows the same codebase to operate against different Snowflake databases without code changes.

### Main Processing Flow (epa_ingestion.py)
The `EPADataProcessor` class orchestrates the entire pipeline:

1. **Data Ingestion** (`process_and_load_data`): Fetches EPA product data from REST API and loads into normalized Snowflake tables (EPA_PRODUCTS, EPA_COMPANY_INFO, EPA_ACTIVE_INGREDIENTS, etc.)

2. **PDF Download** (`download_and_store_pdfs`): Downloads PDF label files from EPA website, uploads to Snowflake stage (`@{ENV}_SRC_INGEST.EPA_RAW.PDF_STORE/EPA_LABEL_PDF`), and tracks metadata in `EPA_PDF_INGESTION_METADATA`

3. **PDF Processing** (`process_pdf_chunks`): Uses Snowflake Cortex to parse PDFs and chunk them using a custom UDF (`TEXT_CHUNKER`), storing results in `DOCS_CHUNKS_TABLE`

4. **Metadata Enrichment** (`update_category`): Joins chunk data with product metadata (company name, signal word, product types)

5. **Search Index Creation** (`create_cortex_search_`): Creates Snowflake Cortex Search Service for semantic search over chunks

### Snowflake Schema
- `{ENV}_SRC_INGEST.EPA_RAW.*`: Raw ingested data (products, PDFs, companies, ingredients, etc.)
- `{ENV}_SRC_INGEST.EPA_PROCESSED.*`: Processed data (chunks, search service, UDFs)
- `{ENV}_DP_APP.MODELED.*`: Application layer (focus products input table)

### Legacy Script
`pdf_ingestion.py` is a standalone script with similar PDF download functionality but without the full pipeline orchestration. It's superseded by `epa_ingestion.py`.

## Common Development Commands

### Local Development
```bash
# Activate virtual environment
.\env\Scripts\activate  # Windows
source env/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Set environment variables in .env file
# Required: user, password, account (Snowflake credentials)
# Optional: ENVIRONMENT (defaults to DEV)

# Run the pipeline locally
python epa_ingestion.py
```

### Docker
```bash
# Build Docker image
docker build \
  --build-arg SNOWFLAKE_USER=<user> \
  --build-arg SNOWFLAKE_PASSWORD=<password> \
  --build-arg SNOWFLAKE_ACCOUNT=<account> \
  --build-arg ENVIRONMENT=<DEV|TEST|PROD> \
  -t epa-data-processor .

# Run container
docker run epa-data-processor
```

## CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/main.yml`) triggers on **push** to `main` or `test`:

1. Builds Docker image with `ENVIRONMENT` build arg (`main` → PROD, `test` → TEST).
2. Pushes to AWS ECR with both `<commit-sha>` and `latest-{ENVIRONMENT}` tags.
3. Renders `.aws/epa-task.template.json` with GitHub Variables (`envsubst`).
4. Registers a new ECS task definition revision (`epa-task-PROD` / `epa-task-TEST`).

The task runs on **Fargate**. **EventBridge Scheduler** triggers it daily (default `cron(0 6 * * ? *)` UTC). The schedule targets the task definition family without a revision, so it always picks up the newest revision automatically.

**Snowflake credentials** come from **AWS Secrets Manager** via the task definition's `secrets` block. They are injected as env vars `user`, `password`, `account` (lowercase, matching `epa_ingestion.py:947-949`). They are NOT passed as Docker build args.

**Infrastructure** (ECR, Fargate cluster, IAM roles, secret, log group, EventBridge schedule, security group, GitHub OIDC provider + CI role) is provisioned by Terraform under `terraform/`. To deploy in a new AWS account: `cd terraform && terraform apply`, then copy outputs into GitHub Variables.

**AWS auth from CI uses GitHub OIDC**, not static access keys. The workflow assumes `vars.AWS_ROLE_ARN` (output `github_actions_role_arn` from Terraform). The trust policy is scoped to `repo:<owner>/<repo>:environment:<PROD|TEST>`, so the role can only be assumed by this repo's workflow running in the matching GitHub Environment.

When applying Terraform to a second environment in the same AWS account, set `create_github_oidc_provider = false` in that env's tfvars — the OIDC provider is account-wide and only needs to be created once.

Required GitHub Variables (per environment): `AWS_REGION`, `AWS_ACCOUNT_ID`, `AWS_ROLE_ARN`, `ECR_REPOSITORY`, `ECS_CLUSTER`, `TASK_ROLE_ARN`, `EXECUTION_ROLE_ARN`, `LOG_GROUP`, `SECRETS_ARN`. No long-lived AWS access keys are needed.

## Key Implementation Details

### Custom SSL Adapter
The `CustomAdapter` class (lines 21-28 in epa_ingestion.py) disables SSL certificate verification for the EPA API, which uses legacy SSL configurations. This is necessary for making HTTPS requests to `ordspub.epa.gov`.

### PDF Chunking Strategy
The Snowflake UDF `TEXT_CHUNKER` (lines 266-293) uses LangChain's `RecursiveCharacterTextSplitter` with:
- Chunk size: 1512 characters
- Overlap: 256 characters

This is optimized for semantic search and LLM context windows.

### View-based Change Detection
The pipeline uses views (`VW_PDF_TO_DOWNLOAD`, `VW_PDF_TO_CHUNK`) to identify:
- New PDFs to download (PDFs in API response but not in metadata table)
- PDFs ready to chunk (uploaded to stage but status = 'PENDING')

This enables incremental processing without reprocessing all data.

### Merge Strategy for Metadata
PDF metadata uses MERGE statements (lines 585-623) to handle reprocessing of existing products - updates existing records and inserts new ones based on EPAREGNO.

## Snowflake Credentials

Credentials are loaded from environment variables:
- Local: `.env` file (git-ignored)
- Docker: Build args passed during image build
- CI/CD: GitHub Secrets (`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`)

The warehouse is hardcoded to `COMPUTE_WH`.
