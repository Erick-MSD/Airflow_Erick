import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import ProgrammingError

# Rutas del DAG: todo vive dentro de dags/northwind/
BASE_DIR = Path(__file__).resolve().parent
IN_DIR = BASE_DIR / "data"  # CSV de entrada
OUTPUT_DIR = BASE_DIR / "output"  # artefactos generados (ignorado en git)
PG_DSN_ENV_VAR = "PG_DSN"  # variable de entorno con la cadena de conexión a PostgreSQL

EXPECTED_COUNTS = {
    "orders.csv": 830,
    "customers.csv": 91,
    "products.csv": 77,
    "categories.csv": 8,
    "order-details.csv": 2155,
}


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("northwind_etl")


def normalize_row_length(row: list[str], expected_columns: list[str]) -> list[str]:
    while len(row) > len(expected_columns):
        merged = False
        for index in range(len(expected_columns) - 1):
            if expected_columns[index] in {"shipAddress", "address"}:
                row[index] = f"{row[index]}, {row[index + 1]}"
                del row[index + 1]
                merged = True
                break
        if not merged:
            row[-2] = f"{row[-2]}, {row[-1]}"
            row.pop()
    while len(row) < len(expected_columns):
        row.append("")
    return row


def resolve_input_path(file_name: str) -> Path:
    candidate = IN_DIR / file_name
    if candidate.exists():
        return candidate
    return candidate


def check_input_directory() -> dict[str, object]:
    logger = setup_logging()
    missing = [name for name in EXPECTED_COUNTS if not resolve_input_path(name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing expected input files: {', '.join(missing)}")
    logger.info("Input directory verified: %s", IN_DIR)
    return {"input_dir": str(IN_DIR), "files": sorted(EXPECTED_COUNTS.keys())}


def load_csv(path: Path, schema: dict | None = None) -> pd.DataFrame:
    logger = logging.getLogger("northwind_etl")
    logger.info("Step 1 - Extracting %s", path.name)
    actual_path = resolve_input_path(path.name)

    if path.name in {"orders.csv", "customers.csv"}:
        expected_columns = [
            "orderID",
            "customerID",
            "employeeID",
            "orderDate",
            "requiredDate",
            "shippedDate",
            "shipVia",
            "freight",
            "shipName",
            "shipAddress",
            "shipCity",
            "shipRegion",
            "shipPostalCode",
            "shipCountry",
        ] if path.name == "orders.csv" else [
            "customerID",
            "companyName",
            "contactName",
            "contactTitle",
            "address",
            "city",
            "region",
            "postalCode",
            "country",
            "phone",
            "fax",
        ]
        rows: list[list[str]] = []
        with open(actual_path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            if not header:
                return pd.DataFrame(columns=expected_columns)
            for raw_row in reader:
                if not raw_row or not any(cell.strip() for cell in raw_row):
                    continue
                rows.append(normalize_row_length([cell.strip() for cell in raw_row], expected_columns))
        df = pd.DataFrame(rows, columns=expected_columns)
    else:
        df = pd.read_csv(actual_path, dtype=schema, keep_default_na=False)

    expected = EXPECTED_COUNTS.get(path.name)
    if expected is not None:
        assert len(df) == expected, (
            f"Row count assertion failed for {path.name}: "
            f"expected {expected}, got {len(df)}"
        )
    logger.info("Loaded %s rows from %s", len(df), path.name)
    return df


def extract_raw_files() -> dict[str, pd.DataFrame]:
    logger = setup_logging()
    frames = {}
    for file_name in sorted(EXPECTED_COUNTS.keys()):
        df = load_csv(IN_DIR / file_name)
        frames[file_name.replace(".csv", "").replace("-", "_")] = df  # order-details -> order_details
    logger.info("Raw extraction completed for %s files", len(frames))
    return frames


def add_invalid_row(records: list[dict], table_name: str, index: int, reason: str, row: dict) -> None:
    records.append(
        {
            "table_name": table_name,
            "row_index": index,
            "reason": reason,
            "raw_row": json.dumps(row, ensure_ascii=False),
        }
    )


def find_literal_nulls(df: pd.DataFrame, table_name: str) -> tuple[pd.DataFrame, list[dict]]:
    logger = logging.getLogger("northwind_etl")
    invalid_rows: list[dict] = []
    text_columns = [col for col in df.columns if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col])]

    for column in text_columns:
        mask = df[column].astype(str).str.strip().str.upper().eq("NULL")
        if mask.any():
            logger.warning("Found literal NULL in %s.%s on %d rows", table_name, column, int(mask.sum()))
            for idx in df.index[mask]:
                add_invalid_row(invalid_rows, table_name, int(idx), f"Literal NULL in {column}", df.loc[idx].to_dict())
            df.loc[mask, column] = pd.NA

    return df, invalid_rows


def looks_like_postal_code(value: object) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if text == "" or text.upper() == "NULL":
        return False
    normalized = text.replace(" ", "").replace("-", "")
    postal_patterns = [
        r"^\d{3,10}$",
        r"^[A-Z]{1,2}\d{1,5}$",
        r"^[A-Z]{1,2}\d[A-Z0-9]?\d[A-Z]{2}$",
        r"^\d{5}-\d{4}$",
    ]
    return any(re.fullmatch(pattern, normalized, flags=re.IGNORECASE) for pattern in postal_patterns)


def validate_country_field(df: pd.DataFrame, table_name: str, country_column: str) -> tuple[pd.DataFrame, list[dict]]:
    invalid_rows: list[dict] = []
    if country_column not in df.columns:
        return df, invalid_rows

    mask = df[country_column].apply(looks_like_postal_code)
    if mask.any():
        logger = logging.getLogger("northwind_etl")
        logger.warning("Country field looks like postal code in %s.%s on %d rows", table_name, country_column, int(mask.sum()))
        for idx in df.index[mask]:
            add_invalid_row(invalid_rows, table_name, int(idx), f"Country value resembles postal code in {country_column}", df.loc[idx].to_dict())
        df.loc[mask, country_column] = pd.NA

    return df, invalid_rows


def normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["orderID", "customerID", "productID", "categoryID", "employeeID", "shipVia", "supplierID"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


def create_versioned_output_dir() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUT_DIR / timestamp
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def save_outputs(df_map: dict[str, pd.DataFrame], output_dir: Path) -> None:
    logger = logging.getLogger("northwind_etl")
    for name, df in df_map.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)
        logger.info("Saved %s to %s", name, output_dir / f"{name}.csv")


def load_to_postgres(df_map: dict[str, pd.DataFrame], audit_rows: list[dict], batch_id: str) -> None:
    # La conexión se lee del entorno para no guardar contraseñas en el código
    dsn = os.getenv(PG_DSN_ENV_VAR)
    if not dsn:
        raise RuntimeError(f"Define la variable de entorno {PG_DSN_ENV_VAR} (ver .env.example)")

    engine = create_engine(dsn)
    key_map = {
        "orders_clean": ["orderID"],
        "customers_clean": ["customerID"],
        "products_clean": ["productID"],
        "categories_clean": ["categoryID"],
        "order_details_clean": ["orderID", "productID"],
        "fact_orders": ["orderID", "productID"],
        "agg_sales_by_country": ["shipCountry"],
        "agg_sales_by_category": ["categoryName"],
    }

    for name, df in df_map.items():
        df = df.copy()
        df["batch_id"] = batch_id
        if "timestamp" not in df.columns:
            df["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        table_exists = inspect(engine).has_table(name)
        if not table_exists:
            df.head(0).to_sql(name, con=engine, if_exists="fail", index=False)

        key_columns = key_map.get(name, [])
        if key_columns:
            with engine.connect() as connection:
                result = connection.execute(text(f'SELECT * FROM "{name}"'))
                existing = pd.DataFrame(result.fetchall(), columns=result.keys())
            if not existing.empty:
                existing_keys = existing[key_columns].astype(str).agg(lambda row: row.str.cat(sep='|'), axis=1)
                new_keys = df[key_columns].astype(str).agg(lambda row: row.str.cat(sep='|'), axis=1)
                df = df[~new_keys.isin(existing_keys)].copy()

        if not df.empty:
            df.to_sql(name, con=engine, if_exists="append", index=False)

    audit_df = pd.DataFrame(audit_rows)
    audit_df["batch_id"] = batch_id
    audit_df["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if not audit_df.empty:
        audit_table_exists = inspect(engine).has_table("audit_invalid_rows")
        if not audit_table_exists:
            audit_df.head(0).to_sql("audit_invalid_rows", con=engine, if_exists="fail", index=False)
        audit_df = audit_df.drop_duplicates(subset=["table_name", "row_index", "reason"], keep="last")
        with engine.connect() as connection:
            result = connection.execute(text('SELECT * FROM "audit_invalid_rows"'))
            existing_audit = pd.DataFrame(result.fetchall(), columns=result.keys())
        if not existing_audit.empty:
            combined = pd.concat([existing_audit, audit_df], ignore_index=True)
            audit_df = combined.drop_duplicates(subset=["table_name", "row_index", "reason"], keep="last")
        audit_records = audit_df.to_dict(orient="records")
        with engine.begin() as connection:
            connection.execute(
                text(
                    'INSERT INTO "audit_invalid_rows" '
                    '("table_name", "row_index", "reason", "raw_row", "batch_id", "timestamp") '
                    'VALUES (:table_name, :row_index, :reason, CAST(:raw_row AS JSONB), '
                    ':batch_id, :timestamp)'
                ),
                audit_records,
            )

    logging.getLogger("northwind_etl").info("Data uploaded to PostgreSQL without overwriting existing tables")


def transform_and_aggregate(raw_frames: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], list[dict], str]:
    logger = setup_logging()
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    orders = normalize_ids(raw_frames["orders"].copy())
    customers = normalize_ids(raw_frames["customers"].copy())
    products = normalize_ids(raw_frames["products"].copy())
    categories = normalize_ids(raw_frames["categories"].copy())
    order_details = normalize_ids(raw_frames["order_details"].copy())

    orders, orders_invalid = find_literal_nulls(orders, "orders")
    customers, customers_invalid = find_literal_nulls(customers, "customers")
    products, products_invalid = find_literal_nulls(products, "products")
    categories, categories_invalid = find_literal_nulls(categories, "categories")
    order_details, details_invalid = find_literal_nulls(order_details, "order_details")

    orders, orders_country_invalid = validate_country_field(orders, "orders", "shipCountry")
    customers, customers_country_invalid = validate_country_field(customers, "customers", "country")

    all_invalid_rows = (
        orders_invalid
        + customers_invalid
        + products_invalid
        + categories_invalid
        + details_invalid
        + orders_country_invalid
        + customers_country_invalid
    )

    orders_with_customers = orders.merge(
        customers[["customerID", "country", "city", "region"]].rename(columns={"country": "customer_country"}),
        on="customerID",
        how="left",
    )

    fact_orders = orders_with_customers.merge(
        order_details,
        on="orderID",
        how="left",
    )

    fact_orders = fact_orders.merge(
        products[["productID", "productName", "categoryID", "unitPrice"]].rename(columns={"unitPrice": "product_unit_price"}),
        on="productID",
        how="left",
    )

    fact_orders = fact_orders.merge(
        categories[["categoryID", "categoryName"]],
        on="categoryID",
        how="left",
    )

    fact_orders["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    fact_orders["batch_id"] = batch_id

    country_sales = (
        fact_orders.groupby("shipCountry", dropna=False, as_index=False)
        .agg(total_orders=("orderID", "nunique"), total_revenue=("quantity", lambda s: float(s.sum())))
    )

    category_sales = (
        fact_orders.groupby("categoryName", dropna=False, as_index=False)
        .agg(total_orders=("orderID", "nunique"), total_quantity=("quantity", "sum"), total_revenue=("quantity", lambda s: float(s.sum())))
    )

    cleaned_tables = {
        "orders_clean": orders,
        "customers_clean": customers,
        "products_clean": products,
        "categories_clean": categories,
        "order_details_clean": order_details,
        "fact_orders": fact_orders,
        "agg_sales_by_country": country_sales,
        "agg_sales_by_category": category_sales,
    }

    logger.info("Transformation step completed for %s tables", len(cleaned_tables))
    return cleaned_tables, all_invalid_rows, batch_id


def save_transformed_outputs(transformed_frames: dict[str, pd.DataFrame], audit_rows: list[dict], batch_id: str) -> str:
    output_dir = create_versioned_output_dir()
    save_outputs({**transformed_frames, "audit_invalid_rows": pd.DataFrame(audit_rows)}, output_dir)
    logging.getLogger("northwind_etl").info("Pipeline artifacts saved in %s", output_dir)
    return str(output_dir)


def run_pipeline() -> dict[str, pd.DataFrame]:
    logger = setup_logging()
    logger.info("Python ETL pipeline started.")
    check_input_directory()
    raw_frames = extract_raw_files()
    cleaned_tables, audit_rows, batch_id = transform_and_aggregate(raw_frames)
    output_dir = save_transformed_outputs(cleaned_tables, audit_rows, batch_id)
    load_to_postgres(cleaned_tables, audit_rows, batch_id)
    logger.info("Pipeline complete. Output is saved in %s", output_dir)
    return {**cleaned_tables, "audit_invalid_rows": pd.DataFrame(audit_rows)}


if __name__ == "__main__":
    run_pipeline()
