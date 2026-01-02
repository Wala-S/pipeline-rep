import os
import csv
import logging
import time
import psycopg2
from psycopg2 import sql
from google.cloud import storage
import functions_framework

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

@functions_framework.cloud_event
def main(cloudevent):
    data = cloudevent.data or {}
    bucket_name = os.environ.get("BUCKET_NAME", data.get("bucket"))
    file_name = data.get("name")
    logging.info("Event received: bucket=%s file=%s", bucket_name, file_name)

    PROJECT = os.environ.get("GCP_PROJECT", "pipeline-test-483100")
    REGION = os.environ.get("DB_REGION", "us-central1")
    DB_INSTANCE = os.environ.get("DB_INSTANCE", "comp-db")
    DB_NAME = os.environ.get("DB_NAME", "comp-db")
    DB_USER = os.environ.get("DB_USER", "postgres")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "1234567")
    SCHEMA = os.environ.get("SCHEMA_NAME", "company")

    cloudsql_socket = f"/cloudsql/{PROJECT}:{REGION}:{DB_INSTANCE}"
    logging.info("Attempting DB connect via socket: %s", cloudsql_socket)

    # Download CSV
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        logging.error("Blob does not exist: gs://%s/%s", bucket_name, file_name)
        return

    try:
        csv_text = blob.download_as_text()
    except Exception:
        raw = blob.download_as_bytes()
        csv_text = raw.decode("utf-8", errors="replace")
        logging.warning("Downloaded bytes and decoded with replacement for gs://%s/%s", bucket_name, file_name)

    reader = csv.DictReader(csv_text.splitlines())
    rows = list(reader)
    logging.info("Parsed CSV rows=%d columns=%s", len(rows), reader.fieldnames)

    # Connect to Cloud SQL via Unix socket
    try:
        conn = psycopg2.connect(host=cloudsql_socket, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()
        logging.info("Connected to Cloud SQL instance %s", DB_INSTANCE)
    except Exception as e:
        logging.exception("DB connection failed: %s", e)
        return

    # Insert rows into qualified table company.employee
    rows_inserted = 0
    for idx, row in enumerate(rows, start=1):
        columns = [c.strip() for c in row.keys()]
        values = [row[col].strip() if isinstance(row[col], str) else row[col] for col in columns]

        if all(v == "" or v is None for v in values):
            logging.info("Skipping empty row %d", idx)
            continue

        placeholders = sql.SQL(", ").join([sql.Placeholder() for _ in columns])
        insert_stmt = sql.SQL("INSERT INTO {schema}.employee ({fields}) VALUES ({placeholders})").format(
            schema=sql.Identifier(SCHEMA),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=placeholders
        )

        try:
            cur.execute("SAVEPOINT sp_row")
            cur.execute(insert_stmt, values)
            cur.execute("RELEASE SAVEPOINT sp_row")
            rows_inserted += 1
        except Exception as e:
            try:
                sql_text = insert_stmt.as_string(conn)
            except Exception:
                sql_text = str(insert_stmt)
            logging.error("Insert failed for row %d: SQL=%s", idx, sql_text)
            logging.error("Values: %s", values)
            logging.exception("Row %d error: %s", idx, e)
            try:
                cur.execute("ROLLBACK TO SAVEPOINT sp_row")
            except Exception:
                conn.rollback()
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(1)
                try:
                    conn = psycopg2.connect(host=cloudsql_socket, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
                    cur = conn.cursor()
                except Exception:
                    logging.exception("Reconnection failed; stopping processing")
                    break

    try:
        conn.commit()
        logging.info("Committed transaction. Total rows inserted: %d", rows_inserted)
    except Exception as e:
        logging.exception("Commit failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    logging.info("Done processing file %s from bucket %s. Rows inserted: %d", file_name, bucket_name, rows_inserted)
