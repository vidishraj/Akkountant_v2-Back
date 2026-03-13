#!/usr/bin/env python3
"""
One-time migration: Drop and recreate the processedEmails table with full schema.

The old table only had 3 columns (gmail_id, user_id, processed_at) with 91 rows
that have no useful metadata. This recreates it with all columns from the model.

Usage:
    python migrate_processed_emails.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in environment")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Check current state
    try:
        result = conn.execute(text("SELECT COUNT(*) FROM processed_emails"))
        count = result.scalar()
        print(f"Found 'processed_emails' table with {count} rows")
        table_name = "processed_emails"
    except Exception:
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM processedEmails"))
            count = result.scalar()
            print(f"Found 'processedEmails' table with {count} rows")
            table_name = "processedEmails"
        except Exception:
            print("No existing processedEmails table found, will create fresh")
            table_name = None

    # Drop old table
    if table_name:
        print(f"Dropping table '{table_name}'...")
        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        conn.commit()
        print("Dropped.")

    # Also drop the other variant just in case
    conn.execute(text("DROP TABLE IF EXISTS `processed_emails`"))
    conn.execute(text("DROP TABLE IF EXISTS `processedEmails`"))
    conn.commit()

    # Create new table with full schema
    print("Creating 'processedEmails' table with full schema...")
    conn.execute(text("""
        CREATE TABLE `processedEmails` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `gmail_id` VARCHAR(200) NOT NULL,
            `user_id` VARCHAR(100) NOT NULL,
            `sender` VARCHAR(300) DEFAULT NULL,
            `subject` VARCHAR(500) DEFAULT NULL,
            `email_date` DATETIME DEFAULT NULL,
            `category` VARCHAR(50) DEFAULT NULL,
            `processing_type` VARCHAR(20) DEFAULT NULL,
            `status` VARCHAR(20) NOT NULL DEFAULT 'processed',
            `items_extracted` INT DEFAULT 0,
            `extraction_summary` JSON DEFAULT NULL,
            `error_message` TEXT DEFAULT NULL,
            `pdf_filename` VARCHAR(500) DEFAULT NULL,
            `processed_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_user_gmail` (`user_id`, `gmail_id`),
            KEY `idx_pe_user_gmail` (`user_id`, `gmail_id`),
            KEY `idx_pe_user_date` (`user_id`, `processed_at`),
            CONSTRAINT `fk_pe_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`userID`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """))
    conn.commit()
    print("Table 'processedEmails' created successfully!")

    # Verify
    result = conn.execute(text("DESCRIBE `processedEmails`"))
    print("\nColumns:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
