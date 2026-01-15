"""
MongoDB Sync Script for Karting Lap Time Analysis
Reads CSV files from RaceFacer scraper and syncs to MongoDB Atlas.
Calculates tiers, percentiles, gaps, and track statistics.
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path to import calculations
sys.path.append(str(Path(__file__).parent))
from calculations import (
    parse_time_to_seconds,
    format_seconds_to_time,
    calculate_z_score,
    assign_tier,
    calculate_percentile,
    parse_date,
    create_slug
)

# Load environment variables
load_dotenv()

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI')
if not MONGODB_URI:
    print("Error: MONGODB_URI not found in environment variables")
    print("Please create a .env file with your MongoDB connection string")
    sys.exit(1)

# Validate and sanitize URI
MONGODB_URI = MONGODB_URI.strip()  # Remove any leading/trailing whitespace
if not MONGODB_URI.startswith(('mongodb://', 'mongodb+srv://')):
    print("Error: Invalid MongoDB URI format")
    print(f"URI must start with 'mongodb://' or 'mongodb+srv://'")
    print(f"Current URI starts with: '{MONGODB_URI[:20]}...'")
    sys.exit(1)

print("Connecting to MongoDB...")
print(f"URI scheme: {MONGODB_URI.split('://')[0]}://")
client = MongoClient(MONGODB_URI)
db = client['karting-analysis']
tracks_col = db['tracks']
drivers_col = db['drivers']
records_col = db['laprecords']

print("Connected successfully!")

# Track data configuration
TRACKS_DATA = [
    {
        'name': 'Sportzilla Formula Karting',
        'location': 'Lahore, Pakistan',
        'csv_paths': [
            'Sportzilla/data_sportzilla_sprint_karts.csv',
            'Sportzilla/data_sportzilla_championship_karts.csv',
            'Sportzilla/data_sportzilla_pro_karts.csv'
        ],
        'description': 'Premier karting track in Lahore with technical layout'
    },
    {
        'name': '2F2F Formula Karting',
        'location': 'Lahore, Pakistan',
        'csv_paths': [
            '2F2F-Lahore/data_2f2f_rx8.csv',
            '2F2F-Lahore/data_2f2f_sr5.csv'
        ],
        'description': 'High-performance karting track in Lahore'
    },
    {
        'name': 'Apex Autodrome',
        'location': 'Lahore, Pakistan',
        'csv_path': 'Apex Autodrome/data_apex.csv',
        'description': 'Fast-paced karting circuit in Lahore'
    }
]


def clean_data(df):
    """Clean and prepare DataFrame."""
    # Remove empty rows
    df = df.dropna(subset=['Name', 'Best Time'])

    # Strip whitespace from string columns
    string_columns = ['Name', 'Best Time', 'Profile URL']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def sync_track(track_info):
    """Sync a single track's data to MongoDB."""
    print(f"\n{'='*60}")
    print(f"Processing: {track_info['name']}")
    print(f"{'='*60}")

    # Handle both single csv_path and multiple csv_paths
    if 'csv_paths' in track_info:
        # Multiple CSV files (e.g., different kart types)
        dfs = []
        for csv_rel_path in track_info['csv_paths']:
            csv_path = Path(__file__).parent.parent / csv_rel_path
            if not csv_path.exists():
                print(f"Warning: CSV file not found at {csv_path}, skipping...")
                continue
            print(f"Reading CSV from: {csv_path}")
            df_temp = pd.read_csv(csv_path)
            dfs.append(df_temp)

        if not dfs:
            print(f"Error: No valid CSV files found for {track_info['name']}")
            return

        # Combine all dataframes
        df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(df)} total records from {len(dfs)} CSV files")
    else:
        # Single CSV file (backward compatibility for Apex)
        csv_path = Path(__file__).parent.parent / track_info['csv_path']
        if not csv_path.exists():
            print(f"Error: CSV file not found at {csv_path}")
            return

        print(f"Reading CSV from: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} records")

    # Clean data
    df = clean_data(df)
    print(f"After cleaning: {len(df)} records")

    # Create slug for track
    track_slug = create_slug(track_info['name'])

    # Parse times to seconds
    df['best_time_seconds'] = df['Best Time'].apply(parse_time_to_seconds)

    # Parse dates
    df['date_obj'] = df['Date'].apply(parse_date)

    # Filter out invalid times (0 or negative) and outliers (> 1:45)
    df = df[df['best_time_seconds'] > 0]
    CUTOFF_SECONDS = 105.0  # 01:45.000 - matches lap analysis notebooks
    df = df[df['best_time_seconds'] <= CUTOFF_SECONDS]
    print(f"After filtering (< 01:45.000): {len(df)} records")

    # Calculate statistics
    print("\nCalculating statistics...")
    mean_time = df['best_time_seconds'].mean()
    std_dev = df['best_time_seconds'].std()
    median_time = df['best_time_seconds'].median()
    world_record = df['best_time_seconds'].min()
    slowest_time = df['best_time_seconds'].max()
    total_drivers = len(df)

    # Calculate percentiles
    top_1_percent_time = df['best_time_seconds'].quantile(0.01)
    top_5_percent_time = df['best_time_seconds'].quantile(0.05)
    top_10_percent_time = df['best_time_seconds'].quantile(0.10)

    # Find most common time (mode with binning)
    time_bins = pd.cut(df['best_time_seconds'], bins=20)
    mode_bin = time_bins.value_counts().idxmax()
    meta_time = (mode_bin.left + mode_bin.right) / 2

    # Find record holder
    record_row = df.loc[df['best_time_seconds'].idxmin()]
    record_holder = record_row['Name']
    record_holder_slug = create_slug(record_holder)

    # Get available kart types for this track (if any)
    available_kart_types = []
    if 'Kart Type' in df.columns:
        available_kart_types = sorted(df['Kart Type'].dropna().unique().tolist())

    print(f"World Record: {format_seconds_to_time(world_record)} by {record_holder}")
    print(f"Total Drivers: {total_drivers}")
    print(f"Mean: {format_seconds_to_time(mean_time)}")
    print(f"Median: {format_seconds_to_time(median_time)}")
    print(f"Std Dev: {std_dev:.3f}s")
    if available_kart_types:
        print(f"Available Kart Types: {', '.join(available_kart_types)}")

    # Calculate z-scores and tiers
    print("\nCalculating tiers...")
    df['z_score'] = df['best_time_seconds'].apply(
        lambda t: calculate_z_score(t, mean_time, std_dev)
    )
    df['tier'] = df['z_score'].apply(assign_tier)

    # Calculate gaps and intervals
    df['gap_to_p1'] = df['best_time_seconds'] - world_record
    df['interval'] = df['best_time_seconds'].diff().fillna(0)

    # Calculate percentiles
    df['percentile'] = df.apply(
        lambda row: calculate_percentile(row['Position'], total_drivers),
        axis=1
    )

    # Print tier distribution
    tier_counts = df['tier'].value_counts().sort_index()
    print("\nTier Distribution:")
    for tier, count in tier_counts.items():
        percentage = (count / total_drivers) * 100
        print(f"  {tier}: {count:4d} drivers ({percentage:5.2f}%)")

    # Upsert track document
    print(f"\nUpserting track document...")
    track_doc = {
        'name': track_info['name'],
        'slug': track_slug,
        'location': track_info['location'],
        'description': track_info.get('description'),
        'kartTypes': available_kart_types,  # Add available kart types
        'stats': {
            'totalDrivers': total_drivers,
            'worldRecord': world_record,
            'worldRecordStr': format_seconds_to_time(world_record),
            'recordHolder': record_holder,
            'recordHolderSlug': record_holder_slug,
            'top1Percent': top_1_percent_time,
            'top5Percent': top_5_percent_time,
            'top10Percent': top_10_percent_time,
            'median': median_time,
            'slowest': slowest_time,
            'metaTime': meta_time,
            'lastUpdated': datetime.utcnow()
        },
        'updatedAt': datetime.utcnow()
    }

    result = tracks_col.update_one(
        {'slug': track_slug},
        {'$set': track_doc, '$setOnInsert': {'createdAt': datetime.utcnow()}},
        upsert=True
    )

    if result.upserted_id:
        print(f"Created new track document")
        track_id = result.upserted_id
    else:
        print(f"Updated existing track document")
        track_id = tracks_col.find_one({'slug': track_slug})['_id']

    # Process drivers using bulk operations for performance
    print(f"\nProcessing {len(df)} drivers with bulk operations...")

    # Prepare bulk operations for lap records
    lap_record_ops = []
    driver_data = {}  # Store driver info keyed by slug

    for idx, row in df.iterrows():
        driver_name = row['Name']
        driver_slug = create_slug(driver_name)
        profile_url = row['Profile URL']
        kart_type = row.get('Kart Type')

        # Create lap record document
        lap_record = {
            'trackId': track_id,
            'trackName': track_info['name'],
            'trackSlug': track_slug,
            'driverName': driver_name,
            'driverSlug': driver_slug,
            'profileUrl': profile_url,
            'position': int(row['Position']),
            'bestTime': row['best_time_seconds'],
            'bestTimeStr': row['Best Time'],
            'date': row['date_obj'],
            'maxKmh': int(row['Max km/h']) if pd.notna(row['Max km/h']) else None,
            'maxG': float(row['Max G']) if pd.notna(row['Max G']) else None,
            'kartType': kart_type,
            'tier': row['tier'],
            'percentile': row['percentile'],
            'gapToP1': row['gap_to_p1'],
            'interval': row['interval'],
            'zScore': row['z_score'],
            'updatedAt': datetime.utcnow()
        }

        # Build filter query for lap record
        filter_query = {'trackSlug': track_slug, 'driverSlug': driver_slug}
        if kart_type:
            filter_query['kartType'] = kart_type

        # Add to bulk operations
        lap_record_ops.append(
            UpdateOne(
                filter_query,
                {'$set': lap_record, '$setOnInsert': {'createdAt': datetime.utcnow()}},
                upsert=True
            )
        )

        # Store driver record data
        driver_record = {
            'trackId': track_id,
            'trackName': track_info['name'],
            'trackSlug': track_slug,
            'position': int(row['Position']),
            'bestTime': row['best_time_seconds'],
            'bestTimeStr': row['Best Time'],
            'date': row['date_obj'],
            'maxKmh': int(row['Max km/h']) if pd.notna(row['Max km/h']) else None,
            'maxG': float(row['Max G']) if pd.notna(row['Max G']) else None,
            'kartType': kart_type,
            'tier': row['tier'],
            'percentile': row['percentile'],
            'gapToP1': row['gap_to_p1'],
            'interval': row['interval']
        }

        # Group records by driver slug
        if driver_slug not in driver_data:
            driver_data[driver_slug] = {
                'name': driver_name,
                'profileUrl': profile_url,
                'records': []
            }
        driver_data[driver_slug]['records'].append(driver_record)

    # Execute bulk lap record operations
    print(f"  Upserting {len(lap_record_ops)} lap records...")
    records_created = 0
    if lap_record_ops:
        result = records_col.bulk_write(lap_record_ops, ordered=False)
        records_created = result.upserted_count + result.modified_count
        print(f"  ✓ Lap records: {result.upserted_count} inserted, {result.modified_count} updated")

    # Process driver documents in batches
    print(f"  Processing {len(driver_data)} unique drivers...")
    drivers_processed = 0
    driver_ops_pull = []
    driver_ops_push = []

    for driver_slug, driver_info in driver_data.items():
        # First operation: upsert driver and pull old records for this track
        for record in driver_info['records']:
            pull_filter = {
                'trackSlug': track_slug,
                'kartType': record['kartType']
            }

            driver_ops_pull.append(
                UpdateOne(
                    {'slug': driver_slug},
                    {
                        '$set': {
                            'name': driver_info['name'],
                            'slug': driver_slug,
                            'profileUrl': driver_info['profileUrl'],
                            'updatedAt': datetime.utcnow()
                        },
                        '$setOnInsert': {'createdAt': datetime.utcnow()},
                        '$pull': {'records': pull_filter}
                    },
                    upsert=True
                )
            )

        # Second operation: push new records
        driver_ops_push.append(
            UpdateOne(
                {'slug': driver_slug},
                {'$push': {'records': {'$each': driver_info['records']}}}
            )
        )

    # Execute driver bulk operations
    if driver_ops_pull:
        print(f"  Removing old records for {len(driver_ops_pull)} driver-track combinations...")
        drivers_col.bulk_write(driver_ops_pull, ordered=False)

    if driver_ops_push:
        print(f"  Adding new records for {len(driver_ops_push)} drivers...")
        result = drivers_col.bulk_write(driver_ops_push, ordered=False)
        drivers_processed = result.modified_count
        print(f"  ✓ Updated {drivers_processed} driver documents")

    print(f"\n✓ Track sync complete!")
    print(f"  - Drivers processed: {drivers_processed}")
    print(f"  - Lap records created/updated: {records_created}")

    return {
        'track': track_info['name'],
        'drivers': drivers_processed,
        'records': records_created
    }


def create_indexes():
    """Create database indexes for efficient querying."""
    print("\nCreating database indexes...")

    # Track indexes
    tracks_col.create_index([('slug', ASCENDING)], unique=True)

    # Driver indexes
    drivers_col.create_index([('slug', ASCENDING)], unique=True)
    drivers_col.create_index([('profileUrl', ASCENDING)])

    # Drop old lap record index that doesn't include kartType
    try:
        print("Checking for old index without kartType...")
        existing_indexes = records_col.index_information()
        if 'trackSlug_1_driverSlug_1' in existing_indexes:
            print("Dropping old index 'trackSlug_1_driverSlug_1'...")
            records_col.drop_index('trackSlug_1_driverSlug_1')
            print("✓ Old index dropped")
    except Exception as e:
        print(f"Note: Could not drop old index (may not exist): {e}")

    # Lap record indexes
    records_col.create_index([('trackId', ASCENDING), ('position', ASCENDING)])
    records_col.create_index([('trackSlug', ASCENDING), ('position', ASCENDING)])
    records_col.create_index([('driverSlug', ASCENDING)])
    # Unique constraint includes kartType (allows same driver on different kart types)
    records_col.create_index(
        [('trackSlug', ASCENDING), ('driverSlug', ASCENDING), ('kartType', ASCENDING)],
        unique=True
    )
    records_col.create_index([('tier', ASCENDING), ('trackId', ASCENDING)])
    records_col.create_index([('trackSlug', ASCENDING), ('kartType', ASCENDING)])  # Index for kart type filtering
    records_col.create_index([('kartType', ASCENDING)])

    print("✓ Indexes created successfully!")


def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("KARTING LAP TIME ANALYSIS - MONGODB SYNC")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Create indexes
    create_indexes()

    # Sync each track
    results = []
    for track_info in TRACKS_DATA:
        try:
            result = sync_track(track_info)
            results.append(result)
        except Exception as e:
            print(f"\nError processing {track_info['name']}: {e}")
            import traceback
            traceback.print_exc()

    # Print summary
    print("\n" + "="*60)
    print("SYNC COMPLETE - SUMMARY")
    print("="*60)
    total_drivers = sum(r['drivers'] for r in results)
    total_records = sum(r['records'] for r in results)

    for result in results:
        print(f"✓ {result['track']}")
        print(f"    Drivers: {result['drivers']}")
        print(f"    Records: {result['records']}")

    print(f"\nTotal: {total_drivers} drivers, {total_records} lap records")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n✓ All tracks synced successfully to MongoDB Atlas!")

    # Close connection
    client.close()


if __name__ == '__main__':
    main()
