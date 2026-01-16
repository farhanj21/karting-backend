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
warzones_col = db['warzones']
worldrecordhistory_col = db['worldrecordhistory']

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
        'name': '2F2F Formula Karting Islamabad',
        'location': 'Islamabad, Pakistan',
        'csv_paths': [
            '2F2F-Islamabad/data_2f2f_islamabad_sr5.csv'
        ],
        'description': 'High-performance karting track in Islamabad'
    },
    {
        'name': 'Apex Autodrome',
        'location': 'Lahore, Pakistan',
        'csv_path': 'Apex Autodrome/data_apex.csv',
        'description': 'Fast-paced karting circuit in Lahore'
    },
    {
        'name': 'Omni Karting Circuit',
        'location': 'Karachi, Pakistan',
        'csv_paths': [
            'Omni Circuit/data_omni_circuit_rt8.csv',
            'Omni Circuit/data_omni_circuit_rx250.csv'
        ],
        'description': 'Premier karting circuit in Karachi'
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


def calculate_hall_of_fame(df, track_id, track_slug):
    """Calculate World Record history (Hall of Fame) for a track."""
    print("\nCalculating Hall of Fame (World Record History)...")

    # Check if track has kart types
    has_kart_types = 'Kart Type' in df.columns and df['Kart Type'].notna().any()

    hall_of_fame_records = []

    if has_kart_types:
        # Process each kart type separately
        kart_types = df[df['Kart Type'].notna()]['Kart Type'].unique()

        for kart_type in kart_types:
            kart_df = df[df['Kart Type'] == kart_type].copy()
            kart_df = kart_df.sort_values('date_obj')  # Sort by date chronologically

            current_record = 9999.0  # Start with impossibly high time
            record_holders = []

            for _, row in kart_df.iterrows():
                time = row['best_time_seconds']
                if time < current_record:
                    current_record = time
                    record_holders.append({
                        'dateBroken': row['date_obj'],
                        'driverName': row['Name'],
                        'driverSlug': create_slug(row['Name']),
                        'profileUrl': row['Profile URL'],
                        'recordTime': time,
                        'recordTimeStr': format_seconds_to_time(time),
                        'kartType': kart_type
                    })

            # Calculate days reigned for each record holder
            for i in range(len(record_holders)):
                if i < len(record_holders) - 1:
                    # Calculate days between this record and next
                    next_date = record_holders[i + 1]['dateBroken']
                    days_reigned = (next_date - record_holders[i]['dateBroken']).days
                else:
                    # Current WR holder - calculate days until today
                    days_reigned = (datetime.utcnow() - record_holders[i]['dateBroken'].replace(tzinfo=None)).days

                record_holders[i]['daysReigned'] = days_reigned
                record_holders[i]['isCurrent'] = (i == len(record_holders) - 1)

            hall_of_fame_records.extend(record_holders)
            print(f"  {kart_type}: {len(record_holders)} WR holders")

    else:
        # No kart types - calculate for entire track
        df_sorted = df.sort_values('date_obj').copy()

        current_record = 9999.0
        record_holders = []

        for _, row in df_sorted.iterrows():
            time = row['best_time_seconds']
            if time < current_record:
                current_record = time
                record_holders.append({
                    'dateBroken': row['date_obj'],
                    'driverName': row['Name'],
                    'driverSlug': create_slug(row['Name']),
                    'profileUrl': row['Profile URL'],
                    'recordTime': time,
                    'recordTimeStr': format_seconds_to_time(time),
                    'kartType': None
                })

        # Calculate days reigned
        for i in range(len(record_holders)):
            if i < len(record_holders) - 1:
                next_date = record_holders[i + 1]['dateBroken']
                days_reigned = (next_date - record_holders[i]['dateBroken']).days
            else:
                days_reigned = (datetime.utcnow() - record_holders[i]['dateBroken'].replace(tzinfo=None)).days

            record_holders[i]['daysReigned'] = days_reigned
            record_holders[i]['isCurrent'] = (i == len(record_holders) - 1)

        hall_of_fame_records = record_holders
        print(f"  Total: {len(record_holders)} WR holders")

    # Upsert to MongoDB
    if hall_of_fame_records:
        print(f"  Upserting {len(hall_of_fame_records)} Hall of Fame records...")

        # First, mark all existing records as not current
        worldrecordhistory_col.update_many(
            {'trackSlug': track_slug},
            {'$set': {'isCurrent': False}}
        )

        for record in hall_of_fame_records:
            record_doc = {
                'trackId': track_id,
                'trackSlug': track_slug,
                'driverName': record['driverName'],
                'driverSlug': record['driverSlug'],
                'profileUrl': record['profileUrl'],
                'recordTime': record['recordTime'],
                'recordTimeStr': record['recordTimeStr'],
                'kartType': record['kartType'],
                'dateBroken': record['dateBroken'],
                'daysReigned': record['daysReigned'],
                'isCurrent': record['isCurrent'],
                'updatedAt': datetime.utcnow()
            }

            filter_query = {
                'trackSlug': track_slug,
                'dateBroken': record['dateBroken'],
                'recordTime': record['recordTime']
            }
            if record['kartType']:
                filter_query['kartType'] = record['kartType']
            else:
                filter_query['kartType'] = None

            worldrecordhistory_col.update_one(
                filter_query,
                {'$set': record_doc, '$setOnInsert': {'createdAt': datetime.utcnow()}},
                upsert=True
            )

        print(f"  [OK] Hall of Fame records upserted successfully")

    return len(hall_of_fame_records)


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

    # Initialize data storage for new stats
    war_zones_data = []

    # Parse times to seconds
    df['best_time_seconds'] = df['Best Time'].apply(parse_time_to_seconds)

    # Parse dates
    df['date_obj'] = df['Date'].apply(parse_date)

    # Filter out invalid times (0 or negative) and outliers (> 1:45)
    df = df[df['best_time_seconds'] > 0]
    CUTOFF_SECONDS = 105.0  # 01:45.000 - matches lap analysis notebooks
    df = df[df['best_time_seconds'] <= CUTOFF_SECONDS]
    print(f"After filtering (< 01:45.000): {len(df)} records")

    # Calculate track-level statistics (for overall track stats)
    print("\nCalculating track statistics...")
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
    has_kart_types = 'Kart Type' in df.columns and df['Kart Type'].notna().any()
    if has_kart_types:
        available_kart_types = sorted(df['Kart Type'].dropna().unique().tolist())

    print(f"World Record: {format_seconds_to_time(world_record)} by {record_holder}")
    print(f"Total Drivers: {total_drivers}")
    print(f"Median: {format_seconds_to_time(median_time)}")
    if available_kart_types:
        print(f"Available Kart Types: {', '.join(available_kart_types)}")

    # Calculate z-scores and tiers PER KART TYPE
    print("\nCalculating tiers per kart type...")

    # Initialize columns
    df['z_score'] = 0.0
    df['tier'] = 'D'
    df['percentile'] = 0.0

    if has_kart_types:
        # Process each kart type separately
        for kart_type in available_kart_types:
            kart_mask = df['Kart Type'] == kart_type
            kart_df = df[kart_mask]

            if len(kart_df) == 0:
                continue

            # Calculate statistics for this kart type
            kart_mean = kart_df['best_time_seconds'].mean()
            kart_std = kart_df['best_time_seconds'].std()
            kart_count = len(kart_df)

            print(f"\n  {kart_type}:")
            print(f"    Drivers: {kart_count}")
            print(f"    Mean: {format_seconds_to_time(kart_mean)}")
            print(f"    Std Dev: {kart_std:.3f}s")

            # Calculate z-scores and tiers for this kart type
            df.loc[kart_mask, 'z_score'] = kart_df['best_time_seconds'].apply(
                lambda t: calculate_z_score(t, kart_mean, kart_std)
            )
            df.loc[kart_mask, 'tier'] = df.loc[kart_mask, 'z_score'].apply(assign_tier)

            # Calculate percentiles within this kart type
            # Reset position to be within kart type
            kart_df_sorted = kart_df.sort_values('best_time_seconds').reset_index(drop=True)
            kart_df_sorted['kart_position'] = range(1, len(kart_df_sorted) + 1)

            for idx in kart_df_sorted.index:
                original_idx = kart_df_sorted.loc[idx, 'index'] if 'index' in kart_df_sorted.columns else kart_df_sorted.iloc[idx].name
                kart_position = kart_df_sorted.loc[idx, 'kart_position']
                percentile = calculate_percentile(kart_position, kart_count)
                df.loc[df.index == original_idx, 'percentile'] = percentile

            # Print tier distribution for this kart type
            tier_counts = df.loc[kart_mask, 'tier'].value_counts().sort_index()
            print(f"    Tier Distribution:")
            for tier, count in tier_counts.items():
                percentage = (count / kart_count) * 100
                print(f"      {tier}: {count:4d} drivers ({percentage:5.2f}%)")

            # Calculate War Zone for this kart type
            kart_df['time_bucket'] = (kart_df['best_time_seconds'] * 10).round() / 10
            bucket_counts = kart_df['time_bucket'].value_counts()
            if len(bucket_counts) > 0:
                war_zone_time = bucket_counts.idxmax()
                war_zone_count = int(bucket_counts.max())
                war_zones_data.append({
                    'kartType': kart_type,
                    'timeStart': war_zone_time,
                    'timeEnd': war_zone_time + 0.1,
                    'driverCount': war_zone_count
                })
                print(f"    War Zone: {format_seconds_to_time(war_zone_time)} - {format_seconds_to_time(war_zone_time + 0.1)} ({war_zone_count} drivers)")
    else:
        # No kart types - calculate for entire track
        mean_time = df['best_time_seconds'].mean()
        std_dev = df['best_time_seconds'].std()

        print(f"Mean: {format_seconds_to_time(mean_time)}")
        print(f"Std Dev: {std_dev:.3f}s")

        df['z_score'] = df['best_time_seconds'].apply(
            lambda t: calculate_z_score(t, mean_time, std_dev)
        )
        df['tier'] = df['z_score'].apply(assign_tier)

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

        # Calculate War Zone for entire track (no kart types)
        df['time_bucket'] = (df['best_time_seconds'] * 10).round() / 10
        bucket_counts = df['time_bucket'].value_counts()
        if len(bucket_counts) > 0:
            war_zone_time = bucket_counts.idxmax()
            war_zone_count = int(bucket_counts.max())
            war_zones_data.append({
                'kartType': None,
                'timeStart': war_zone_time,
                'timeEnd': war_zone_time + 0.1,
                'driverCount': war_zone_count
            })
            print(f"\nWar Zone: {format_seconds_to_time(war_zone_time)} - {format_seconds_to_time(war_zone_time + 0.1)} ({war_zone_count} drivers)")

    # Calculate gaps and intervals (these remain track-level)
    df['gap_to_p1'] = df['best_time_seconds'] - world_record
    df['interval'] = df['best_time_seconds'].diff().fillna(0)

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

    # Upsert War Zone data
    print(f"\nUpserting {len(war_zones_data)} war zone(s)...")
    for wz_data in war_zones_data:
        wz_doc = {
            'trackId': track_id,
            'trackSlug': track_slug,
            'kartType': wz_data['kartType'],
            'timeStart': wz_data['timeStart'],
            'timeEnd': wz_data['timeEnd'],
            'driverCount': wz_data['driverCount'],
            'updatedAt': datetime.utcnow()
        }

        filter_query = {'trackSlug': track_slug}
        if wz_data['kartType']:
            filter_query['kartType'] = wz_data['kartType']
        else:
            filter_query['kartType'] = None

        warzones_col.update_one(
            filter_query,
            {'$set': wz_doc, '$setOnInsert': {'createdAt': datetime.utcnow()}},
            upsert=True
        )
    print(f"[OK] War zones upserted successfully")

    # Calculate and store Hall of Fame
    hall_of_fame_count = calculate_hall_of_fame(df, track_id, track_slug)

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

        # Helper function to safely convert to numeric
        def safe_int(value):
            if pd.isna(value):
                return None
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None

        def safe_float(value):
            if pd.isna(value):
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

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
            'maxKmh': safe_int(row['Max km/h']),
            'maxG': safe_float(row['Max G']),
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
            'maxKmh': safe_int(row['Max km/h']),
            'maxG': safe_float(row['Max G']),
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
        print(f"  [OK] Lap records: {result.upserted_count} inserted, {result.modified_count} updated")

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
        print(f"  [OK] Updated {drivers_processed} driver documents")

    print(f"\n[OK] Track sync complete!")
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
            print("[OK] Old index dropped")
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

    # War Zone indexes
    warzones_col.create_index([('trackSlug', ASCENDING), ('kartType', ASCENDING)], unique=True)
    warzones_col.create_index([('trackId', ASCENDING)])

    # World Record History indexes
    worldrecordhistory_col.create_index([('trackSlug', ASCENDING), ('kartType', ASCENDING), ('dateBroken', ASCENDING)])
    worldrecordhistory_col.create_index([('trackSlug', ASCENDING), ('kartType', ASCENDING), ('isCurrent', ASCENDING)])
    worldrecordhistory_col.create_index([('trackId', ASCENDING)])

    print("[OK] Indexes created successfully!")


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
        print(f"[OK] {result['track']}")
        print(f"    Drivers: {result['drivers']}")
        print(f"    Records: {result['records']}")

    print(f"\nTotal: {total_drivers} drivers, {total_records} lap records")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n[OK] All tracks synced successfully to MongoDB Atlas!")

    # Close connection
    client.close()


if __name__ == '__main__':
    main()
