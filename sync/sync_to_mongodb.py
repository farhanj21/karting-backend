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
from pymongo import MongoClient, ASCENDING, DESCENDING
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

print("Connecting to MongoDB...")
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
        'csv_path': 'Sportzilla/data_sportzilla.csv',
        'description': 'Premier karting track in Lahore with technical layout'
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

    # Read CSV file
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

    # Filter out invalid times (0 or negative)
    df = df[df['best_time_seconds'] > 0]

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

    print(f"World Record: {format_seconds_to_time(world_record)} by {record_holder}")
    print(f"Total Drivers: {total_drivers}")
    print(f"Mean: {format_seconds_to_time(mean_time)}")
    print(f"Median: {format_seconds_to_time(median_time)}")
    print(f"Std Dev: {std_dev:.3f}s")

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

    # Process each driver
    print(f"\nProcessing {len(df)} drivers...")
    drivers_processed = 0
    records_created = 0

    for idx, row in df.iterrows():
        driver_name = row['Name']
        driver_slug = create_slug(driver_name)
        profile_url = row['Profile URL']

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
            'tier': row['tier'],
            'percentile': row['percentile'],
            'gapToP1': row['gap_to_p1'],
            'interval': row['interval'],
            'zScore': row['z_score'],
            'updatedAt': datetime.utcnow()
        }

        # Upsert lap record
        records_col.update_one(
            {'trackSlug': track_slug, 'driverSlug': driver_slug},
            {'$set': lap_record, '$setOnInsert': {'createdAt': datetime.utcnow()}},
            upsert=True
        )
        records_created += 1

        # Upsert driver document
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
            'tier': row['tier'],
            'percentile': row['percentile'],
            'gapToP1': row['gap_to_p1'],
            'interval': row['interval']
        }

        drivers_col.update_one(
            {'slug': driver_slug},
            {
                '$set': {
                    'name': driver_name,
                    'slug': driver_slug,
                    'profileUrl': profile_url,
                    'updatedAt': datetime.utcnow()
                },
                '$setOnInsert': {'createdAt': datetime.utcnow()},
                '$addToSet': {'records': driver_record}
            },
            upsert=True
        )
        drivers_processed += 1

        if drivers_processed % 500 == 0:
            print(f"  Processed {drivers_processed}/{len(df)} drivers...")

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

    # Lap record indexes
    records_col.create_index([('trackId', ASCENDING), ('position', ASCENDING)])
    records_col.create_index([('trackSlug', ASCENDING), ('position', ASCENDING)])
    records_col.create_index([('driverSlug', ASCENDING)])
    records_col.create_index([('trackSlug', ASCENDING), ('driverSlug', ASCENDING)], unique=True)
    records_col.create_index([('tier', ASCENDING), ('trackId', ASCENDING)])

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
