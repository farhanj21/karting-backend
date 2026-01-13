# MongoDB Sync Script for Karting Analysis

This directory contains scripts to sync karting lap time data from CSV files to MongoDB Atlas.

## Setup

### 1. Install Dependencies

```bash
cd scraper
pip install -r requirements.txt
```

### 2. Configure MongoDB Connection

1. Create a MongoDB Atlas account at https://www.mongodb.com/cloud/atlas
2. Create a new cluster (free tier is fine)
3. Create a database user with read/write permissions
4. Get your connection string
5. Create a `.env` file in this directory:

```bash
cp .env.example .env
```

6. Edit `.env` and add your MongoDB connection string:

```
MONGODB_URI=mongodb+srv://your-username:your-password@your-cluster.mongodb.net/karting-analysis?retryWrites=true&w=majority
```

## Usage

### Manual Sync

Run the sync script manually to sync CSV data to MongoDB:

```bash
python sync_to_mongodb.py
```

This will:
1. Connect to MongoDB Atlas
2. Read CSV files from `Sportzilla/data_sportzilla.csv` and `Apex Autodrome/data_apex.csv`
3. Calculate tiers, percentiles, gaps, and statistics
4. Upsert data into MongoDB collections: `tracks`, `drivers`, `laprecords`

### Scheduled Sync (GitHub Actions)

The scraper can run automatically via GitHub Actions:

1. Go to your GitHub repository settings
2. Navigate to Secrets and Variables > Actions
3. Add a new secret named `MONGODB_URI` with your connection string
4. The workflow will run every 6 hours automatically
5. You can also trigger it manually from the Actions tab

## Database Schema

### Collections

- **tracks**: Track information and statistics
  - name, slug, location, stats (world record, percentiles, etc.)

- **drivers**: Driver profiles with records across tracks
  - name, slug, profileUrl, records array

- **laprecords**: Individual lap records (optimized for queries)
  - trackId, driverId, position, time, tier, percentile, gaps

## Calculations Module

The `calculations.py` module provides helper functions:

- `parse_time_to_seconds()`: Convert "MM:SS.ms" to seconds
- `format_seconds_to_time()`: Convert seconds to "MM:SS.ms"
- `calculate_z_score()`: Calculate z-score for tier assignment
- `assign_tier()`: Assign tier (S+, S, A, B, C, D) based on z-score
- `calculate_percentile()`: Calculate percentile rank
- `create_slug()`: Create URL-safe slugs

## Tier System

Tiers are assigned based on z-scores:

| Tier | Name | Z-Score Range | Description |
|------|------|---------------|-------------|
| S+ | Alien | < -1.5 | Exceptional, world-class |
| S | Elite | -1.5 to -1.0 | Elite performance |
| A | Pro | -1.0 to -0.5 | Professional level |
| B | Above Average | -0.5 to 0.0 | Above average |
| C | Average | 0.0 to 0.5 | Average performance |
| D | Rookie | ≥ 0.5 | Beginner/Rookie |

## Testing

Test the calculations module:

```bash
python calculations.py
```

This will run doctests and print example conversions.

## Troubleshooting

**Connection Error:**
- Verify your MONGODB_URI is correct
- Check that your IP is whitelisted in MongoDB Atlas (or allow from anywhere)
- Ensure database user has proper permissions

**CSV Not Found:**
- Ensure you're running from the repository root or scraper directory
- Check that CSV files exist at the expected paths

**Import Error:**
- Make sure all dependencies are installed: `pip install -r requirements.txt`
