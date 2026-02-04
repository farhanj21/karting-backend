# Pakistan Karting Lap Time Analysis 🏎️

### Last Data Sync: 3rd February 2026

**Pakistan Karting Lap Time Analysis** is a comprehensive data-analysis system for karting lap times from Pakistan's premier tracks. The system consists of Python-based data scraping and analysis tools, plus a modern Next.js dashboard for interactive visualization and leaderboards.

**Coverage**: 5 major tracks across Lahore, Islamabad, and Karachi with 9 different kart types and 19,200+ driver records.

## 🚀 System Architecture

```
RaceFacer.com → Python Scraper → MongoDB Atlas → Next.js Dashboard
```

## 📂 Project Structure

### Analysis & Scraping

- **Sportzilla/** — Sportzilla Formula Karting (Lahore)
  - `data_sportzilla_sprint_karts.csv` — Sprint Karts data 
  - `data_sportzilla_championship_karts.csv` — Championship Karts data 
  - `data_sportzilla_pro_karts.csv` — Pro Karts data 
  - `sportzilla_scrapper.ipynb` — Selenium-based web scraper
  - `lap-analysis-sportzilla.ipynb` — Statistical analysis notebook

- **Apex Autodrome/** — Apex Autodrome (Lahore)
  - `data_apex.csv` — Current lap time data 
  - `apex_autodrome_scrapper.ipynb` — Selenium-based web scraper
  - `lap-analysis-apex.ipynb` — Statistical analysis notebook

- **2F2F-Lahore/** — 2F2F Formula Karting (Lahore)
  - `data_2f2f_rx8.csv` — RX8 Karts data 
  - `data_2f2f_sr5.csv` — SR5 Karts data 
  - `2f2f_scrapper.ipynb` — Selenium-based web scraper

- **2F2F-Islamabad/** — 2F2F Formula Karting (Islamabad)
  - `data_2f2f_islamabad_sr5.csv` — SR5 Karts data 
  - `2f2f_islamabad_scrapper.ipynb` — Selenium-based web scraper

- **Omni Circuit/** — Omni Karting Circuit (Karachi)
  - `data_omni_circuit_rt8.csv` — RT8 Karts data 
  - `data_omni_circuit_rx250.csv` — RX250 Karts data 

### MongoDB Integration
- **sync/** — MongoDB synchronization module
  - `sync_to_mongodb.py` — Main sync script (CSV → MongoDB)
  - `calculations.py` — Statistical calculations (z-scores, tiers, percentiles)
  - `requirements.txt` — Python dependencies
  - `.env.example` — Environment variable template
  - `README.md` — Setup and usage instructions

### Automation
- **.github/workflows/** — GitHub Actions
  - `scrape-and-sync.yml` — Automatic MongoDB sync when CSV files are updated
- **scrape_local.md** — Instructions for updating data locally

## ✅ Features

### Analysis Features
- **Multi-Track Support**: 5 major tracks across Pakistan (Lahore, Islamabad, Karachi)
- **Multiple Kart Types**: 9 different kart types with separate tier classifications
  - Sprint, Championship, Pro Karts (Sportzilla)
  - RX8, SR5 Karts (2F2F Lahore & Islamabad)
  - RT8, RX250 Karts (Omni Circuit)
- **Tier Classification**: S+ to D tier system based on z-score analysis
- **Statistical Insights**: Percentiles, gaps, intervals, and battle zones
- **World Record History (Hall of Fame)**: Tracks record progression, days reigned, and all-time champions
- **War Zone Detection**: Identifies most competitive time clusters per kart type
- **Per-Kart-Type Statistics**: Separate calculations for each kart type on multi-type tracks
- **Historical Tracking**: Record progression and performance evolution

### Dashboard Features (karting-dashboard repo)
- **Interactive Leaderboards**: Real-time rankings with search and filters
- **Multi-Track View**: Compare performance across different tracks and kart types
- **Data Visualizations**: Time distribution histograms and tier charts
- **Track Statistics**: World records, percentiles, median times per kart type
- **Hall of Fame**: World record history with record holder progression
- **Responsive Design**: Mobile-first dark racing theme
- **Real-time Updates**: Automatic data sync via GitHub Actions

## 🛠️ Setup

### Prerequisites
- Python 3.10+
- MongoDB Atlas account (free tier)
- Node.js 18+ (for dashboard)

### 1. Install Python Dependencies

```bash
pip install -r sync/requirements.txt
```

### 2. Configure MongoDB

1. Create MongoDB Atlas account and cluster
2. Get connection string
3. Create `sync/.env`:

```bash
cp sync/.env.example sync/.env
```

Edit `.env` and add your MongoDB URI:

```
MONGODB_URI= Your MongoDB connection string
```

### 3. Sync Data to MongoDB

```bash
python sync/sync_to_mongodb.py
```

This will:
- Read CSV files from all 5 tracks (9 different kart types)
- Calculate tiers, percentiles, and statistics per kart type
- Calculate World Record History (Hall of Fame)
- Detect War Zones (most competitive time clusters)
- Upload data to MongoDB Atlas with optimized bulk operations
- Create database indexes for efficient querying

### 4. Set Up Dashboard

See the [karting-dashboard](../karting-dashboard) repository for Next.js setup and deployment instructions.

## 📊 Analysis Notebooks

The Jupyter notebooks provide in-depth statistical analysis:

- **Tier System**: S+ (Alien), S (Elite), A (Pro), B (Above Average), C (Average), D (Rookie)
- **Performance Metrics**: Z-scores, percentile rankings, gaps to P1
- **Competitive Analysis**: Battle zones, war zones, photo finishes
- **Visualizations**: Histograms, KDE curves, competition curves

To run analysis:

```bash
jupyter notebook Sportzilla/lap-analysis-sportzilla.ipynb
```

## 🔄 Data Updates

### How to Update Data

1. **Scrape data locally** using Jupyter notebooks (see `scrape_local.md`)
   - Run track-specific scraper notebooks in Jupyter
   - Scrapers use Selenium to extract data from RaceFacer.com
   - Data is saved to CSV files in respective track directories

2. **Commit CSV files** to the repository
   ```bash
   git add Sportzilla/data_sportzilla_*.csv
   git add "Apex Autodrome/data_apex.csv"
   git add 2F2F-Lahore/data_2f2f_*.csv
   git add 2F2F-Islamabad/data_2f2f_islamabad_*.csv
   git add "Omni Circuit/data_omni_circuit_*.csv"
   git commit -m "Update lap time data"
   ```

3. **Push to GitHub** - Triggers automatic MongoDB sync
   ```bash
   git push
   ```

4. **GitHub Actions Workflow**
   - Detects CSV file changes
   - Runs `sync/sync_to_mongodb.py`
   - Processes all tracks and kart types
   - Updates MongoDB Atlas with new data

5. **Dashboard updates** immediately with fresh data

### Configure GitHub Secrets

Add these secrets in your repository settings:
- `MONGODB_URI`: Your MongoDB connection string

### Data Processing Features

The sync script includes:
- **Data Cleaning**: Removes empty rows, invalid times, and outliers
- **Time Filtering**: Excludes unrealistic times (> 01:45.000)
- **Per-Kart-Type Calculations**: Separate statistics for each kart type
- **Hall of Fame Tracking**: Calculates world record progression
- **War Zone Detection**: Finds most competitive time clusters
- **Bulk Operations**: Optimized MongoDB upserts for performance
- **Index Creation**: Automatic database indexing for fast queries

## 📦 Data Format & Database Schema

### CSV Structure
```csv
Position,Name,Date,Max km/h,Max G,Best Time,Profile URL,Kart Type
1,Ammar Hassan,27.12.2025,65,3.4,01:01.518,https://www.racefacer.com/...,Sprint Karts
```

### MongoDB Collections

**tracks** - Track information and statistics
- Track metadata (name, slug, location, description)
- Available kart types per track
- Track-level statistics (world records, percentiles, total drivers)
- Last updated timestamp

**drivers** - Driver profiles with multi-track records
- Driver information (name, slug, profileUrl)
- Array of all records across different tracks and kart types
- Performance history and statistics

**laprecords** - Individual lap records (optimized for queries)
- Track and driver references
- Lap time data (bestTime, bestTimeStr, date)
- Performance metrics (position, tier, percentile, zScore)
- Kart-specific data (kartType, maxKmh, maxG)
- Competition metrics (gapToP1, interval)
- Unique index on: (trackSlug, driverSlug, kartType)

**warzones** - Competitive time clusters
- Track and kart type references
- Time range (timeStart, timeEnd)
- Driver density in that time range
- Identifies "battle zones" where most drivers compete

**worldrecordhistory** - Hall of Fame
- World record progression over time
- Record holder information
- Time record was held (dateBroken, daysReigned)
- Current record flag (isCurrent)
- Track and kart type specific

### GitHub Actions Workflow

The workflow (`scrape-and-sync.yml`) automatically syncs data to MongoDB when:
- Any CSV file is pushed to the repository
- Manual workflow dispatch is triggered

**Monitored Files:**
- `Sportzilla/data_sportzilla_*.csv` (Sprint, Championship, Pro)
- `Apex Autodrome/data_apex.csv`
- `2F2F-Lahore/data_2f2f_*.csv` (RX8, SR5)
- `2F2F-Islamabad/data_2f2f_islamabad_*.csv` (SR5)
- `Omni Circuit/data_omni_circuit_*.csv` (RT8, RX250)

**Workflow Steps:**
1. Verifies CSV files exist and have data
2. Installs Python dependencies
3. Runs `sync/sync_to_mongodb.py`
4. Uploads artifacts on failure for debugging
5. Notifies success with record counts

## 🏁 Tracks & Kart Types

### Covered Tracks

| Track | Location | Kart Types | Drivers |
|-------|----------|------------|---------|
| **Sportzilla Formula Karting** | Lahore | Sprint, Championship, Pro | 6,016 |
| **Apex Autodrome** | Lahore | Single type | 3,783 |
| **2F2F Formula Karting** | Lahore | RX8, SR5 | 7,962 |
| **2F2F Formula Karting** | Islamabad | SR5 | 1,427 |
| **Omni Karting Circuit** | Karachi | RT8, RX250 | 20 |

**Total**: 5 major tracks, 9 different kart types, 19,200+ driver records

### Kart Type Breakdown

- **Sprint Karts** (Sportzilla): Most popular category with 5,885 drivers
- **Championship Karts** (Sportzilla): 113 drivers
- **Pro Karts** (Sportzilla): 18 elite drivers
- **RX8 Karts** (2F2F Lahore): 7,952 drivers
- **SR5 Karts** (2F2F Lahore & Islamabad): Combined data
- **RT8 Karts** (Omni Circuit): New category
- **RX250 Karts** (Omni Circuit): High-performance category

Each kart type has separate tier classifications and statistics to ensure fair comparison.

## 🔧 Tier Calculation

Tiers are assigned based on z-scores:

| Tier | Name | Z-Score | Description |
|------|------|---------|-------------|
| S+ | Alien | < -1.5 | Exceptional, world-class |
| S | Elite | -1.5 to -1.0 | Elite performance |
| A | Pro | -1.0 to -0.5 | Professional level |
| B | Above Average | -0.5 to 0.0 | Above average |
| C | Average | 0.0 to 0.5 | Average performance |
| D | Rookie | ≥ 0.5 | Beginner/Rookie |

## ⚙️ Technical Implementation

### Statistical Calculations (calculations.py)

**Core Functions:**
- `parse_time_to_seconds()` - Convert "MM:SS.ms" format to seconds (float)
- `format_seconds_to_time()` - Convert seconds back to readable format
- `calculate_z_score()` - Statistical distance from mean for tier assignment
- `assign_tier()` - Maps z-scores to 6-tier skill classification (S+ to D)
- `calculate_percentile()` - Driver rank percentile calculation
- `parse_date()` - Handles multiple date formats (DD.MM.YYYY, YYYY-MM-DD)
- `create_slug()` - Generates URL-safe identifiers from names
- `get_tier_color()` - Returns hex color codes for UI visualization

### Data Processing Pipeline (sync_to_mongodb.py)

**1. CSV Data Loading**
- Reads CSV files from all 5 tracks
- Handles multiple CSV files per track (different kart types)
- Concatenates data from multiple sources when needed

**2. Data Validation & Cleaning**
- Removes rows with empty/null critical fields
- Filters outlier times (> 01:45.000 / 105 seconds)
- Validates time format and parsing
- Handles missing or malformed data gracefully

**3. Statistical Processing**
- Per-kart-type mean and standard deviation
- Z-score calculation for each driver
- Tier assignment based on z-score thresholds
- Percentile ranking within kart type
- Gap to P1 (world record) calculation
- Interval calculation (time gap to next driver)

**4. Hall of Fame Calculation**
- Tracks world record progression over time
- Calculates days each record was held
- Identifies current record holder
- Stores complete record history per kart type

**5. War Zone Detection**
- Identifies time ranges with highest driver density
- Highlights most competitive performance brackets
- Helps drivers understand where competition is fiercest

**6. MongoDB Operations**
- Bulk upsert operations for performance
- Creates indexes for optimized queries
- Updates 5 collections: tracks, drivers, laprecords, warzones, worldrecordhistory
- Ensures data consistency with unique constraints

### Database Indexes

**Optimized for Performance:**
- `laprecords`: Indexed on (trackSlug, position), (driverSlug), (tier, trackId), (kartType)
- `laprecords`: Unique compound index on (trackSlug, driverSlug, kartType)
- `drivers`: Unique on slug, indexed on profileUrl
- `tracks`: Unique on slug
- `warzones`: Unique on (trackSlug, kartType)
- `worldrecordhistory`: Multiple indexes for Hall of Fame queries

### Performance Optimizations

- **Bulk Write Operations**: Reduces database round-trips by batching updates
- **Upsert Strategy**: Insert if new, update if exists - maintains data integrity
- **Efficient Indexing**: Fast lookups for common query patterns
- **Pandas Integration**: Leverages pandas for efficient CSV processing
- **Lazy Loading**: Processes data track-by-track to manage memory

## 👤 Author & Credits

Created by Farhan Jafri

Data sourced from [RaceFacer](https://www.racefacer.com)
