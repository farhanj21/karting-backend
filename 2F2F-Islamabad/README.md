# 2F2F Formula Karting Islamabad - Data Scraper

This folder contains the web scraper for 2F2F Formula Karting Islamabad lap time data from RaceFacer.

## Track Information

- **Track Name**: 2F2F Formula Karting Islamabad
- **Location**: Islamabad, Pakistan
- **RaceFacer URL**: https://www.racefacer.com/en/karting-tracks/pakistan/2f2fislamabad

## Kart Types

Currently scraping data for:
- **SR5** (Kart ID: 1099)

## Files

- `2f2f_islamabad_scrapper.ipynb` - Jupyter notebook for scraping lap time data
- `data_2f2f_islamabad_sr5.csv` - Output CSV file with scraped data (generated after running scraper)

## Usage

1. Open `2f2f_islamabad_scrapper.ipynb` in Jupyter Notebook
2. Install required dependencies (first cell)
3. Run the scraper cell to fetch all-time lap records for SR5 karts
4. The data will be saved to `data_2f2f_islamabad_sr5.csv`

## Data Structure

The CSV file contains the following columns:
- Position
- Name
- Date
- Max km/h
- Max G
- Best Time
- Profile URL
- Kart Type

## Syncing to MongoDB

After scraping, run the sync script to upload data to MongoDB:
```bash
python sync/sync_to_mongodb.py
```

The sync script will automatically include 2F2F Islamabad data.
