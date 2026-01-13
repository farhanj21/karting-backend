"""
Statistical calculations for karting lap time analysis.
Includes time parsing, z-score calculations, tier assignments, and percentiles.
"""

import re
from datetime import datetime
from typing import Tuple


def parse_time_to_seconds(time_str: str) -> float:
    """
    Convert lap time string to seconds.

    Args:
        time_str: Time in format "MM:SS.ms" (e.g., "01:01.518" or "00:25.026")

    Returns:
        Time in seconds as float (e.g., 61.518 or 25.026)

    Examples:
        >>> parse_time_to_seconds("01:01.518")
        61.518
        >>> parse_time_to_seconds("00:25.026")
        25.026
    """
    try:
        # Handle format MM:SS.ms
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
        else:
            # If already in seconds format
            return float(time_str)
    except (ValueError, AttributeError):
        print(f"Warning: Could not parse time '{time_str}', returning 0")
        return 0.0


def format_seconds_to_time(seconds: float) -> str:
    """
    Convert seconds to lap time string format.

    Args:
        seconds: Time in seconds (e.g., 61.518)

    Returns:
        Time in format "MM:SS.ms" (e.g., "01:01.518")

    Examples:
        >>> format_seconds_to_time(61.518)
        '01:01.518'
        >>> format_seconds_to_time(25.026)
        '00:25.026'
    """
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes:02d}:{remaining_seconds:06.3f}"


def calculate_z_score(time: float, mean: float, std_dev: float) -> float:
    """
    Calculate z-score for a given time.
    Z-score indicates how many standard deviations a time is from the mean.

    Args:
        time: Lap time in seconds
        mean: Mean lap time in seconds
        std_dev: Standard deviation of lap times

    Returns:
        Z-score (negative means faster than average)

    Examples:
        >>> calculate_z_score(60.0, 65.0, 2.0)
        -2.5
    """
    if std_dev == 0:
        return 0.0
    return (time - mean) / std_dev


def assign_tier(z_score: float) -> str:
    """
    Assign skill tier based on z-score.

    Tier System:
    - S+ (Alien): z-score < -1.5
    - S (Elite): z-score < -1.0
    - A (Pro): z-score < -0.5
    - B (Above Average): z-score < 0.0
    - C (Average): z-score < 0.5
    - D (Rookie): z-score >= 0.5

    Args:
        z_score: Z-score value (negative is better)

    Returns:
        Tier string ("S+", "S", "A", "B", "C", or "D")

    Examples:
        >>> assign_tier(-1.6)
        'S+'
        >>> assign_tier(-0.7)
        'A'
        >>> assign_tier(0.6)
        'D'
    """
    if z_score < -1.5:
        return 'S+'
    elif z_score < -1.0:
        return 'S'
    elif z_score < -0.5:
        return 'A'
    elif z_score < 0.0:
        return 'B'
    elif z_score < 0.5:
        return 'C'
    else:
        return 'D'


def calculate_percentile(position: int, total: int) -> float:
    """
    Calculate percentile rank for a driver.

    Args:
        position: Driver's position (1-indexed)
        total: Total number of drivers

    Returns:
        Percentile (e.g., 1.5 for top 1.5%)

    Examples:
        >>> calculate_percentile(1, 100)
        1.0
        >>> calculate_percentile(50, 1000)
        5.0
    """
    return (position / total) * 100


def parse_date(date_str: str) -> datetime:
    """
    Parse date string from CSV to datetime object.

    Args:
        date_str: Date in format "DD.MM.YYYY" (e.g., "27.12.2025")

    Returns:
        datetime object

    Examples:
        >>> parse_date("27.12.2025")
        datetime.datetime(2025, 12, 27, 0, 0)
    """
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        try:
            # Try alternative format
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print(f"Warning: Could not parse date '{date_str}', using current date")
            return datetime.now()


def create_slug(name: str) -> str:
    """
    Create URL-safe slug from name.

    Args:
        name: Name to convert (e.g., "Sportzilla Formula Karting")

    Returns:
        URL-safe slug (e.g., "sportzilla-formula-karting")

    Examples:
        >>> create_slug("Sportzilla Formula Karting")
        'sportzilla-formula-karting'
        >>> create_slug("Ammar Hassan")
        'ammar-hassan'
    """
    # Convert to lowercase
    slug = name.lower()
    # Replace spaces with hyphens
    slug = re.sub(r'\s+', '-', slug)
    # Remove special characters except hyphens
    slug = re.sub(r'[^a-z0-9-]', '', slug)
    # Remove duplicate hyphens
    slug = re.sub(r'-+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    return slug


def get_tier_color(tier: str) -> str:
    """
    Get color code for tier badge.

    Args:
        tier: Tier string ("S+", "S", "A", "B", "C", or "D")

    Returns:
        Hex color code
    """
    tier_colors = {
        'S+': '#a855f7',  # Purple (Alien)
        'S': '#fbbf24',   # Gold (Elite)
        'A': '#10b981',   # Green (Pro)
        'B': '#3b82f6',   # Blue (Above Average)
        'C': '#6b7280',   # Gray (Average)
        'D': '#ef4444',   # Red (Rookie)
    }
    return tier_colors.get(tier, '#6b7280')


if __name__ == '__main__':
    # Test the functions
    import doctest
    doctest.testmod()

    print("Calculations module loaded successfully!")
    print("\nExample conversions:")
    print(f"'01:01.518' -> {parse_time_to_seconds('01:01.518')} seconds")
    print(f"61.518 seconds -> '{format_seconds_to_time(61.518)}'")
    print(f"\nTier for z-score -1.6: {assign_tier(-1.6)}")
    print(f"Percentile for position 50/1000: {calculate_percentile(50, 1000)}%")
