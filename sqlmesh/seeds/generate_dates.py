#!/usr/bin/env python3
"""Generate dates.csv with all dates in a range with useful date columns."""

import csv
from datetime import datetime, timedelta
import calendar

def generate_dates_csv(
    output_file='dates.csv',
    start_date='1990-01-01',
    end_date='2050-01-01'
):
    """Generate a CSV file with all dates in a range.
    
    Args:
        output_file: Output CSV filename
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (exclusive)
    """
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = [
            'date',
            'year',
            'quarter',
            'month',
            'week',
            'day_of_year',
            'day_of_month',
            'day_of_week',
            'iso_year',
            'iso_week',
            'iso_day',
            'iso_week_date',
            'week_of_year',
            'month_name',
            'month_name_short',
            'day_name',
            'day_name_short',
            'is_weekend',
            'is_weekday',
            'is_month_start',
            'is_month_end',
            'is_quarter_start',
            'is_quarter_end',
            'is_year_start',
            'is_year_end',
            'is_leap_year',
            'days_in_month',
            'quarter_name',
            'year_month',
            'year_quarter',
            'year_week',
            'fiscal_year',
            'fiscal_quarter',
            'fiscal_month',
            'fiscal_week',
            'date_int',
            'date_iso'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        current_date = start
        total_days = 0
        
        while current_date < end:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            
            # Calculate quarter
            quarter = (month - 1) // 3 + 1
            
            # ISO calendar calculations
            iso_calendar = current_date.isocalendar()
            iso_year = iso_calendar[0]
            iso_week = iso_calendar[1]
            iso_day = iso_calendar[2]  # Monday=1, Sunday=7
            
            # ISO week date format (e.g., 2025-W01-5)
            iso_week_date = f"{iso_year:04d}-W{iso_week:02d}-{iso_day}"
            
            # Week calculations
            week_of_year = iso_week
            day_of_week = current_date.weekday()  # Monday=0, Sunday=6
            day_of_week_iso = iso_day
            
            # Day of year
            day_of_year = current_date.timetuple().tm_yday
            
            # Names
            month_name = calendar.month_name[month]
            month_name_short = calendar.month_abbr[month]
            day_name = calendar.day_name[day_of_week]
            day_name_short = calendar.day_abbr[day_of_week]
            
            # Boolean flags
            is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6
            is_weekday = not is_weekend
            
            # Month boundaries
            is_month_start = day == 1
            _, last_day = calendar.monthrange(year, month)
            is_month_end = day == last_day
            
            # Quarter boundaries
            is_quarter_start = is_month_start and month in [1, 4, 7, 10]
            is_quarter_end = is_month_end and month in [3, 6, 9, 12]
            
            # Year boundaries
            is_year_start = month == 1 and day == 1
            is_year_end = month == 12 and day == 31
            
            # Leap year
            is_leap_year = calendar.isleap(year)
            
            # Days in month
            days_in_month = last_day
            
            # Formatted strings
            quarter_name = f"Q{quarter}"
            year_month = f"{year:04d}-{month:02d}"
            year_quarter = f"{year:04d}-Q{quarter}"
            year_week = f"{year:04d}-W{week_of_year:02d}"
            
            # Fiscal year (assuming fiscal year starts July 1)
            if month >= 7:
                fiscal_year = year + 1
                fiscal_quarter = ((month - 7) // 3) + 1
                fiscal_month = month - 6
            else:
                fiscal_year = year
                fiscal_quarter = ((month + 5) // 3) + 1
                fiscal_month = month + 6
            
            fiscal_week = week_of_year if month >= 7 else week_of_year + 26
            if fiscal_week > 52:
                fiscal_week = fiscal_week - 52
            
            # Integer representation (YYYYMMDD)
            date_int = year * 10000 + month * 100 + day
            
            # ISO format
            date_iso = current_date.strftime('%Y-%m-%d')
            
            writer.writerow({
                'date': date_iso,
                'year': year,
                'quarter': quarter,
                'month': month,
                'week': week_of_year,
                'day_of_year': day_of_year,
                'day_of_month': day,
                'day_of_week': day_of_week_iso,
                'iso_year': iso_year,
                'iso_week': iso_week,
                'iso_day': iso_day,
                'iso_week_date': iso_week_date,
                'week_of_year': week_of_year,
                'month_name': month_name,
                'month_name_short': month_name_short,
                'day_name': day_name,
                'day_name_short': day_name_short,
                'is_weekend': is_weekend,
                'is_weekday': is_weekday,
                'is_month_start': is_month_start,
                'is_month_end': is_month_end,
                'is_quarter_start': is_quarter_start,
                'is_quarter_end': is_quarter_end,
                'is_year_start': is_year_start,
                'is_year_end': is_year_end,
                'is_leap_year': is_leap_year,
                'days_in_month': days_in_month,
                'quarter_name': quarter_name,
                'year_month': year_month,
                'year_quarter': year_quarter,
                'year_week': year_week,
                'fiscal_year': fiscal_year,
                'fiscal_quarter': fiscal_quarter,
                'fiscal_month': fiscal_month,
                'fiscal_week': fiscal_week,
                'date_int': date_int,
                'date_iso': date_iso
            })
            
            current_date += timedelta(days=1)
            total_days += 1
    
    print(f"Generated {output_file} with {total_days:,} date records")
    print(f"Date range: {start_date} to {end_date} (exclusive)")
    print(f"Columns included: {', '.join(fieldnames)}")

if __name__ == '__main__':
    import sys
    
    # Allow command line arguments
    if len(sys.argv) > 1:
        output_file = sys.argv[1] if len(sys.argv) > 1 else 'dates.csv'
        start_date = sys.argv[2] if len(sys.argv) > 2 else '1990-01-01'
        end_date = sys.argv[3] if len(sys.argv) > 3 else '2050-01-01'
        generate_dates_csv(output_file, start_date, end_date)
    else:
        # Default parameters
        generate_dates_csv()