#!/usr/bin/env python3
"""Generate times.csv with all 86400 seconds in a day with useful time columns."""

import csv
from datetime import datetime, timedelta

def generate_times_csv(output_file='times.csv'):
    """Generate a CSV file with all times in a day."""
    
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = [
            'second_of_day',
            'time_24h',
            'time_12h',
            'hour_24',
            'hour_12',
            'minute',
            'second',
            'am_pm',
            'hour_minute_24h',
            'hour_minute_12h',
            'quarter_hour',
            'half_hour',
            'time_of_day',
            'part_of_day',
            'business_hours',
            'shift'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        base_date = datetime(2000, 1, 1)  # Arbitrary date for time calculations
        
        for second_of_day in range(86400):
            current_time = base_date + timedelta(seconds=second_of_day)
            
            hour_24 = current_time.hour
            minute = current_time.minute
            second = current_time.second
            
            # Calculate 12-hour format
            hour_12 = hour_24 % 12
            if hour_12 == 0:
                hour_12 = 12
            am_pm = 'AM' if hour_24 < 12 else 'PM'
            
            # Format times
            time_24h = current_time.strftime('%H:%M:%S')
            time_12h = current_time.strftime('%I:%M:%S %p')
            hour_minute_24h = current_time.strftime('%H:%M')
            hour_minute_12h = current_time.strftime('%I:%M %p')
            
            # Quarter and half hour indicators
            quarter_hour = f"Q{(minute // 15) + 1}"
            half_hour = 'H1' if minute < 30 else 'H2'
            
            # Time of day classification
            if 0 <= hour_24 < 6:
                time_of_day = 'Night'
                part_of_day = 'Early Morning'
            elif 6 <= hour_24 < 12:
                time_of_day = 'Morning'
                part_of_day = 'Morning'
            elif 12 <= hour_24 < 18:
                time_of_day = 'Afternoon'
                part_of_day = 'Afternoon'
            else:
                time_of_day = 'Evening'
                part_of_day = 'Evening/Night'
            
            # Business hours (9 AM - 5 PM)
            business_hours = 'Business Hours' if 9 <= hour_24 < 17 else 'Non-Business Hours'
            
            # Shift classification
            if 7 <= hour_24 < 15:
                shift = 'Day Shift'
            elif 15 <= hour_24 < 23:
                shift = 'Evening Shift'
            else:
                shift = 'Night Shift'
            
            writer.writerow({
                'second_of_day': second_of_day,
                'time_24h': time_24h,
                'time_12h': time_12h,
                'hour_24': hour_24,
                'hour_12': hour_12,
                'minute': minute,
                'second': second,
                'am_pm': am_pm,
                'hour_minute_24h': hour_minute_24h,
                'hour_minute_12h': hour_minute_12h,
                'quarter_hour': quarter_hour,
                'half_hour': half_hour,
                'time_of_day': time_of_day,
                'part_of_day': part_of_day,
                'business_hours': business_hours,
                'shift': shift
            })
    
    print(f"Generated {output_file} with 86,400 time records")
    print(f"Columns included: {', '.join(fieldnames)}")

if __name__ == '__main__':
    generate_times_csv()