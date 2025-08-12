#!/usr/bin/env python3
"""
Apache Cloudberry (Incubating) - Airline Demo Data Generator
============================================================

This script generates realistic CSV data for the airline reservations demo.
It creates three files that can be loaded into Apache Cloudberry using \COPY commands.

Usage:
    python data-generator.py
    
Output Files:
    - passengers.csv (10,000 rows)
    - flights.csv (750 rows) 
    - bookings.csv (~15,000 rows)

Features:
- Realistic airport codes from major US airports
- Proper flight duration calculations
- Realistic passenger names and contact information
- Logical booking patterns (1-3 bookings per passenger)
"""

import csv
import random
import datetime
from typing import List, Tuple

# Realistic data for generation
FIRST_NAMES = [
    'John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 
    'James', 'Jessica', 'William', 'Ashley', 'Christopher', 'Amanda', 'Daniel', 
    'Melissa', 'Matthew', 'Deborah', 'Anthony', 'Dorothy', 'Mark', 'Amy', 
    'Donald', 'Angela', 'Steven', 'Helen', 'Paul', 'Brenda', 'Andrew', 'Emma',
    'Joshua', 'Olivia', 'Kenneth', 'Cynthia', 'Kevin', 'Marie', 'Brian', 
    'Janet', 'George', 'Catherine', 'Timothy', 'Frances', 'Ronald', 'Christine',
    'Jason', 'Samantha', 'Edward', 'Debra', 'Jeffrey', 'Rachel'
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
    'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 
    'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
    'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 
    'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King',
    'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores', 'Green', 
    'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
    'Carter', 'Roberts'
]

# Major US airports with typical flight times between them
AIRPORTS = {
    'JFK': ('New York JFK', 40.6413, -73.7781),
    'LAX': ('Los Angeles', 33.9428, -118.4081),
    'ORD': ('Chicago OHare', 41.9742, -87.9073),
    'DFW': ('Dallas Fort Worth', 32.8998, -97.0403),
    'DEN': ('Denver', 39.8561, -104.6737),
    'ATL': ('Atlanta', 33.6407, -84.4277),
    'SFO': ('San Francisco', 37.6213, -122.3790),
    'SEA': ('Seattle', 47.4502, -122.3088),
    'LAS': ('Las Vegas', 36.0840, -115.1537),
    'MCO': ('Orlando', 28.4312, -81.3081),
    'EWR': ('Newark', 40.6895, -74.1745),
    'CLT': ('Charlotte', 35.2144, -80.9473),
    'PHX': ('Phoenix', 33.4484, -112.0740),
    'IAH': ('Houston Intercontinental', 29.9902, -95.3368),
    'MIA': ('Miami', 25.7959, -80.2870),
    'BOS': ('Boston', 42.3656, -71.0096),
    'MSP': ('Minneapolis', 44.8818, -93.2044),
    'DTW': ('Detroit', 42.2162, -83.3554),
    'PHL': ('Philadelphia', 39.8744, -75.2424),
    'LGA': ('New York LaGuardia', 40.7769, -73.8740),
    'FLL': ('Fort Lauderdale', 26.0742, -80.1506),
    'BWI': ('Baltimore', 39.1774, -76.6684),
    'IAD': ('Washington Dulles', 38.9531, -77.4565),
    'MDW': ('Chicago Midway', 41.7868, -87.7522),
    'TPA': ('Tampa', 27.9755, -82.5332),
    'SAN': ('San Diego', 32.7338, -117.1933),
    'HNL': ('Honolulu', 21.3099, -157.8581),
    'PDX': ('Portland', 45.5898, -122.5951),
    'STL': ('St. Louis', 38.7487, -90.3700),
    'AUS': ('Austin', 30.1975, -97.6664)
}

def calculate_flight_duration(origin: str, destination: str) -> int:
    """Calculate realistic flight duration in hours based on distance."""
    if origin == destination:
        return 0
    
    # Simple distance-based duration (rough approximation)
    distance_factors = {
        ('JFK', 'LAX'): 6, ('JFK', 'SFO'): 6, ('JFK', 'SEA'): 6,
        ('JFK', 'DEN'): 4, ('JFK', 'ORD'): 2, ('JFK', 'ATL'): 2,
        ('LAX', 'SFO'): 1, ('LAX', 'LAS'): 1, ('LAX', 'PHX'): 2,
        ('ORD', 'DEN'): 2, ('ORD', 'ATL'): 2, ('ORD', 'DFW'): 2,
        ('ATL', 'MIA'): 2, ('ATL', 'MCO'): 1, ('DFW', 'LAX'): 3,
        ('DEN', 'SFO'): 2, ('DEN', 'SEA'): 2, ('SEA', 'SFO'): 2,
        ('BOS', 'JFK'): 1, ('IAD', 'ATL'): 2, ('PHL', 'ORD'): 2
    }
    
    # Try to find specific route, otherwise estimate
    key = (origin, destination)
    reverse_key = (destination, origin)
    
    if key in distance_factors:
        return distance_factors[key]
    elif reverse_key in distance_factors:
        return distance_factors[reverse_key]
    else:
        # Default estimation based on airport codes
        return random.randint(1, 5)

def generate_passengers(count: int) -> List[Tuple]:
    """Generate synthetic passenger data."""
    passengers = []
    used_emails = set()
    
    for i in range(1, count + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        
        # Generate unique email
        base_email = f"{first_name.lower()}.{last_name.lower()}{i}@airline-demo.com"
        while base_email in used_emails:
            base_email = f"{first_name.lower()}.{last_name.lower()}{i}_{random.randint(1,999)}@airline-demo.com"
        used_emails.add(base_email)
        
        # Generate phone number
        phone = f"+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        
        passengers.append((i, first_name, last_name, base_email, phone))
    
    return passengers

def generate_flights(count: int) -> List[Tuple]:
    """Generate synthetic flight data."""
    flights = []
    airports = list(AIRPORTS.keys())
    base_date = datetime.date.today()
    
    for i in range(1, count + 1):
        # Random origin and destination (ensure different)
        origin = random.choice(airports)
        destination = random.choice([a for a in airports if a != origin])
        
        # Generate flight number
        airline_code = random.choice(['AA', 'DL', 'UA', 'SW', 'AS', 'B6'])
        flight_number = f"{airline_code}{random.randint(1000, 9999)}"
        
        # Generate departure time (next 30 days)
        departure_date = base_date + datetime.timedelta(days=random.randint(0, 30))
        departure_hour = random.randint(6, 22)  # 6 AM to 10 PM
        departure_minute = random.choice([0, 15, 30, 45])
        departure_time = datetime.datetime.combine(departure_date, 
                                                 datetime.time(departure_hour, departure_minute))
        
        # Calculate arrival time
        flight_duration = calculate_flight_duration(origin, destination)
        arrival_time = departure_time + datetime.timedelta(hours=flight_duration, 
                                                         minutes=random.randint(0, 30))
        
        flights.append((i, flight_number, origin, destination, 
                       departure_time.isoformat(), arrival_time.isoformat()))
    
    return flights

def generate_bookings(passenger_count: int, flights: List[Tuple]) -> List[Tuple]:
    """Generate booking data linking passengers to flights."""
    bookings = []
    booking_id = 1
    
    flight_ids = [f[0] for f in flights]  # Extract flight IDs
    
    for passenger_id in range(1, passenger_count + 1):
        # Each passenger gets 1-3 bookings
        num_bookings = random.choices([1, 2, 3], weights=[0.5, 0.3, 0.2])[0]
        
        # Select random flights for this passenger
        passenger_flights = random.sample(flight_ids, min(num_bookings, len(flight_ids)))
        
        for flight_id in passenger_flights:
            # Find the flight details
            flight = next(f for f in flights if f[0] == flight_id)
            flight_departure = datetime.datetime.fromisoformat(flight[4])
            
            # Booking date should be before departure
            booking_date = flight_departure - datetime.timedelta(
                days=random.randint(1, 60), 
                hours=random.randint(0, 23)
            )
            
            # Generate seat number
            seat_row = random.randint(1, 35)
            seat_letter = random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
            seat_number = f"{seat_row}{seat_letter}"
            
            bookings.append((booking_id, passenger_id, flight_id, 
                           booking_date.isoformat(), seat_number))
            booking_id += 1
    
    return bookings

def write_csv(filename: str, headers: List[str], data: List[Tuple]):
    """Write data to CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"Generated {filename} with {len(data)} rows")

def main():
    """Generate all CSV files for the airline demo."""
    print("Apache Cloudberry (Incubating) - Airline Demo Data Generator")
    print("=" * 60)
    
    # Generate passengers
    print("Generating passenger data...")
    passengers = generate_passengers(10000)
    write_csv('passengers.csv', 
              ['passenger_id', 'first_name', 'last_name', 'email', 'phone'], 
              passengers)
    
    # Generate flights
    print("Generating flight data...")
    flights = generate_flights(750)
    write_csv('flights.csv',
              ['flight_id', 'flight_number', 'origin', 'destination', 'departure_time', 'arrival_time'],
              flights)
    
    # Generate bookings
    print("Generating booking data...")
    bookings = generate_bookings(10000, flights)
    write_csv('bookings.csv',
              ['booking_id', 'passenger_id', 'flight_id', 'booking_date', 'seat_number'],
              bookings)
    
    print("\nData generation complete!")
    print(f"Generated files:")
    print(f"  - passengers.csv ({len(passengers)} rows)")
    print(f"  - flights.csv ({len(flights)} rows)")
    print(f"  - bookings.csv ({len(bookings)} rows)")
    
    print("\nTo load into Apache Cloudberry, use:")
    print("\\COPY passenger FROM 'passengers.csv' CSV HEADER;")
    print("\\COPY flights FROM 'flights.csv' CSV HEADER;")
    print("\\COPY booking FROM 'bookings.csv' CSV HEADER;")

if __name__ == "__main__":
    main()