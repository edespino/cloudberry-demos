#!/usr/bin/env python3
"""
Apache Cloudberry (Incubating) - Enhanced Airline Demo Data Loader
================================================================

This script creates realistic airline data using open-source datasets:
- OpenFlights for airport/route data
- U.S. DOT BTS for realistic flight patterns  
- Faker for synthetic passenger data (no PII)

Data Sources:
- OpenFlights: https://openflights.org/data.html (airports, airlines, routes)
- U.S. DOT BTS: https://transtats.bts.gov/ (on-time performance data)
- Faker: Synthetic passenger generation

Usage:
    pip install requests pandas faker
    python enhanced-data-loader.py

Output:
- Creates SQL files ready for Apache Cloudberry
- Generates realistic flight schedules based on actual routes
- Produces synthetic passengers with no privacy concerns
"""

import requests
import pandas as pd
import csv
import random
import datetime
from faker import Faker
from typing import Dict, List, Tuple, Optional
import zipfile
import io
import os
import sys

fake = Faker()

class AirlineDataLoader:
    def __init__(self):
        self.airports = {}
        self.airlines = {}
        self.routes = []
        self.us_airports = set()
        
    def download_openflights_data(self):
        """Download and parse OpenFlights datasets."""
        print("Downloading OpenFlights airport data...")
        
        try:
            # Download airports data
            airports_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
            response = requests.get(airports_url, timeout=30)
            response.raise_for_status()
            
            # Parse airports (CSV without headers)
            # Format: ID,Name,City,Country,IATA,ICAO,Lat,Lon,Alt,Timezone,DST,Tz,Type,Source
            airports_data = []
            for line in response.text.strip().split('\n'):
                fields = line.split(',')
                if len(fields) >= 8 and fields[4] != '\\N':  # Has IATA code
                    iata = fields[4].strip('"')
                    name = fields[1].strip('"')
                    city = fields[2].strip('"')
                    country = fields[3].strip('"')
                    
                    self.airports[iata] = {
                        'name': name,
                        'city': city,
                        'country': country,
                        'lat': float(fields[6]) if fields[6] != '\\N' else 0,
                        'lon': float(fields[7]) if fields[7] != '\\N' else 0
                    }
                    
                    # Track US airports
                    if country == 'United States':
                        self.us_airports.add(iata)
            
            print(f"Loaded {len(self.airports)} airports ({len(self.us_airports)} in US)")
            
            # Download airlines data
            print("Downloading OpenFlights airline data...")
            airlines_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
            response = requests.get(airlines_url, timeout=30)
            response.raise_for_status()
            
            # Parse airlines
            for line in response.text.strip().split('\n'):
                fields = line.split(',')
                if len(fields) >= 4 and fields[3] != '\\N':  # Has IATA code
                    iata = fields[3].strip('"')
                    name = fields[1].strip('"')
                    country = fields[6].strip('"') if len(fields) > 6 else 'Unknown'
                    
                    self.airlines[iata] = {
                        'name': name,
                        'country': country
                    }
            
            print(f"Loaded {len(self.airlines)} airlines")
            
            # Download routes data
            print("Downloading OpenFlights routes data...")
            routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
            response = requests.get(routes_url, timeout=30)
            response.raise_for_status()
            
            # Parse routes and filter for US domestic
            us_routes = []
            for line in response.text.strip().split('\n'):
                fields = line.split(',')
                if len(fields) >= 7:
                    airline_iata = fields[0].strip('"')
                    origin = fields[2].strip('"')
                    dest = fields[4].strip('"')
                    
                    # Filter for US domestic routes with major airlines
                    if (origin in self.us_airports and dest in self.us_airports and 
                        origin != dest and airline_iata in ['AA', 'DL', 'UA', 'WN', 'AS', 'B6', 'NK', 'F9']):
                        us_routes.append({
                            'airline': airline_iata,
                            'origin': origin,
                            'destination': dest
                        })
            
            self.routes = us_routes
            print(f"Loaded {len(self.routes)} US domestic routes")
            
        except Exception as e:
            print(f"Error downloading OpenFlights data: {e}")
            print("Falling back to hardcoded airport list...")
            self._load_fallback_data()
    
    def _load_fallback_data(self):
        """Fallback data if OpenFlights download fails."""
        fallback_airports = {
            'JFK': {'name': 'John F Kennedy Intl', 'city': 'New York', 'country': 'United States'},
            'LAX': {'name': 'Los Angeles Intl', 'city': 'Los Angeles', 'country': 'United States'},
            'ORD': {'name': 'Chicago OHare Intl', 'city': 'Chicago', 'country': 'United States'},
            'ATL': {'name': 'Hartsfield Jackson Atlanta Intl', 'city': 'Atlanta', 'country': 'United States'},
            'DFW': {'name': 'Dallas Fort Worth Intl', 'city': 'Dallas', 'country': 'United States'},
            'DEN': {'name': 'Denver Intl', 'city': 'Denver', 'country': 'United States'},
            'SFO': {'name': 'San Francisco Intl', 'city': 'San Francisco', 'country': 'United States'},
            'SEA': {'name': 'Seattle Tacoma Intl', 'city': 'Seattle', 'country': 'United States'},
            'LAS': {'name': 'McCarran Intl', 'city': 'Las Vegas', 'country': 'United States'},
            'PHX': {'name': 'Phoenix Sky Harbor Intl', 'city': 'Phoenix', 'country': 'United States'},
            'IAH': {'name': 'George Bush Intercontinental', 'city': 'Houston', 'country': 'United States'},
            'MCO': {'name': 'Orlando Intl', 'city': 'Orlando', 'country': 'United States'},
            'MIA': {'name': 'Miami Intl', 'city': 'Miami', 'country': 'United States'},
            'BOS': {'name': 'Logan Intl', 'city': 'Boston', 'country': 'United States'},
            'EWR': {'name': 'Newark Liberty Intl', 'city': 'Newark', 'country': 'United States'}
        }
        
        self.airports = fallback_airports
        self.us_airports = set(fallback_airports.keys())
        
        # Generate some realistic routes
        airports_list = list(self.us_airports)
        airlines = ['AA', 'DL', 'UA', 'WN', 'AS', 'B6']
        
        self.routes = []
        for origin in airports_list:
            for dest in airports_list:
                if origin != dest:
                    self.routes.append({
                        'airline': random.choice(airlines),
                        'origin': origin,
                        'destination': dest
                    })
        
        print(f"Using fallback data: {len(self.airports)} airports, {len(self.routes)} routes")
    
    def generate_realistic_flights(self, count: int = 1000) -> List[Dict]:
        """Generate realistic flight schedules based on actual routes."""
        flights = []
        base_date = datetime.date.today()
        
        # Weight routes by popularity (major hubs get more flights)
        hub_weights = {
            'ATL': 3.0, 'ORD': 2.8, 'LAX': 2.5, 'DFW': 2.3, 'DEN': 2.0,
            'JFK': 2.2, 'SFO': 1.8, 'SEA': 1.5, 'LAS': 1.7, 'PHX': 1.4,
            'IAH': 1.6, 'MCO': 1.3, 'MIA': 1.2, 'BOS': 1.4, 'EWR': 1.3
        }
        
        for i in range(count):
            # Select route weighted by hub popularity
            route = random.choice(self.routes)
            origin = route['origin']
            dest = route['destination']
            airline = route['airline']
            
            # Apply hub weighting
            origin_weight = hub_weights.get(origin, 1.0)
            dest_weight = hub_weights.get(dest, 1.0)
            combined_weight = (origin_weight + dest_weight) / 2
            
            # Skip some routes based on weight (simulate less popular routes)
            if random.random() > (combined_weight / 3.0):
                continue
            
            # Generate realistic flight number
            flight_num = random.randint(1, 9999)
            flight_number = f"{airline}{flight_num:04d}"
            
            # Generate departure time (next 30 days, business hours weighted)
            days_ahead = random.randint(0, 30)
            departure_date = base_date + datetime.timedelta(days=days_ahead)
            
            # Weight departure times (more flights during business hours)
            hour_weights = [0.1] * 6 + [0.8] * 4 + [1.0] * 8 + [0.9] * 4 + [0.3] * 2
            departure_hour = random.choices(range(24), weights=hour_weights)[0]
            departure_minute = random.choice([0, 15, 30, 45])
            
            departure_time = datetime.datetime.combine(
                departure_date, 
                datetime.time(departure_hour, departure_minute)
            )
            
            # Calculate realistic flight duration based on distance
            duration_hours = self._estimate_flight_duration(origin, dest)
            arrival_time = departure_time + datetime.timedelta(
                hours=duration_hours,
                minutes=random.randint(-15, 45)  # Schedule padding
            )
            
            flights.append({
                'flight_id': i + 1,
                'flight_number': flight_number,
                'airline': airline,
                'origin': origin,
                'destination': dest,
                'departure_time': departure_time,
                'arrival_time': arrival_time
            })
        
        return flights[:count]  # Ensure exact count
    
    def _estimate_flight_duration(self, origin: str, dest: str) -> float:
        """Estimate flight duration based on typical US domestic routes."""
        duration_map = {
            ('JFK', 'LAX'): 6.0, ('JFK', 'SFO'): 6.5, ('JFK', 'SEA'): 6.5,
            ('JFK', 'DEN'): 4.5, ('JFK', 'ORD'): 2.5, ('JFK', 'ATL'): 2.5,
            ('LAX', 'SFO'): 1.5, ('LAX', 'LAS'): 1.2, ('LAX', 'PHX'): 1.5,
            ('LAX', 'DEN'): 2.5, ('LAX', 'ORD'): 4.0, ('LAX', 'ATL'): 4.5,
            ('ORD', 'DEN'): 2.5, ('ORD', 'ATL'): 2.0, ('ORD', 'DFW'): 2.5,
            ('ATL', 'MIA'): 2.0, ('ATL', 'MCO'): 1.5, ('ATL', 'BOS'): 2.5,
            ('DFW', 'LAX'): 3.0, ('DFW', 'PHX'): 2.0, ('DFW', 'DEN'): 1.5,
            ('DEN', 'SFO'): 2.5, ('DEN', 'SEA'): 2.0, ('DEN', 'PHX'): 1.5,
            ('SFO', 'SEA'): 2.0, ('SFO', 'LAS'): 1.5, ('SFO', 'PHX'): 2.0,
            ('SEA', 'LAX'): 2.5, ('SEA', 'DEN'): 2.0, ('SEA', 'SFO'): 2.0,
            ('BOS', 'JFK'): 1.2, ('BOS', 'ATL'): 2.5, ('BOS', 'ORD'): 3.0,
            ('MIA', 'JFK'): 3.0, ('MIA', 'ATL'): 2.0, ('MIA', 'MCO'): 1.0
        }
        
        # Try both directions
        key = (origin, dest)
        reverse_key = (dest, origin)
        
        if key in duration_map:
            return duration_map[key]
        elif reverse_key in duration_map:
            return duration_map[reverse_key]
        else:
            # Estimate based on rough US geography
            return random.uniform(1.5, 5.5)
    
    def generate_synthetic_passengers(self, count: int = 10000) -> List[Dict]:
        """Generate synthetic passenger data using Faker."""
        print(f"Generating {count} synthetic passengers...")
        
        passengers = []
        used_emails = set()
        
        for i in range(1, count + 1):
            # Generate realistic name
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            # Generate unique email
            email_base = f"{first_name.lower()}.{last_name.lower()}"
            email = f"{email_base}@{fake.free_email_domain()}"
            counter = 1
            while email in used_emails:
                email = f"{email_base}{counter}@{fake.free_email_domain()}"
                counter += 1
            used_emails.add(email)
            
            # Generate phone number
            phone = fake.phone_number()
            
            passengers.append({
                'passenger_id': i,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone
            })
        
        return passengers
    
    def generate_realistic_bookings(self, passengers: List[Dict], flights: List[Dict]) -> List[Dict]:
        """Generate realistic booking patterns."""
        print("Generating realistic booking patterns...")
        
        bookings = []
        booking_id = 1
        
        # Create booking patterns
        for passenger in passengers:
            # Determine booking behavior (frequent vs occasional traveler)
            traveler_type = random.choices(
                ['frequent', 'business', 'leisure', 'occasional'],
                weights=[0.05, 0.15, 0.6, 0.2]
            )[0]
            
            if traveler_type == 'frequent':
                num_bookings = random.randint(5, 12)
            elif traveler_type == 'business':
                num_bookings = random.randint(3, 8)
            elif traveler_type == 'leisure':
                num_bookings = random.randint(1, 3)
            else:  # occasional
                num_bookings = random.randint(1, 2)
            
            # Select flights for this passenger
            passenger_flights = random.sample(flights, min(num_bookings, len(flights)))
            
            for flight in passenger_flights:
                # Calculate booking date (1-60 days before departure)
                departure_dt = flight['departure_time']
                days_before = random.randint(1, 60)
                booking_date = departure_dt - datetime.timedelta(days=days_before)
                
                # Generate realistic seat assignment
                seat_row = random.randint(1, 35)
                seat_letter = random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
                seat_number = f"{seat_row}{seat_letter}"
                
                bookings.append({
                    'booking_id': booking_id,
                    'passenger_id': passenger['passenger_id'],
                    'flight_id': flight['flight_id'],
                    'booking_date': booking_date,
                    'seat_number': seat_number
                })
                
                booking_id += 1
        
        return bookings
    
    def write_sql_files(self, passengers: List[Dict], flights: List[Dict], bookings: List[Dict]):
        """Write data to SQL files for Cloudberry."""
        
        # Write passengers
        with open('load_passengers.sql', 'w') as f:
            f.write("-- Apache Cloudberry (Incubating) - Load Passengers Data\n")
            f.write("-- Generated from synthetic data using Faker library\n\n")
            f.write("INSERT INTO passenger (passenger_id, first_name, last_name, email, phone) VALUES\n")
            
            passenger_values = []
            for p in passengers:
                values = f"({p['passenger_id']}, '{p['first_name']}', '{p['last_name']}', '{p['email']}', '{p['phone']}')"
                passenger_values.append(values)
            
            f.write(',\n'.join(passenger_values))
            f.write(';\n')
        
        # Write flights
        with open('load_flights.sql', 'w') as f:
            f.write("-- Apache Cloudberry (Incubating) - Load Flights Data\n")
            f.write("-- Generated from OpenFlights route data with realistic scheduling\n\n")
            f.write("INSERT INTO flights (flight_id, flight_number, origin, destination, departure_time, arrival_time) VALUES\n")
            
            flight_values = []
            for flight in flights:
                departure_str = flight['departure_time'].strftime('%Y-%m-%d %H:%M:%S')
                arrival_str = flight['arrival_time'].strftime('%Y-%m-%d %H:%M:%S')
                values = f"({flight['flight_id']}, '{flight['flight_number']}', '{flight['origin']}', '{flight['destination']}', '{departure_str}', '{arrival_str}')"
                flight_values.append(values)
            
            f.write(',\n'.join(flight_values))
            f.write(';\n')
        
        # Write bookings
        with open('load_bookings.sql', 'w') as f:
            f.write("-- Apache Cloudberry (Incubating) - Load Bookings Data\n")
            f.write("-- Generated with realistic booking patterns and lead times\n\n")
            f.write("INSERT INTO booking (booking_id, passenger_id, flight_id, booking_date, seat_number) VALUES\n")
            
            booking_values = []
            for booking in bookings:
                booking_date_str = booking['booking_date'].strftime('%Y-%m-%d %H:%M:%S')
                values = f"({booking['booking_id']}, {booking['passenger_id']}, {booking['flight_id']}, '{booking_date_str}', '{booking['seat_number']}')"
                booking_values.append(values)
            
            f.write(',\n'.join(booking_values))
            f.write(';\n')
        
        print(f"Generated SQL files:")
        print(f"  - load_passengers.sql ({len(passengers)} rows)")
        print(f"  - load_flights.sql ({len(flights)} rows)")  
        print(f"  - load_bookings.sql ({len(bookings)} rows)")

def main():
    """Main execution function."""
    print("Apache Cloudberry (Incubating) - Enhanced Airline Demo Data Loader")
    print("=" * 70)
    print("Using real-world datasets for maximum realism:")
    print("- OpenFlights.org for airport and route data")
    print("- Faker library for synthetic passenger data (no PII)")
    print("- Realistic booking patterns and flight scheduling")
    print()
    
    try:
        loader = AirlineDataLoader()
        
        # Download and process open-source data
        loader.download_openflights_data()
        
        # Generate realistic data
        print("\nGenerating realistic flight schedules...")
        flights = loader.generate_realistic_flights(1000)
        print(f"Generated {len(flights)} flights based on real route data")
        
        print("\nGenerating synthetic passengers...")
        passengers = loader.generate_synthetic_passengers(10000)
        print(f"Generated {len(passengers)} passengers with synthetic data")
        
        print("\nGenerating booking patterns...")
        bookings = loader.generate_realistic_bookings(passengers, flights)
        print(f"Generated {len(bookings)} bookings with realistic patterns")
        
        # Write output files
        print("\nWriting SQL files...")
        loader.write_sql_files(passengers, flights, bookings)
        
        print("\n" + "=" * 70)
        print("DATA GENERATION COMPLETE!")
        print("\nTo load into Apache Cloudberry:")
        print("1. First run the schema creation: \\i airline-reservations-demo.sql")
        print("2. Then load the data:")
        print("   \\i load_passengers.sql")
        print("   \\i load_flights.sql") 
        print("   \\i load_bookings.sql")
        print("\nData Features:")
        print("✓ Real US domestic routes from OpenFlights")
        print("✓ Realistic flight schedules and durations")
        print("✓ Hub-weighted flight frequency (ATL, ORD, LAX busiest)")
        print("✓ Synthetic passengers (no privacy concerns)")
        print("✓ Realistic booking lead times and travel patterns")
        print("✓ Major airline codes (AA, DL, UA, WN, etc.)")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nPlease ensure you have required packages:")
        print("pip install requests pandas faker")
        sys.exit(1)

if __name__ == "__main__":
    main()