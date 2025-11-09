#!/usr/bin/env python3
"""
CDC Data Generator - Continuously inserts, updates, and deletes MongoDB documents.

This script simulates real-world data changes to test CDC functionality.
Uses the meeting data sample provided.
"""

import pymongo
import random
import time
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Sample meeting data template
MEETING_TEMPLATE = {
    "meeting_id": None,  # Will be generated
    "created_date": None,  # Will be set
    "last_modified_date": None,  # Will be set
    "user_id": None,  # Will be generated
    "tennant_id": "tenant-acme",
    "meeting_title": None,  # Will be generated
    "meeting_type": None,  # Will be selected
    "meeting_priority": None,  # Will be selected
    "meeting_start_datetime": None,  # Will be generated
    "meeting_end_datetime": None,  # Will be generated
    "meeting_duration_hours": None,  # Will be calculated
    "source_location": {
        "city": None,
        "country": None,
        "airport_code": None,
        "address": None
    },
    "destination_location": {
        "city": None,
        "country": None,
        "airport_code": None,
        "venue": None
    },
    "meeting_purpose": None,  # Will be generated
    "meeting_attendees": [],
    "pre_meeting_buffer_hours": 4,
    "post_meeting_buffer_hours": 2,
    "travel_required": True,
    "manager_approval_required": True,
    "cost_center": "PROD-APAC-2025",
    "workflow_status": "manager_approval_pending",
    "is_multi_day_trip": True,
    "created_by_agent": "user_input",
    "is_deleted": False,
    "travel_feasibility_status": "pending",
    "manager_approval_status": "not_required",
    "cost_currency": "USD",
    "special_requirements": [],
    "emergency_contacts": [],
    "estimated_total_cost": None  # Will be generated
}


class CDCDataGenerator:
    """Generate continuous data changes for CDC testing."""
    
    def __init__(
        self,
        mongo_uri: str,
        database: str = "test_db",
        collection: str = "meetings",
        operations_per_second: float = 1.0
    ):
        """
        Initialize data generator.
        
        Args:
            mongo_uri: MongoDB connection URI
            database: Database name
            collection: Collection name
            operations_per_second: Target operations per second
        """
        self.mongo_uri = mongo_uri
        self.database_name = database
        self.collection_name = collection
        self.ops_per_sec = operations_per_second
        
        # Connect to MongoDB
        self.client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[database]
        self.collection = self.db[collection]
        
        # Test connection
        try:
            self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB: {database}.{collection}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
        
        # Data pools for generation
        self.cities = ["Hyderabad", "Singapore", "Tokyo", "New York", "London", "Dubai", "Sydney"]
        self.countries = ["India", "Singapore", "Japan", "USA", "UK", "UAE", "Australia"]
        self.airport_codes = ["HYD", "SIN", "NRT", "JFK", "LHR", "DXB", "SYD"]
        self.meeting_types = ["client_meeting", "internal_meeting", "conference", "workshop", "training"]
        self.priorities = ["urgent", "high", "medium", "low"]
        self.workflow_statuses = [
            "draft", "manager_approval_pending", "approved", "rejected",
            "travel_booked", "completed", "cancelled"
        ]
        self.meeting_titles = [
            "Q4 Product Strategy Review",
            "Annual Planning Session",
            "Client Onboarding Meeting",
            "Technical Architecture Review",
            "Sales Pipeline Discussion",
            "Team Retrospective",
            "Customer Success Workshop"
        ]
        
        # Statistics
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0,
            'start_time': None
        }
    
    def generate_meeting_doc(self, meeting_id: str = None) -> Dict[str, Any]:
        """Generate a random meeting document."""
        now = datetime.utcnow()
        
        if meeting_id is None:
            meeting_id = f"meeting-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        
        # Random dates (future meeting)
        start_date = now + timedelta(days=random.randint(1, 30))
        duration_hours = random.choice([2, 4, 6, 8])
        end_date = start_date + timedelta(hours=duration_hours)
        
        # Random locations
        source_city = random.choice(self.cities)
        dest_city = random.choice([c for c in self.cities if c != source_city])
        source_idx = self.cities.index(source_city)
        dest_idx = self.cities.index(dest_city)
        
        # Generate attendees
        num_attendees = random.randint(1, 5)
        attendees = []
        for i in range(num_attendees):
            attendees.append({
                "name": f"Attendee {i+1}",
                "email": f"attendee{i+1}@company.com",
                "role": random.choice(["Manager", "Engineer", "Product Manager", "Designer"]),
                "company": random.choice(["Internal", "External"])
            })
        
        doc = MEETING_TEMPLATE.copy()
        doc.update({
            "meeting_id": meeting_id,
            "created_date": now,
            "last_modified_date": now,
            "user_id": f"user-{random.randint(10000, 99999)}",
            "meeting_title": random.choice(self.meeting_titles),
            "meeting_type": random.choice(self.meeting_types),
            "meeting_priority": random.choice(self.priorities),
            "meeting_start_datetime": start_date,
            "meeting_end_datetime": end_date,
            "meeting_duration_hours": duration_hours,
            "source_location": {
                "city": source_city,
                "country": self.countries[source_idx],
                "airport_code": self.airport_codes[source_idx],
                "address": f"{source_city} Office"
            },
            "destination_location": {
                "city": dest_city,
                "country": self.countries[dest_idx],
                "airport_code": self.airport_codes[dest_idx],
                "venue": f"{dest_city} Convention Center"
            },
            "meeting_purpose": f"Strategic planning session with regional stakeholders",
            "meeting_attendees": attendees,
            "workflow_status": random.choice(self.workflow_statuses),
            "estimated_total_cost": random.randint(1000, 5000),
            "is_deleted": False
        })
        
        return doc
    
    def insert_meeting(self) -> str:
        """Insert a new meeting document."""
        try:
            doc = self.generate_meeting_doc()
            result = self.collection.insert_one(doc)
            self.stats['inserts'] += 1
            logger.info(f"✅ INSERT: meeting_id={doc['meeting_id']}, _id={result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ INSERT failed: {e}")
            return None
    
    def update_meeting(self, doc_id: str = None) -> bool:
        """Update an existing meeting document."""
        try:
            # Find a random document if not provided
            if doc_id is None:
                count = self.collection.count_documents({"is_deleted": False})
                if count == 0:
                    logger.warning("⚠️  No documents to update, skipping")
                    return False
                
                skip = random.randint(0, min(count - 1, 100))
                doc = self.collection.find({"is_deleted": False}).skip(skip).limit(1).next()
                doc_id = doc['_id']
            
            # Generate update fields
            updates = {
                "$set": {
                    "last_modified_date": datetime.utcnow(),
                    "meeting_priority": random.choice(self.priorities),
                    "workflow_status": random.choice(self.workflow_statuses),
                    "estimated_total_cost": random.randint(1000, 5000)
                }
            }
            
            # Sometimes add a new field (schema evolution test)
            if random.random() < 0.3:  # 30% chance
                updates["$set"]["new_field"] = f"value-{random.randint(1, 1000)}"
            
            result = self.collection.update_one(
                {"_id": doc_id, "is_deleted": False},
                updates
            )
            
            if result.modified_count > 0:
                self.stats['updates'] += 1
                logger.info(f"✅ UPDATE: _id={doc_id}, modified={result.modified_count}")
                return True
            else:
                logger.warning(f"⚠️  UPDATE: No document modified (_id={doc_id})")
                return False
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ UPDATE failed: {e}")
            return False
    
    def delete_meeting(self, doc_id: str = None) -> bool:
        """Soft delete a meeting document."""
        try:
            # Find a random document if not provided
            if doc_id is None:
                count = self.collection.count_documents({"is_deleted": False})
                if count == 0:
                    logger.warning("⚠️  No documents to delete, skipping")
                    return False
                
                skip = random.randint(0, min(count - 1, 100))
                doc = self.collection.find({"is_deleted": False}).skip(skip).limit(1).next()
                doc_id = doc['_id']
            
            # Soft delete
            result = self.collection.update_one(
                {"_id": doc_id, "is_deleted": False},
                {
                    "$set": {
                        "is_deleted": True,
                        "deleted_at": datetime.utcnow(),
                        "last_modified_date": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                self.stats['deletes'] += 1
                logger.info(f"✅ DELETE: _id={doc_id}")
                return True
            else:
                logger.warning(f"⚠️  DELETE: No document modified (_id={doc_id})")
                return False
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ DELETE failed: {e}")
            return False
    
    def run_continuous(self, duration_seconds: int = 300, operation_weights: Dict[str, float] = None):
        """
        Run continuous data generation.
        
        Args:
            duration_seconds: How long to run (0 = infinite)
            operation_weights: Weights for insert/update/delete (default: 50/40/10)
        """
        if operation_weights is None:
            operation_weights = {'insert': 0.5, 'update': 0.4, 'delete': 0.1}
        
        self.stats['start_time'] = time.time()
        logger.info(f"🚀 Starting CDC data generator for {duration_seconds}s (or Ctrl+C to stop)")
        logger.info(f"   Operations/sec: {self.ops_per_sec}")
        logger.info(f"   Weights: Insert={operation_weights['insert']*100}%, "
                   f"Update={operation_weights['update']*100}%, "
                   f"Delete={operation_weights['delete']*100}%")
        
        interval = 1.0 / self.ops_per_sec
        end_time = time.time() + duration_seconds if duration_seconds > 0 else None
        
        try:
            while True:
                if end_time and time.time() >= end_time:
                    break
                
                # Select operation based on weights
                rand = random.random()
                if rand < operation_weights['insert']:
                    self.insert_meeting()
                elif rand < operation_weights['insert'] + operation_weights['update']:
                    self.update_meeting()
                else:
                    self.delete_meeting()
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("\n⚠️  Interrupted by user")
        finally:
            self.print_stats()
            self.client.close()
    
    def print_stats(self):
        """Print operation statistics."""
        duration = time.time() - self.stats['start_time'] if self.stats['start_time'] else 0
        total_ops = self.stats['inserts'] + self.stats['updates'] + self.stats['deletes']
        
        logger.info("\n" + "="*60)
        logger.info("📊 CDC Data Generator Statistics")
        logger.info("="*60)
        logger.info(f"Duration: {duration:.1f}s")
        logger.info(f"Total Operations: {total_ops}")
        logger.info(f"  ✅ Inserts: {self.stats['inserts']}")
        logger.info(f"  ✅ Updates: {self.stats['updates']}")
        logger.info(f"  ✅ Deletes: {self.stats['deletes']}")
        logger.info(f"  ❌ Errors: {self.stats['errors']}")
        if duration > 0:
            logger.info(f"Operations/sec: {total_ops / duration:.2f}")
        logger.info("="*60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="CDC Data Generator for MongoDB")
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017/test_db"),
        help="MongoDB connection URI"
    )
    parser.add_argument(
        "--database",
        default=os.getenv("MONGO_DATABASE", "test_db"),
        help="Database name"
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("MONGO_COLLECTION", "meetings"),
        help="Collection name"
    )
    parser.add_argument(
        "--ops-per-sec",
        type=float,
        default=1.0,
        help="Operations per second"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Duration in seconds (0 = infinite)"
    )
    parser.add_argument(
        "--insert-weight",
        type=float,
        default=0.5,
        help="Weight for insert operations (0-1)"
    )
    parser.add_argument(
        "--update-weight",
        type=float,
        default=0.4,
        help="Weight for update operations (0-1)"
    )
    parser.add_argument(
        "--delete-weight",
        type=float,
        default=0.1,
        help="Weight for delete operations (0-1)"
    )
    
    args = parser.parse_args()
    
    # Normalize weights
    total_weight = args.insert_weight + args.update_weight + args.delete_weight
    operation_weights = {
        'insert': args.insert_weight / total_weight,
        'update': args.update_weight / total_weight,
        'delete': args.delete_weight / total_weight
    }
    
    generator = CDCDataGenerator(
        mongo_uri=args.mongo_uri,
        database=args.database,
        collection=args.collection,
        operations_per_second=args.ops_per_sec
    )
    
    generator.run_continuous(
        duration_seconds=args.duration,
        operation_weights=operation_weights
    )


if __name__ == "__main__":
    main()

