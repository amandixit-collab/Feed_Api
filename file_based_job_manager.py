import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional
import threading

class FileBasedJobManager:
    def __init__(self, jobs_dir="./jobs", activities_dir="./activities"):
        self.jobs_dir = jobs_dir
        self.activities_dir = activities_dir
        self._lock = threading.Lock()
        
        # Create directories if they don't exist
        os.makedirs(jobs_dir, exist_ok=True)
        os.makedirs(activities_dir, exist_ok=True)
    
    def create_job(self, affiliate_merchant_id: str, partner_id: str, job_data: Dict) -> str:
        """Create a new job"""
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "affiliate_merchant_id": affiliate_merchant_id,
            "partner_id": partner_id,
            "type": "violet_feed_generation",
            "status": "generating",
            "retry_count": 0,
            "job_data": job_data,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        job_file = os.path.join(self.jobs_dir, f"{job_id}.json")
        with self._lock:
            with open(job_file, 'w') as f:
                json.dump(job, f, indent=2)
        
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job by ID"""
        job_file = os.path.join(self.jobs_dir, f"{job_id}.json")
        if not os.path.exists(job_file):
            return None
        
        with open(job_file, 'r') as f:
            return json.load(f)
    
    def get_job_by_affiliate_merchant(self, affiliate_merchant_id: str) -> Optional[Dict]:
        """Find job by affiliate merchant ID (requires scanning all files)"""
        with self._lock:
            for filename in os.listdir(self.jobs_dir):
                if filename.endswith('.json'):
                    job_file = os.path.join(self.jobs_dir, filename)
                    with open(job_file, 'r') as f:
                        job = json.load(f)
                        if job.get('affiliate_merchant_id') == affiliate_merchant_id:
                            return job
        return None
    
    def update_job(self, job_id: str, updates: Dict) -> bool:
        """Update job with new data"""
        job_file = os.path.join(self.jobs_dir, f"{job_id}.json")
        if not os.path.exists(job_file):
            return False
        
        with self._lock:
            with open(job_file, 'r') as f:
                job = json.load(f)
            
            job.update(updates)
            job['updated_at'] = datetime.utcnow().isoformat()
            
            with open(job_file, 'w') as f:
                json.dump(job, f, indent=2)
        
        return True
    
    def create_activity(self, entity: str, entity_id: str, source: str, 
                       requested_by: str, activity_data: Dict) -> str:
        """Create an activity record"""
        activity_id = str(uuid.uuid4())
        activity = {
            "id": activity_id,
            "entity": entity,
            "entity_id": entity_id,
            "source": source,
            "requested_by": requested_by,
            "activity": activity_data,
            "created_at": datetime.utcnow().isoformat()
        }
        
        activity_file = os.path.join(self.activities_dir, f"{activity_id}.json")
        with self._lock:
            with open(activity_file, 'w') as f:
                json.dump(activity, f, indent=2)
        
        return activity_id
    
    def get_job_activities(self, job_id: str) -> List[Dict]:
        """Get all activities for a job (requires scanning all activity files)"""
        activities = []
        with self._lock:
            for filename in os.listdir(self.activities_dir):
                if filename.endswith('.json'):
                    activity_file = os.path.join(self.activities_dir, filename)
                    with open(activity_file, 'r') as f:
                        activity = json.load(f)
                        if activity.get('entity_id') == job_id:
                            activities.append(activity)
        
        # Sort by created_at
        activities.sort(key=lambda x: x.get('created_at', ''))
        return activities
    
    def list_all_jobs(self) -> List[Dict]:
        """List all jobs (can be slow with many jobs)"""
        jobs = []
        with self._lock:
            for filename in os.listdir(self.jobs_dir):
                if filename.endswith('.json'):
                    job_file = os.path.join(self.jobs_dir, filename)
                    with open(job_file, 'r') as f:
                        jobs.append(json.load(f))
        
        return jobs
