import asyncio
import aiohttp
import json
import csv
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import time


@dataclass
class WorkerProfile:
    id: int
    full_name: str
    slug: str
    profile_image_url: Optional[str]
    bio: Optional[str]
    open_to_work_salary_by_agreement: bool
    resume_url: Optional[str]
    educations: List[Dict[str, Any]]
    open_to_work_salary_range: Optional[Dict[str, Any]]
    open_to_work_experience: Optional[Dict[str, Any]]
    languages: List[Dict[str, Any]]
    technical_skills: List[Dict[str, Any]]


class WorkAzClient:
    def __init__(self):
        self.base_url = "https://api.work.az/v1"
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
            "content-type": "application/json",
            "dnt": "1",
            "origin": "https://www.work.az",
            "referer": "https://www.work.az/",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "x-session-id": "session_1756319932256_oyc4vt6fm"
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=100)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_full_time_workers(self, count: int = 12, page: int = 1) -> Dict[str, Any]:
        """
        Get full-time workers from work.az API
        
        Args:
            count: Number of workers per page (default: 12)
            page: Page number (default: 1)
            
        Returns:
            Dict containing API response data
        """
        url = f"{self.base_url}/users/full-time-workers"
        payload = {"count": count, "page": page}
        
        try:
            async with self.session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    text = await response.text()
                    if text.strip():
                        data = json.loads(text)
                        return data
                    else:
                        raise Exception("Empty response")
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
        except asyncio.TimeoutError:
            raise Exception("Request timed out")
        except aiohttp.ClientError as e:
            raise Exception(f"Client error: {str(e)}")

    async def get_multiple_pages(self, pages: List[int], count: int = 12) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages concurrently
        
        Args:
            pages: List of page numbers to fetch
            count: Number of workers per page
            
        Returns:
            List of API responses for each page
        """
        tasks = []
        for page in pages:
            task = self.get_full_time_workers(count=count, page=page)
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)

    def parse_workers(self, response_data: Dict[str, Any]) -> List[WorkerProfile]:
        """
        Parse API response data into WorkerProfile objects
        
        Args:
            response_data: Raw API response data
            
        Returns:
            List of WorkerProfile objects
        """
        workers = []
        if response_data.get("success") and response_data.get("data", {}).get("content"):
            for worker_data in response_data["data"]["content"]:
                worker = WorkerProfile(
                    id=worker_data["id"],
                    full_name=worker_data["fullName"],
                    slug=worker_data["slug"],
                    profile_image_url=worker_data.get("profileImageUrl"),
                    bio=worker_data.get("bio"),
                    open_to_work_salary_by_agreement=worker_data["openToWorkSalaryByAgreement"],
                    resume_url=worker_data.get("resumeUrl"),
                    educations=worker_data.get("educations", []),
                    open_to_work_salary_range=worker_data.get("openToWorkSalaryRange"),
                    open_to_work_experience=worker_data.get("openToWorkExperience"),
                    languages=worker_data.get("languages", []),
                    technical_skills=worker_data.get("technicalSkills", [])
                )
                workers.append(worker)
        return workers

    async def search_workers_with_skills(self, skill_names: List[str], max_pages: int = 5) -> List[WorkerProfile]:
        """
        Search for workers with specific technical skills
        
        Args:
            skill_names: List of skill names to search for
            max_pages: Maximum number of pages to search
            
        Returns:
            List of WorkerProfile objects matching the skills
        """
        all_workers = []
        pages = list(range(1, max_pages + 1))
        
        responses = await self.get_multiple_pages(pages)
        
        for response in responses:
            if isinstance(response, Exception):
                print(f"Error fetching page: {response}")
                continue
                
            workers = self.parse_workers(response)
            
            # Filter workers by skills
            for worker in workers:
                worker_skills = [skill["skill"]["name"].lower() 
                               for skill in worker.technical_skills 
                               if skill["skill"]["name"]]
                
                if any(skill.lower() in worker_skills for skill in skill_names):
                    all_workers.append(worker)
        
        return all_workers

    async def get_all_workers(self, batch_size: int = 10) -> List[WorkerProfile]:
        """
        Fetch ALL workers from the API with progress tracking
        
        Args:
            batch_size: Number of pages to fetch concurrently in each batch
            
        Returns:
            List of all WorkerProfile objects
        """
        print("Getting total page count...")
        first_page = await self.get_full_time_workers(count=12, page=1)
        
        if not first_page.get("success"):
            raise Exception("Failed to get first page")
        
        total_pages = first_page["data"]["totalPages"]
        total_elements = first_page["data"]["totalElements"]
        
        print(f"Found {total_elements} workers across {total_pages} pages")
        print(f"Fetching in batches of {batch_size} pages...")
        
        all_workers = []
        
        # Process first page
        workers = self.parse_workers(first_page)
        all_workers.extend(workers)
        
        # Process remaining pages in batches
        for batch_start in range(2, total_pages + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, total_pages)
            pages = list(range(batch_start, batch_end + 1))
            
            print(f"Fetching pages {batch_start}-{batch_end} ({len(pages)} pages)...")
            
            try:
                responses = await self.get_multiple_pages(pages)
                
                batch_workers = []
                for i, response in enumerate(responses):
                    if isinstance(response, Exception):
                        print(f"Error on page {pages[i]}: {response}")
                        continue
                    
                    workers = self.parse_workers(response)
                    batch_workers.extend(workers)
                
                all_workers.extend(batch_workers)
                print(f"Total workers collected: {len(all_workers)}")
                
                # Small delay to be respectful to the API
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Error in batch {batch_start}-{batch_end}: {e}")
                continue
        
        print(f"Final count: {len(all_workers)} workers collected")
        return all_workers

    def save_to_csv(self, workers: List[WorkerProfile], filename: str = "workers.csv"):
        """Save workers data to CSV file"""
        print(f"Saving {len(workers)} workers to {filename}...")
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if not workers:
                return
            
            # Flatten the data structure for CSV
            fieldnames = [
                'id', 'full_name', 'slug', 'profile_image_url', 'bio',
                'open_to_work_salary_by_agreement', 'resume_url',
                'salary_range', 'experience_level',
                'education_count', 'languages_count', 'technical_skills_count',
                'languages', 'technical_skills', 'educations'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for worker in workers:
                # Flatten complex fields
                languages = '; '.join([
                    f"{lang['skill']['name']}({lang['level']})" 
                    for lang in worker.languages 
                    if lang.get('skill', {}).get('name')
                ])
                
                technical_skills = '; '.join([
                    f"{skill['skill']['name']}({skill['level']})" 
                    for skill in worker.technical_skills 
                    if skill.get('skill', {}).get('name')
                ])
                
                educations = '; '.join([
                    f"{(edu.get('university', {}) or {}).get('name', edu.get('universityOther', ''))} - {edu.get('professionOther', '')}"
                    for edu in worker.educations
                    if edu is not None
                ])
                
                writer.writerow({
                    'id': worker.id,
                    'full_name': worker.full_name,
                    'slug': worker.slug,
                    'profile_image_url': worker.profile_image_url,
                    'bio': worker.bio,
                    'open_to_work_salary_by_agreement': worker.open_to_work_salary_by_agreement,
                    'resume_url': worker.resume_url,
                    'salary_range': worker.open_to_work_salary_range.get('name') if worker.open_to_work_salary_range else None,
                    'experience_level': worker.open_to_work_experience.get('name') if worker.open_to_work_experience else None,
                    'education_count': len(worker.educations),
                    'languages_count': len(worker.languages),
                    'technical_skills_count': len(worker.technical_skills),
                    'languages': languages,
                    'technical_skills': technical_skills,
                    'educations': educations
                })
        
        print(f"CSV saved successfully: {filename}")

    def save_to_json(self, workers: List[WorkerProfile], filename: str = "workers.json"):
        """Save workers data to JSON file"""
        print(f"Saving {len(workers)} workers to {filename}...")
        
        # Convert dataclass objects to dictionaries
        workers_data = []
        for worker in workers:
            worker_dict = asdict(worker)
            workers_data.append(worker_dict)
        
        # Create final structure
        output_data = {
            "timestamp": datetime.now().isoformat(),
            "total_workers": len(workers),
            "workers": workers_data
        }
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(output_data, jsonfile, ensure_ascii=False, indent=2)
        
        print(f"JSON saved successfully: {filename}")


async def main():
    """Extract ALL worker data and save to CSV and JSON"""
    start_time = time.time()
    
    async with WorkAzClient() as client:
        try:
            # Get ALL workers
            print("Starting full data extraction...")
            all_workers = await client.get_all_workers(batch_size=10)
            
            if not all_workers:
                print("No workers found!")
                return
            
            # Save to CSV
            client.save_to_csv(all_workers, "work_az_workers.csv")
            
            # Save to JSON
            client.save_to_json(all_workers, "work_az_workers.json")
            
            # Statistics
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"\n{'='*50}")
            print(f"EXTRACTION COMPLETE!")
            print(f"{'='*50}")
            print(f"Total workers extracted: {len(all_workers)}")
            print(f"Time taken: {duration:.2f} seconds")
            print(f"Average: {len(all_workers)/duration:.1f} workers/second")
            print(f"Files created:")
            print(f"  - work_az_workers.csv ({os.path.getsize('work_az_workers.csv')} bytes)")
            print(f"  - work_az_workers.json ({os.path.getsize('work_az_workers.json')} bytes)")
            
            # Sample data preview
            print(f"\nSample worker data:")
            if all_workers:
                worker = all_workers[0]
                print(f"- {worker.full_name}")
                print(f"- Skills: {len(worker.technical_skills)}")
                print(f"- Languages: {len(worker.languages)}")
                print(f"- Education: {len(worker.educations)}")
                
        except Exception as e:
            print(f"Error during extraction: {e}")
            import traceback
            traceback.print_exc()


async def demo():
    """Example usage of the WorkAzClient"""
    
    async with WorkAzClient() as client:
        # Get first page of workers
        print("Fetching first page of workers...")
        response = await client.get_full_time_workers(count=12, page=1)
        
        if response.get("success"):
            workers = client.parse_workers(response)
            print(f"Found {len(workers)} workers on page 1")
            
            # Display first worker details
            if workers:
                worker = workers[0]
                print(f"\nFirst worker: {worker.full_name}")
                print(f"Bio: {worker.bio}")
                print(f"Technical skills: {[skill['skill']['name'] for skill in worker.technical_skills if skill['skill']['name']]}")
        
        # Fetch multiple pages concurrently
        print("\nFetching pages 1-3 concurrently...")
        responses = await client.get_multiple_pages([1, 2, 3], count=12)
        
        total_workers = 0
        for i, response in enumerate(responses, 1):
            if not isinstance(response, Exception) and response.get("success"):
                workers_count = len(response.get("data", {}).get("content", []))
                total_workers += workers_count
                print(f"Page {i}: {workers_count} workers")
            else:
                print(f"Page {i}: Error - {response}")
        
        print(f"Total workers fetched: {total_workers}")
        
        # Search for workers with specific skills
        print("\nSearching for workers with Python or JavaScript skills...")
        skilled_workers = await client.search_workers_with_skills(["Python", "JavaScript", "PHP"], max_pages=3)
        
        print(f"Found {len(skilled_workers)} workers with programming skills:")
        for worker in skilled_workers[:5]:  # Show first 5
            skills = [skill["skill"]["name"] for skill in worker.technical_skills if skill["skill"]["name"]]
            print(f"- {worker.full_name}: {', '.join(skills)}")


if __name__ == "__main__":
    # Run the full extraction
    asyncio.run(main())