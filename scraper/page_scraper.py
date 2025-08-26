import requests
from bs4 import BeautifulSoup
import logging
import time
import re
from typing import Dict, Optional
from datetime import datetime
from urllib.parse import urljoin, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobPageScraper:
    def __init__(self):
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'az,en-US;q=0.9,en;q=0.8,tr;q=0.7',
            'accept-charset': 'utf-8, iso-8859-1;q=0.5',
            'cache-control': 'max-age=0',
            'dnt': '1',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape_job_page(self, job_url: str) -> Optional[Dict]:
        """Scrape detailed information from a job page"""
        try:
            if not job_url:
                return None
                
            logger.info(f"Scraping job page: {job_url}")
            
            response = self.session.get(job_url)
            response.raise_for_status()
            
            # Handle encoding more robustly
            if response.encoding is None or response.encoding == 'ISO-8859-1':
                response.encoding = 'utf-8'
            
            try:
                # First try with response.text
                soup = BeautifulSoup(response.text, 'html.parser')
            except UnicodeDecodeError:
                # Fallback to content with explicit encoding
                soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
            
            # Extract job details
            job_data = self._extract_job_details(soup, job_url)
            
            time.sleep(3)  # Be more respectful to avoid rate limits
            return job_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching job page {job_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing job page {job_url}: {e}")
            return None
    
    def _extract_job_details(self, soup: BeautifulSoup, job_url: str) -> Dict:
        """Extract job details from the parsed HTML"""
        job_data = {'job_url': job_url}
        
        try:
            # Extract job title - try multiple selectors
            title_element = soup.find('h1')
            if not title_element:
                title_element = soup.find('h2')
            if not title_element:
                title_element = soup.find('title')
            
            if title_element:
                title_text = title_element.get_text(strip=True)
                if title_text and title_text != 'Unknown' and len(title_text) > 2:
                    job_data['title'] = title_text
            
            # Ensure we always have a title, use job URL as fallback
            if 'title' not in job_data or not job_data['title']:
                job_data['title'] = f"Job from {job_url.split('/')[-1]}"
            
            # Extract company information
            company_link = soup.find('a', href=lambda href: href and '/companies/' in href)
            if company_link:
                job_data['company_name'] = company_link.get_text(strip=True)
                # Extract company slug from URL
                company_href = company_link.get('href', '')
                if '/companies/' in company_href:
                    job_data['company_slug'] = company_href.split('/companies/')[-1].strip('/')
            
            # Fallback: Extract company name from image alt text
            if not job_data.get('company_name'):
                logo_imgs = soup.find_all('img', alt=True)
                for img in logo_imgs:
                    alt_text = img.get('alt', '').strip()
                    if alt_text and alt_text != 'logo' and len(alt_text) > 2:
                        # Skip generic alt texts
                        if alt_text.lower() not in ['image', 'logo', 'icon', 'photo']:
                            job_data['company_name'] = alt_text
                            break
            
            # Extract company logo
            logo_img = soup.find('img', alt=True)
            if logo_img:
                logo_src = logo_img.get('src', '')
                if logo_src and ('logo' in logo_src.lower() or 's3' in logo_src or 'company' in logo_src.lower()):
                    job_data['company_logo'] = logo_src
            
            # Extract location
            location_elements = soup.find_all(text=lambda text: text and ('Azərbaycan' in text or 'Azerbaijan' in text))
            for element in location_elements:
                if 'Azərbaycan' in element or 'Azerbaijan' in element:
                    job_data['location'] = element.strip()
                    break
            
            # Extract job type (full-time, part-time, etc.)
            job_type_elements = soup.find_all(text=lambda text: text and ('Tam ştat' in text or 'Yarım ştat' in text or 'Müqavilə' in text))
            for element in job_type_elements:
                if any(job_type in element for job_type in ['Tam ştat', 'Yarım ştat', 'Müqavilə']):
                    job_data['job_type'] = element.strip()
                    break
            
            # Extract experience level
            exp_elements = soup.find_all(text=lambda text: text and re.search(r'\d+[-\s]*il', text))
            for element in exp_elements:
                if re.search(r'\d+[-\s]*il', element):
                    job_data['experience_level'] = element.strip()
                    break
            
            # Extract posted date
            date_pattern = r'\d{2}-\d{2}-\d{4}'
            date_elements = soup.find_all(text=lambda text: text and re.search(date_pattern, text))
            for element in date_elements:
                date_match = re.search(date_pattern, element)
                if date_match:
                    try:
                        date_str = date_match.group()
                        job_data['posted_date'] = datetime.strptime(date_str, '%d-%m-%Y')
                        break
                    except ValueError:
                        continue
            
            # Extract deadline
            deadline_elements = soup.find_all(text=lambda text: text and ('Son tarix' in text))
            if deadline_elements:
                # Look for date near deadline text
                for elem in deadline_elements:
                    # Find the parent paragraph/div
                    parent = elem.parent if hasattr(elem, 'parent') else None
                    if parent:
                        # Look for the next sibling or nearby element with date
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            deadline_text = next_elem.get_text(strip=True)
                            # Try different date formats
                            date_patterns = [
                                r'(\w+ \d{1,2}, \d{4})',  # "August 31, 2025"
                                r'(\d{1,2} \w+ \d{4})',   # "31 August 2025"
                                r'(\d{1,2}-\d{1,2}-\d{4})', # "31-08-2025"
                                r'(\d{1,2}/\d{1,2}/\d{4})'  # "31/08/2025"
                            ]
                            
                            for pattern in date_patterns:
                                date_match = re.search(pattern, deadline_text)
                                if date_match:
                                    try:
                                        date_str = date_match.group(1)
                                        # Try different parsing formats
                                        for fmt in ['%B %d, %Y', '%d %B %Y', '%d-%m-%Y', '%d/%m/%Y']:
                                            try:
                                                job_data['deadline'] = datetime.strptime(date_str, fmt)
                                                break
                                            except ValueError:
                                                continue
                                        if job_data.get('deadline'):
                                            break
                                    except (ValueError, IndexError):
                                        continue
                            if job_data.get('deadline'):
                                break
            
            # Extract view count
            view_elements = soup.find_all(text=lambda text: text and text.strip().isdigit())
            for element in view_elements:
                # Look for elements that might be view counts (numbers in specific contexts)
                parent = element.parent if hasattr(element, 'parent') else None
                if parent and parent.name in ['p', 'span', 'div']:
                    siblings = parent.find_previous_siblings() + parent.find_next_siblings()
                    for sibling in siblings:
                        if sibling and hasattr(sibling, 'name') and sibling.name == 'svg':
                            # If there's an SVG nearby (likely an eye icon), this could be view count
                            try:
                                job_data['view_count'] = int(element.strip())
                                break
                            except ValueError:
                                continue
            
            # Extract description
            description_sections = soup.find_all(['div', 'section'], class_=lambda c: c and 'description' in c.lower())
            if not description_sections:
                # Look for sections with "Təsvir" (Description) heading
                desc_headings = soup.find_all(text=lambda text: text and 'Təsvir' in text)
                for heading in desc_headings:
                    parent = heading.parent if hasattr(heading, 'parent') else None
                    if parent:
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            description_sections = [next_sibling]
                            break
            
            if description_sections:
                description_html = description_sections[0]
                # Clean up the description text
                description_text = description_html.get_text(separator=' ', strip=True)
                job_data['description'] = self._clean_text(description_text)
            
            # Extract requirements
            requirements_sections = []
            req_headings = soup.find_all(text=lambda text: text and 'Tələblər' in text)
            for heading in req_headings:
                parent = heading.parent if hasattr(heading, 'parent') else None
                if parent:
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        requirements_sections = [next_sibling]
                        break
            
            if requirements_sections:
                requirements_html = requirements_sections[0]
                requirements_text = requirements_html.get_text(separator=' ', strip=True)
                job_data['requirements'] = self._clean_text(requirements_text)
            
            # Extract category
            # Look for category in spans with specific styling
            category_elements = soup.find_all('span', class_=lambda c: c and any(word in c.lower() for word in ['inline-block', 'rounded-full', 'bg-', 'text-accent']))
            for element in category_elements:
                category_text = element.get_text(strip=True)
                if category_text and len(category_text) > 5:  # Filter out very short text
                    job_data['category'] = category_text
                    break
                    
            # Fallback: Look for category near "Kateqoriya" text
            if not job_data.get('category'):
                category_headings = soup.find_all(text=lambda text: text and 'Kateqoriya' in text)
                for heading in category_headings:
                    parent = heading.parent if hasattr(heading, 'parent') else None
                    if parent:
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            category_span = next_sibling.find('span')
                            if category_span:
                                category_text = category_span.get_text(strip=True)
                                if category_text and len(category_text) > 5:
                                    job_data['category'] = category_text
                                    break
            
            # Extract apply URL
            apply_links = soup.find_all('a', text=lambda text: text and 'Müraciət' in text)
            if not apply_links:
                apply_links = soup.find_all('a', href=lambda href: href and 'apply' in href)
            
            if apply_links:
                apply_url = apply_links[0].get('href', '')
                if apply_url:
                    # Make sure URL is absolute
                    if apply_url.startswith('/'):
                        apply_url = urljoin(job_url, apply_url)
                    job_data['apply_url'] = apply_url
            
            # Extract job slug from URL
            parsed_url = urlparse(job_url)
            path_parts = parsed_url.path.strip('/').split('/')
            if len(path_parts) >= 2:
                job_data['slug'] = path_parts[-1]
            
            logger.info(f"Successfully extracted details for: {job_data.get('title', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Error extracting job details: {e}")
        
        return job_data
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters
        text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
        
        return text.strip()
    
    def batch_scrape_jobs(self, job_urls: list, max_jobs: int = None) -> list:
        """Scrape multiple job pages"""
        scraped_jobs = []
        
        urls_to_process = job_urls[:max_jobs] if max_jobs else job_urls
        
        logger.info(f"Starting batch scrape of {len(urls_to_process)} job pages")
        
        for i, url in enumerate(urls_to_process, 1):
            logger.info(f"Scraping job {i}/{len(urls_to_process)}")
            
            job_data = self.scrape_job_page(url)
            if job_data:
                scraped_jobs.append(job_data)
            
            # Be respectful to the server
            if i % 10 == 0:
                time.sleep(3)
            else:
                time.sleep(1)
        
        logger.info(f"Batch scraping complete. Successfully scraped {len(scraped_jobs)} jobs")
        return scraped_jobs
    
    def close(self):
        """Close the session"""
        self.session.close()