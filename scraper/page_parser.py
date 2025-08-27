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

class JobPageParser:
    def __init__(self):
        # Rotate between different user agents to avoid blocking
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/139.0.2792.65 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        ]
        
        self.current_user_agent_index = 0
        self.failed_requests = 0  # Track consecutive failures
        self.requests_since_renewal = 0  # Track requests since last renewal
        self.session = None
        self._create_new_session()
    
    def _create_new_session(self):
        """Create a new session with rotated user agent"""
        if self.session:
            self.session.close()
        
        # Rotate user agent
        user_agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (self.current_user_agent_index + 1) % len(self.user_agents)
        
        headers = {
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
            'user-agent': user_agent
        }
        
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.failed_requests = 0
        self.requests_since_renewal = 0
        
        logger.info(f"🔄 Created new session with user agent: {user_agent[:50]}...")
    
    def _renew_session(self):
        """Renew session when it gets blocked"""
        logger.info("🔄 Renewing session due to consecutive failures")
        self._create_new_session()
    
    def scrape_job_page(self, job_url: str) -> Optional[Dict]:
        """Scrape detailed information from a job page with retry logic"""
        max_retries = 3
        base_delay = 1
        
        # Renew session if too many consecutive failures OR too many requests
        if self.failed_requests >= 2 or self.requests_since_renewal >= 50:
            self._renew_session()
        
        for attempt in range(max_retries):
            try:
                if not job_url:
                    return None
                    
                if attempt == 0:
                    logger.info(f"Scraping job page: {job_url}")
                else:
                    logger.info(f"Retry {attempt}/{max_retries-1} for {job_url}")
                
                self.requests_since_renewal += 1
                response = self.session.get(job_url)
                
                # Handle rate limiting and blocking
                if response.status_code == 429:
                    logger.warning(f"Rate limited (429), renewing session and retrying...")
                    self._renew_session()  # Renew session immediately on 429
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for rate limit on {job_url}")
                        return None
                
                # Handle other blocking (403, 404, etc.)
                if response.status_code in [403, 404, 502, 503]:
                    logger.warning(f"Blocked with status {response.status_code}, renewing session...")
                    self._renew_session()
                    if attempt < max_retries - 1:
                        time.sleep(base_delay * (2 ** attempt))
                        continue
                
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
                
                # Reset failure counter on success
                self.failed_requests = 0
                
                time.sleep(1)  # Increased delay to be more respectful
                return job_data
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1 and "429" in str(e):
                    wait_time = base_delay * (2 ** attempt)
                    logger.warning(f"Request error (likely rate limit), waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error fetching job page {job_url}: {e}")
                    self.failed_requests += 1
                    return None
            except Exception as e:
                logger.error(f"Error parsing job page {job_url}: {e}")
                self.failed_requests += 1
                return None
        
        # Increment failure counter if all retries failed
        self.failed_requests += 1
        return None
    
    def _extract_title_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal title extraction using multiple fallback strategies"""
        title_selectors = [
            # Strategy 1: Standard H1 tags
            ('h1', 'text'),
            # Strategy 2: Common job title classes
            ('.job-title', 'text'),
            ('.position-title', 'text'),
            ('.vacancy-title', 'text'),
            # Strategy 3: Heading tags with job-related classes
            ('h1[class*="title"]', 'text'),
            ('h2[class*="title"]', 'text'),
            ('h1[class*="job"]', 'text'),
            ('h1[class*="position"]', 'text'),
            # Strategy 4: Div with title-like classes
            ('div[class*="title"]', 'text'),
            ('div[class*="job-title"]', 'text'),
            # Strategy 5: Meta og:title
            ('meta[property="og:title"]', 'content'),
            # Strategy 6: Page title as last resort
            ('title', 'text')
        ]
        
        for selector, attr_type in title_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    if attr_type == 'text':
                        title_text = element.get_text(strip=True)
                    else:  # content attribute
                        title_text = element.get(attr_type, '')
                    
                    if title_text and len(title_text.strip()) > 2:
                        # Clean title - remove company suffixes and noise
                        clean_title = self._clean_title_text(title_text)
                        if clean_title and clean_title.lower() not in ['jobs', 'vacancies', 'career', 'glorri']:
                            return clean_title
            except Exception as e:
                continue
        
        return None
    
    def _clean_title_text(self, title_text: str) -> str:
        """Clean job title text from common noise"""
        if not title_text:
            return ""
        
        # Remove common suffixes and prefixes
        clean_patterns = [
            r'\s*\|\s*.*$',  # Remove "| Company Name"
            r'\s*-\s*.*$',   # Remove "- Company Name"
            r'\s*@\s*.*$',   # Remove "@ Company"
            r'^\s*Job:\s*',  # Remove "Job: " prefix
            r'^\s*Position:\s*',  # Remove "Position: " prefix
            r'\s*\(\s*\w+\s*\)\s*$',  # Remove location in parentheses at end
        ]
        
        cleaned = title_text
        for pattern in clean_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        return cleaned.strip()
    
    def _extract_company_universal(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Universal company extraction"""
        company_data = {}
        
        # Try multiple strategies for company name and link
        company_selectors = [
            # Direct company link
            'a[href*="/companies/"]',
            'a[href*="/company/"]',
            # Company name in common classes
            '.company-name',
            '.employer-name',
            '[class*="company"][class*="name"]',
            # Links that might be companies (avoid generic links)
            'a[href*="company"]',
        ]
        
        for selector in company_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    company_name = element.get_text(strip=True)
                    if company_name and len(company_name) > 1:
                        company_data['company_name'] = company_name
                        
                        # Extract slug from href
                        href = element.get('href', '')
                        if href:
                            # Extract company slug from various URL patterns
                            for pattern in ['/companies/', '/company/']:
                                if pattern in href:
                                    slug = href.split(pattern)[-1].strip('/').split('/')[0]
                                    if slug:
                                        company_data['company_slug'] = slug
                                    break
                        break
            except Exception:
                continue
        
        # Fallback: look for company logo alt text
        if not company_data.get('company_name'):
            logo_selectors = [
                'img[alt]:not([alt=""]):not([alt*="logo"]):not([alt*="icon"])',
                'img[class*="logo"]',
                'img[class*="company"]'
            ]
            
            for selector in logo_selectors:
                try:
                    img = soup.select_one(selector)
                    if img:
                        alt_text = img.get('alt', '').strip()
                        if alt_text and len(alt_text) > 2 and alt_text.lower() not in ['image', 'logo', 'icon', 'photo']:
                            company_data['company_name'] = alt_text
                            # Try to get logo URL
                            logo_src = img.get('src', '')
                            if logo_src:
                                company_data['company_logo'] = logo_src
                            break
                except Exception:
                    continue
        
        return company_data
    
    def _extract_description_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal description extraction"""
        description_selectors = [
            # Strategy 1: Common description classes
            '.job-description',
            '.job-desc',
            '.description',
            '.vacancy-description',
            '.job-content',
            '.position-description',
            # Strategy 2: Sections with description-like classes
            'section[class*="description"]',
            'div[class*="description"]',
            'div[class*="job-desc"]',
            # Strategy 3: Content after "Description" or "Təsvir" headings
            # This will be handled separately
        ]
        
        for selector in description_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    desc_text = element.get_text(separator=' ', strip=True)
                    if desc_text and len(desc_text) > 50:  # Substantial description
                        return self._clean_text(desc_text)
            except Exception:
                continue
        
        # Strategy: Look for text after description headings
        description_headings = soup.find_all(text=lambda text: text and any(word in text.lower() for word in ['təsvir', 'description', 'about', 'job summary']))
        
        for heading_text in description_headings:
            try:
                parent = heading_text.parent if hasattr(heading_text, 'parent') else None
                if parent:
                    # Look for next sibling or parent's next sibling
                    next_element = parent.find_next_sibling()
                    if not next_element:
                        next_element = parent.parent.find_next_sibling() if parent.parent else None
                    
                    if next_element:
                        desc_text = next_element.get_text(separator=' ', strip=True)
                        if desc_text and len(desc_text) > 50:
                            return self._clean_text(desc_text)
            except Exception:
                continue
        
        # Fallback: Find large text blocks (likely descriptions)
        all_text_elements = soup.find_all(['div', 'p', 'section'], text=True)
        for element in all_text_elements:
            try:
                text_content = element.get_text(strip=True)
                if len(text_content) > 100 and len(text_content.split()) > 15:
                    # Check if this looks like a job description (contains job-related keywords)
                    job_keywords = ['experience', 'skill', 'requirement', 'responsible', 'duties', 'təcrübə', 'məsul', 'bacarıq']
                    if any(keyword in text_content.lower() for keyword in job_keywords):
                        return self._clean_text(text_content)
            except Exception:
                continue
        
        return None
    
    def _extract_location_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal location extraction"""
        location_selectors = [
            '.location',
            '.job-location',
            '.position-location',
            '[class*="location"]',
            '.city',
            '.address'
        ]
        
        # Try CSS selectors first
        for selector in location_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    location_text = element.get_text(strip=True)
                    if location_text and len(location_text) > 1:
                        return location_text
            except Exception:
                continue
        
        # Fallback: Search for location keywords in text
        location_keywords = ['Azərbaycan', 'Azerbaijan', 'Bakı', 'Baku', 'Gəncə', 'Sumqayıt', 'Mingəçevir']
        all_text = soup.find_all(text=True)
        for text in all_text:
            if any(keyword in str(text) for keyword in location_keywords):
                # Extract the containing element's text
                parent = text.parent if hasattr(text, 'parent') else None
                if parent:
                    location_text = parent.get_text(strip=True)
                    if location_text and len(location_text) < 100:  # Not too long
                        return location_text
        
        return None
    
    def _extract_job_type_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal job type extraction"""
        job_type_selectors = [
            '.job-type',
            '.employment-type',
            '.work-type',
            '[class*="type"]',
            '.schedule'
        ]
        
        # Try CSS selectors first
        for selector in job_type_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    job_type_text = element.get_text(strip=True)
                    if job_type_text and len(job_type_text) > 1:
                        return job_type_text
            except Exception:
                continue
        
        # Fallback: Search for job type keywords
        job_type_keywords = ['Tam ştat', 'Yarım ştat', 'Müqavilə', 'Full-time', 'Part-time', 'Contract', 'Remote']
        all_text = soup.find_all(text=lambda text: text and any(keyword in str(text) for keyword in job_type_keywords))
        
        for text in all_text:
            for keyword in job_type_keywords:
                if keyword in str(text):
                    return str(text).strip()
        
        return None
    
    def _extract_experience_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal experience level extraction"""
        experience_selectors = [
            '.experience',
            '.experience-level',
            '.seniority',
            '[class*="experience"]',
            '.level'
        ]
        
        # Try CSS selectors first
        for selector in experience_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    exp_text = element.get_text(strip=True)
                    if exp_text and len(exp_text) > 1:
                        return exp_text
            except Exception:
                continue
        
        # Fallback: Search for experience patterns
        experience_patterns = [
            r'\d+[-\s]*il',  # "3 il", "5-il"
            r'\d+[-\s]*year',  # "3 years"
            r'\d+\+[-\s]*year',  # "3+ years" 
            r'junior|senior|middle|lead|principal',  # Experience levels
            r'entry[-\s]*level|mid[-\s]*level|senior[-\s]*level'
        ]
        
        all_text = soup.get_text()
        for pattern in experience_patterns:
            matches = re.findall(pattern, all_text, re.IGNORECASE)
            if matches:
                return matches[0].strip()
        
        return None
    
    def _extract_requirements_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal requirements extraction"""
        requirements_selectors = [
            '.requirements',
            '.job-requirements', 
            '.qualifications',
            '[class*="requirement"]',
            '[class*="qualification"]'
        ]
        
        # Try CSS selectors first
        for selector in requirements_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    req_text = element.get_text(separator=' ', strip=True)
                    if req_text and len(req_text) > 50:
                        return self._clean_text(req_text)
            except Exception:
                continue
        
        # Strategy: Look for text after requirements headings
        requirement_headings = soup.find_all(text=lambda text: text and any(word in text.lower() for word in ['tələblər', 'requirements', 'qualifications', 'skills needed']))
        
        for heading_text in requirement_headings:
            try:
                parent = heading_text.parent if hasattr(heading_text, 'parent') else None
                if parent:
                    next_element = parent.find_next_sibling()
                    if not next_element:
                        next_element = parent.parent.find_next_sibling() if parent.parent else None
                    
                    if next_element:
                        req_text = next_element.get_text(separator=' ', strip=True)
                        if req_text and len(req_text) > 50:
                            return self._clean_text(req_text)
            except Exception:
                continue
        
        return None
    
    def _extract_category_universal(self, soup: BeautifulSoup) -> Optional[str]:
        """Universal category extraction"""
        category_selectors = [
            '.category',
            '.job-category',
            '.department',
            '.field',
            '[class*="category"]',
            '.tag',
            '.badge'
        ]
        
        # Try CSS selectors first
        for selector in category_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    category_text = element.get_text(strip=True)
                    if category_text and len(category_text) > 2 and len(category_text) < 100:
                        return category_text
            except Exception:
                continue
        
        # Fallback: Look for category near "Kateqoriya" text
        category_headings = soup.find_all(text=lambda text: text and any(word in text.lower() for word in ['kateqoriya', 'category', 'department']))
        
        for heading_text in category_headings:
            try:
                parent = heading_text.parent if hasattr(heading_text, 'parent') else None
                if parent:
                    next_element = parent.find_next_sibling()
                    if next_element:
                        category_span = next_element.find(['span', 'div', 'p'])
                        if category_span:
                            category_text = category_span.get_text(strip=True)
                            if category_text and len(category_text) > 2:
                                return category_text
            except Exception:
                continue
        
        return None
    
    def _extract_apply_url_universal(self, soup: BeautifulSoup, job_url: str) -> Optional[str]:
        """Universal apply URL extraction"""
        apply_selectors = [
            'a[href*="apply"]',
            'a[class*="apply"]',
            '.apply-button',
            '.apply-link',
            'button[class*="apply"]'
        ]
        
        # Try CSS selectors first
        for selector in apply_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    apply_url = element.get('href', '')
                    if apply_url:
                        # Make sure URL is absolute
                        if apply_url.startswith('/'):
                            apply_url = urljoin(job_url, apply_url)
                        return apply_url
            except Exception:
                continue
        
        # Fallback: Look for links with "apply" text
        apply_links = soup.find_all('a', text=lambda text: text and any(word in text.lower() for word in ['müraciət', 'apply', 'başvur']))
        
        for link in apply_links:
            apply_url = link.get('href', '')
            if apply_url:
                if apply_url.startswith('/'):
                    apply_url = urljoin(job_url, apply_url)
                return apply_url
        
        return None
    
    def _extract_additional_details(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Extract additional job details from the 'Vakansiya haqqında' section"""
        additional_data = {}
        
        # Find all the key-value pairs in the job details section
        detail_pairs = soup.find_all('div', class_='mb-4 flex justify-between sm:mb-6')
        
        for pair in detail_pairs:
            try:
                key_elem = pair.find('p', class_='text-neutral-80')
                value_elem = pair.find('p', class_='font-semibold capitalize text-foreground') or \
                           pair.find('p', class_='font-semibold text-foreground')
                
                if key_elem and value_elem:
                    key = key_elem.get_text(strip=True).lower()
                    value = value_elem.get_text(strip=True)
                    
                    # Map Azerbaijani keys to English field names
                    if 'təcrübə' in key:  # Experience
                        additional_data['experience_level'] = value
                    elif 'vəzifə dərəcəsi' in key:  # Position level
                        additional_data['position_level'] = value
                    elif 'təhsil' in key:  # Education
                        additional_data['education_level'] = value
                    elif 'əmək haqqı' in key or 'maaş' in key:  # Salary
                        additional_data.update(self._parse_salary(value))
                    elif 'paylaşılıb' in key:  # Published date
                        additional_data['published_date'] = self._parse_date(value)
                        
            except Exception as e:
                continue
                
        return additional_data
    
    def _parse_salary(self, salary_text: str) -> Dict[str, any]:
        """Parse salary range from text like '1200 - 1500 AZN'"""
        import re
        
        salary_data = {}
        
        # Remove HTML comments and clean text
        salary_text = re.sub(r'<!--.*?-->', '', salary_text)
        salary_text = salary_text.strip()
        
        # Pattern for salary range: "1200 - 1500 AZN" or "1200-1500 AZN"
        range_pattern = r'(\d+)\s*[-–]\s*(\d+)\s*([A-Z]{3})?'
        range_match = re.search(range_pattern, salary_text)
        
        if range_match:
            salary_data['salary_min'] = int(range_match.group(1))
            salary_data['salary_max'] = int(range_match.group(2))
            if range_match.group(3):
                salary_data['salary_currency'] = range_match.group(3)
            else:
                # Default currency if not specified
                salary_data['salary_currency'] = 'AZN'
        else:
            # Pattern for single salary: "1500 AZN"
            single_pattern = r'(\d+)\s*([A-Z]{3})?'
            single_match = re.search(single_pattern, salary_text)
            if single_match:
                salary_amount = int(single_match.group(1))
                salary_data['salary_min'] = salary_amount
                salary_data['salary_max'] = salary_amount
                salary_data['salary_currency'] = single_match.group(2) or 'AZN'
        
        return salary_data
    
    def _parse_date(self, date_text: str) -> Optional:
        """Parse various date formats"""
        import re
        from datetime import datetime
        
        # Clean the text
        date_text = date_text.strip().lower()
        
        # Azerbaijani month names to numbers
        az_months = {
            'yanvar': 1, 'fevral': 2, 'mart': 3, 'aprel': 4, 'may': 5, 'iyun': 6,
            'iyul': 7, 'avqust': 8, 'sentyabr': 9, 'oktyabr': 10, 'noyabr': 11, 'dekabr': 12
        }
        
        # Pattern: "avqust 21, 2025"
        az_pattern = r'(\w+)\s+(\d{1,2}),?\s+(\d{4})'
        az_match = re.search(az_pattern, date_text)
        
        if az_match:
            month_name = az_match.group(1)
            day = int(az_match.group(2))
            year = int(az_match.group(3))
            
            if month_name in az_months:
                try:
                    return datetime(year, az_months[month_name], day)
                except ValueError:
                    pass
        
        # Fallback to existing date parsing logic
        return None
    
    def _extract_job_details(self, soup: BeautifulSoup, job_url: str) -> Dict:
        """Extract job details from the parsed HTML using universal selectors"""
        job_data = {'job_url': job_url}
        
        try:
            # UNIVERSAL TITLE EXTRACTION
            title_text = self._extract_title_universal(soup)
            if title_text:
                job_data['title'] = title_text
            
            # UNIVERSAL COMPANY EXTRACTION
            company_data = self._extract_company_universal(soup)
            job_data.update(company_data)
            
            # UNIVERSAL DESCRIPTION EXTRACTION  
            description = self._extract_description_universal(soup)
            if description:
                job_data['description'] = description
            
            # UNIVERSAL LOCATION EXTRACTION
            location = self._extract_location_universal(soup)
            if location:
                job_data['location'] = location
            
            # UNIVERSAL JOB TYPE EXTRACTION
            job_type = self._extract_job_type_universal(soup)
            if job_type:
                job_data['job_type'] = job_type
            
            # UNIVERSAL EXPERIENCE LEVEL EXTRACTION
            experience_level = self._extract_experience_universal(soup)
            if experience_level:
                job_data['experience_level'] = experience_level
            
            # UNIVERSAL REQUIREMENTS EXTRACTION
            requirements = self._extract_requirements_universal(soup)
            if requirements:
                job_data['requirements'] = requirements
            
            # UNIVERSAL CATEGORY EXTRACTION
            category = self._extract_category_universal(soup)
            if category:
                job_data['category'] = category
            
            # UNIVERSAL APPLY URL EXTRACTION
            apply_url = self._extract_apply_url_universal(soup, job_url)
            if apply_url:
                job_data['apply_url'] = apply_url
            
            # EXTRACT ADDITIONAL JOB DETAILS
            additional_details = self._extract_additional_details(soup)
            job_data.update(additional_details)
            
            # Extract posted date (keep original logic as it works)
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
            
            # Keep existing date and view count extraction (they work fine)
            # Extract deadline (keep original logic)
            deadline_elements = soup.find_all(text=lambda text: text and ('Son tarix' in text))
            if deadline_elements:
                for elem in deadline_elements:
                    parent = elem.parent if hasattr(elem, 'parent') else None
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            deadline_text = next_elem.get_text(strip=True)
                            date_patterns = [
                                r'(\w+ \d{1,2}, \d{4})',
                                r'(\d{1,2} \w+ \d{4})',
                                r'(\d{1,2}-\d{1,2}-\d{4})',
                                r'(\d{1,2}/\d{1,2}/\d{4})'
                            ]
                            
                            for pattern in date_patterns:
                                date_match = re.search(pattern, deadline_text)
                                if date_match:
                                    try:
                                        date_str = date_match.group(1)
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
            
            # Extract view count (keep original logic)
            view_elements = soup.find_all(text=lambda text: text and text.strip().isdigit())
            for element in view_elements:
                parent = element.parent if hasattr(element, 'parent') else None
                if parent and parent.name in ['p', 'span', 'div']:
                    siblings = parent.find_previous_siblings() + parent.find_next_siblings()
                    for sibling in siblings:
                        if sibling and hasattr(sibling, 'name') and sibling.name == 'svg':
                            try:
                                job_data['view_count'] = int(element.strip())
                                break
                            except ValueError:
                                continue
            
            # Extract job slug from URL
            parsed_url = urlparse(job_url)
            path_parts = parsed_url.path.strip('/').split('/')
            if len(path_parts) >= 2:
                job_data['slug'] = path_parts[-1]
            
            # Enhanced logging with extraction stats (including new fields)
            extracted_fields = []
            all_fields = ['title', 'company_name', 'description', 'requirements', 'category', 'job_type', 
                         'experience_level', 'apply_url', 'position_level', 'education_level', 
                         'salary_min', 'salary_max', 'published_date']
            for field in all_fields:
                if job_data.get(field) and str(job_data.get(field)).strip():
                    extracted_fields.append(field)
            
            job_title = job_data.get('title', 'Unknown Job')
            if len(extracted_fields) >= 5:
                logger.info(f"✅ Successfully extracted {job_title} ({len(extracted_fields)} fields)")
            elif len(extracted_fields) >= 3:
                logger.info(f"⚠️  Partially extracted {job_title} ({len(extracted_fields)} fields)")
            else:
                logger.warning(f"❌ Minimal extraction {job_title} ({len(extracted_fields)} fields)")
            
        except Exception as e:
            logger.error(f"Error extracting job details: {e}")
        
        return job_data
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove null characters that cause database errors
        text = text.replace('\x00', '')
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters except common Unicode
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