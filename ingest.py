"""
Data ingestion system for OpsPilot
Handles CSV parsing, text extraction, and data validation
"""

import pandas as pd
import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

# Conditional imports
try:
    from llm import LLMEngine
    from prompts import PROMPTS
    LLM_AVAILABLE = True
except ImportError as e:
    print(f"Warning: LLM features not available: {e}")
    LLM_AVAILABLE = False
    LLMEngine = None
    PROMPTS = {}

class DataIngester:
    """Handles data ingestion from CSV and text sources"""
    
    def __init__(self):
        self.llm = LLMEngine() if LLM_AVAILABLE else None
        self.required_fields = [
            'task_id', 'title', 'owner', 'status', 'start_date', 
            'due_date', 'dependency_ids', 'priority', 'effort', 'notes'
        ]
        self.status_values = ['todo', 'in_progress', 'blocked', 'done']
        self.priority_values = ['low', 'med', 'high']
    
    def parse_csv(self, csv_content: str) -> List[Dict[str, Any]]:
        """Parse CSV content into structured rows"""
        try:
            # Read CSV from string
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content))
            
            rows = []
            for _, row in df.iterrows():
                parsed_row = self._parse_row(row.to_dict())
                if parsed_row:
                    rows.append(parsed_row)
            
            print(f"Parsed {len(rows)} rows from CSV")
            return rows
            
        except Exception as e:
            print(f"Error parsing CSV: {e}")
            return []
    
    def parse_text(self, text_content: str) -> List[Dict[str, Any]]:
        """Parse free-form text using LLM to extract structured data"""
        try:
            if not text_content.strip():
                return []
            
            if not LLM_AVAILABLE or not self.llm:
                print("LLM not available - skipping text parsing")
                return []
            
            # Use LLM to extract structured data
            prompt = PROMPTS.get('text_to_rows', 'Extract tasks from: {raw_text}').format(raw_text=text_content)
            response = self.llm.run_llm(prompt, max_new_tokens=512, temperature=0.1)
            
            # Parse JSON response
            rows = self._parse_llm_response(response)
            print(f"Parsed {len(rows)} rows from text using LLM")
            return rows
            
        except Exception as e:
            print(f"Error parsing text: {e}")
            return []
    
    def _parse_row(self, row_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single row with validation and normalization"""
        try:
            # Normalize column names (case insensitive)
            normalized_row = {}
            for key, value in row_dict.items():
                normalized_key = key.lower().strip().replace(' ', '_')
                normalized_row[normalized_key] = value
            
            # Extract and validate fields
            task_id = self._extract_task_id(normalized_row)
            title = self._extract_title(normalized_row)
            owner = self._extract_owner(normalized_row)
            status = self._extract_status(normalized_row)
            start_date = self._extract_date(normalized_row, 'start_date')
            due_date = self._extract_date(normalized_row, 'due_date')
            dependency_ids = self._extract_dependencies(normalized_row)
            priority = self._extract_priority(normalized_row)
            effort = self._extract_effort(normalized_row)
            notes = self._extract_notes(normalized_row)
            
            # Validate required fields
            if not task_id or not title or not owner:
                return None
            
            return {
                'task_id': task_id,
                'title': title,
                'owner': owner,
                'status': status,
                'start_date': start_date,
                'due_date': due_date,
                'dependency_ids': dependency_ids,
                'priority': priority,
                'effort': effort,
                'notes': notes
            }
            
        except Exception as e:
            print(f"Error parsing row: {e}")
            return None
    
    def _parse_llm_response(self, response: str) -> List[Dict[str, Any]]:
        """Parse LLM response and extract JSON array"""
        try:
            # Extract JSON from response
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                
                if isinstance(data, list):
                    rows = []
                    for item in data:
                        if isinstance(item, dict):
                            parsed_row = self._parse_row(item)
                            if parsed_row:
                                rows.append(parsed_row)
                    return rows
            
            return []
            
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            return []
    
    def _extract_task_id(self, row: Dict[str, Any]) -> str:
        """Extract and validate task_id"""
        task_id = row.get('task_id', '')
        if not task_id:
            # Generate a slug from title
            title = row.get('title', '')
            if title:
                task_id = self._slugify(title)
            else:
                task_id = str(uuid.uuid4())[:8]
        return str(task_id).strip()
    
    def _extract_title(self, row: Dict[str, Any]) -> str:
        """Extract and validate title"""
        title = row.get('title', '')
        if not title:
            title = row.get('name', '')
        return str(title).strip() if title else ''
    
    def _extract_owner(self, row: Dict[str, Any]) -> str:
        """Extract and validate owner"""
        owner = row.get('owner', '')
        if not owner:
            owner = row.get('assignee', '')
        return str(owner).strip() if owner else 'unknown'
    
    def _extract_status(self, row: Dict[str, Any]) -> str:
        """Extract and validate status"""
        status = str(row.get('status', '')).lower().strip()
        if status not in self.status_values:
            # Try to map common status values
            status_mapping = {
                'pending': 'todo',
                'open': 'todo',
                'active': 'in_progress',
                'working': 'in_progress',
                'stuck': 'blocked',
                'completed': 'done',
                'finished': 'done',
                'closed': 'done'
            }
            status = status_mapping.get(status, 'todo')
        return status
    
    def _extract_date(self, row: Dict[str, Any], field: str) -> Optional[str]:
        """Extract and validate date field"""
        date_value = row.get(field, '')
        if not date_value or pd.isna(date_value):
            return None
        
        try:
            # Try to parse various date formats
            date_str = str(date_value).strip()
            if date_str:
                # Parse and reformat to YYYY-MM-DD
                parsed_date = pd.to_datetime(date_str)
                return parsed_date.strftime('%Y-%m-%d')
        except:
            pass
        
        return None
    
    def _extract_dependencies(self, row: Dict[str, Any]) -> List[str]:
        """Extract and validate dependency_ids"""
        deps = row.get('dependency_ids', '')
        if not deps or pd.isna(deps):
            return []
        
        try:
            if isinstance(deps, str):
                # Split by common separators
                deps_list = re.split(r'[,;|\s]+', deps)
                return [d.strip() for d in deps_list if d.strip()]
            elif isinstance(deps, list):
                return [str(d).strip() for d in deps if str(d).strip()]
        except:
            pass
        
        return []
    
    def _extract_priority(self, row: Dict[str, Any]) -> str:
        """Extract and validate priority"""
        priority = str(row.get('priority', '')).lower().strip()
        if priority not in self.priority_values:
            # Try to map common priority values
            priority_mapping = {
                '1': 'low',
                '2': 'med',
                '3': 'high',
                'low': 'low',
                'medium': 'med',
                'high': 'high',
                'urgent': 'high',
                'critical': 'high'
            }
            priority = priority_mapping.get(priority, 'med')
        return priority
    
    def _extract_effort(self, row: Dict[str, Any]) -> int:
        """Extract and validate effort (1-5 scale)"""
        effort = row.get('effort', 3)
        try:
            effort_int = int(effort)
            return max(1, min(5, effort_int))  # Clamp to 1-5
        except:
            return 3  # Default to medium effort
    
    def _extract_notes(self, row: Dict[str, Any]) -> str:
        """Extract notes field"""
        notes = row.get('notes', '')
        if not notes:
            notes = row.get('description', '')
        return str(notes).strip() if notes else ''
    
    def _slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug"""
        # Convert to lowercase and replace spaces/special chars
        slug = re.sub(r'[^\w\s-]', '', text.lower())
        slug = re.sub(r'[-\s]+', '-', slug)
        return slug[:50]  # Limit length
    
    def validate_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and clean parsed rows"""
        validated_rows = []
        
        for row in rows:
            if self._is_valid_row(row):
                validated_rows.append(row)
            else:
                print(f"Skipping invalid row: {row.get('task_id', 'unknown')}")
        
        return validated_rows
    
    def _is_valid_row(self, row: Dict[str, Any]) -> bool:
        """Check if a row has required fields and valid values"""
        # Check required fields
        if not row.get('task_id') or not row.get('title') or not row.get('owner'):
            return False
        
        # Check status
        if row.get('status') not in self.status_values:
            return False
        
        # Check priority
        if row.get('priority') not in self.priority_values:
            return False
        
        # Check effort
        effort = row.get('effort', 0)
        if not isinstance(effort, int) or effort < 1 or effort > 5:
            return False
        
        return True
