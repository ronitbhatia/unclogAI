"""
SQLite persistence layer for OpsPilot
Handles storage and retrieval of analysis runs
"""

import sqlite3
import json
import pickle
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

class StorageManager:
    """Manages SQLite storage for OpsPilot analysis runs"""
    
    def __init__(self, db_path: str = "ops_pilot.db"):
        self.db_path = db_path
    
    def _get_connection(self):
        """Get a new database connection (thread-safe)"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def initialize(self):
        """Initialize database and create tables"""
        try:
            conn = self._get_connection()
            self._create_tables(conn)
            conn.close()
            print(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise
    
    def _create_tables(self, conn):
        """Create necessary tables"""
        cursor = conn.cursor()
        
        # Runs table - stores analysis runs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                settings TEXT,
                raw_csv TEXT,
                raw_text TEXT,
                num_tasks INTEGER,
                num_owners INTEGER,
                num_bottlenecks INTEGER,
                num_risks INTEGER,
                run_report_md TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tasks table - stores individual tasks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                task_id TEXT NOT NULL,
                title TEXT,
                owner TEXT,
                status TEXT,
                start_date TEXT,
                due_date TEXT,
                priority TEXT,
                effort INTEGER,
                notes TEXT,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        # Dependencies table - stores task dependencies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                task_id TEXT NOT NULL,
                dependency_id TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        # Bottlenecks table - stores detected bottlenecks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bottlenecks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                task_id TEXT NOT NULL,
                title TEXT,
                owner TEXT,
                type TEXT,
                score REAL,
                reason TEXT,
                details TEXT,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        # Recommendations table - stores recommendations
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                task_id TEXT NOT NULL,
                title TEXT,
                owner TEXT,
                recommendation_title TEXT,
                type TEXT,
                priority TEXT,
                rationale TEXT,
                expected_effect TEXT,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        # Risks table - stores risk forecasts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS risks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                task_id TEXT NOT NULL,
                title TEXT,
                owner TEXT,
                status TEXT,
                risk_level TEXT,
                risk_score REAL,
                due_date TEXT,
                priority TEXT,
                effort INTEGER,
                reasons TEXT,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        # Metrics table - stores computed metrics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                metric_type TEXT,
                metric_name TEXT,
                metric_value TEXT,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
        """)
        
        conn.commit()
    
    def save_run(self, state: Any) -> int:
        """Save a complete analysis run to the database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Insert run record
            run_data = {
                'timestamp': state.timestamp.isoformat() if state.timestamp else datetime.now().isoformat(),
                'settings': json.dumps(state.settings) if hasattr(state, 'settings') else None,
                'raw_csv': getattr(state, 'raw_csv', None),
                'raw_text': getattr(state, 'raw_text', None),
                'num_tasks': len(state.rows) if hasattr(state, 'rows') else 0,
                'num_owners': len(set(row['owner'] for row in state.rows)) if hasattr(state, 'rows') else 0,
                'num_bottlenecks': len(state.bottlenecks) if hasattr(state, 'bottlenecks') else 0,
                'num_risks': len(state.risk_forecast) if hasattr(state, 'risk_forecast') else 0,
                'run_report_md': getattr(state, 'run_report_md', '')
            }
            
            cursor.execute("""
                INSERT INTO runs (timestamp, settings, raw_csv, raw_text, num_tasks, num_owners, 
                                num_bottlenecks, num_risks, run_report_md)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_data['timestamp'],
                run_data['settings'],
                run_data['raw_csv'],
                run_data['raw_text'],
                run_data['num_tasks'],
                run_data['num_owners'],
                run_data['num_bottlenecks'],
                run_data['num_risks'],
                run_data['run_report_md']
            ))
            
            run_id = cursor.lastrowid
            
            # Save tasks
            if hasattr(state, 'rows') and state.rows:
                self._save_tasks(cursor, run_id, state.rows)
            
            # Save dependencies
            if hasattr(state, 'rows') and state.rows:
                self._save_dependencies(cursor, run_id, state.rows)
            
            # Save bottlenecks
            if hasattr(state, 'bottlenecks') and state.bottlenecks:
                self._save_bottlenecks(cursor, run_id, state.bottlenecks)
            
            # Save recommendations
            if hasattr(state, 'recommendations') and state.recommendations:
                self._save_recommendations(cursor, run_id, state.recommendations)
            
            # Save risks
            if hasattr(state, 'risk_forecast') and state.risk_forecast:
                self._save_risks(cursor, run_id, state.risk_forecast)
            
            # Save metrics
            if hasattr(state, 'metrics') and state.metrics:
                self._save_metrics(cursor, run_id, state.metrics)
            
            conn.commit()
            conn.close()
            print(f"Saved run {run_id} to database")
            return run_id
            
        except Exception as e:
            print(f"Error saving run: {e}")
            conn.rollback()
            conn.close()
            raise
    
    def _save_tasks(self, cursor, run_id: int, rows: List[Dict[str, Any]]):
        """Save tasks to database"""
        for row in rows:
            cursor.execute("""
                INSERT INTO tasks (run_id, task_id, title, owner, status, start_date, 
                                 due_date, priority, effort, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                row['task_id'],
                row['title'],
                row['owner'],
                row['status'],
                row.get('start_date'),
                row.get('due_date'),
                row['priority'],
                row.get('effort', 3),
                row.get('notes', '')
            ))
    
    def _save_dependencies(self, cursor, run_id: int, rows: List[Dict[str, Any]]):
        """Save dependencies to database"""
        for row in rows:
            task_id = row['task_id']
            dependencies = row.get('dependency_ids', [])
            
            for dep_id in dependencies:
                cursor.execute("""
                    INSERT INTO dependencies (run_id, task_id, dependency_id)
                    VALUES (?, ?, ?)
                """, (run_id, task_id, dep_id))
    
    def _save_bottlenecks(self, cursor, run_id: int, bottlenecks: List[Dict[str, Any]]):
        """Save bottlenecks to database"""
        for bottleneck in bottlenecks:
            cursor.execute("""
                INSERT INTO bottlenecks (run_id, task_id, title, owner, type, score, reason, details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                bottleneck['task_id'],
                bottleneck['title'],
                bottleneck['owner'],
                bottleneck['type'],
                bottleneck['score'],
                bottleneck['reason'],
                json.dumps(bottleneck.get('details', {}))
            ))
    
    def _save_recommendations(self, cursor, run_id: int, recommendations: List[Dict[str, Any]]):
        """Save recommendations to database"""
        for rec_group in recommendations:
            task_id = rec_group['task_id']
            title = rec_group['title']
            owner = rec_group['owner']
            
            for rec in rec_group.get('recommendations', []):
                cursor.execute("""
                    INSERT INTO recommendations (run_id, task_id, title, owner, recommendation_title,
                                              type, priority, rationale, expected_effect)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    run_id,
                    task_id,
                    title,
                    owner,
                    rec['title'],
                    rec['type'],
                    rec['priority'],
                    rec['rationale'],
                    rec['expected_effect']
                ))
    
    def _save_risks(self, cursor, run_id: int, risks: List[Dict[str, Any]]):
        """Save risks to database"""
        for risk in risks:
            cursor.execute("""
                INSERT INTO risks (run_id, task_id, title, owner, status, risk_level, risk_score,
                                 due_date, priority, effort, reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                risk['task_id'],
                risk['title'],
                risk['owner'],
                risk['status'],
                risk['risk_level'],
                risk['risk_score'],
                risk.get('due_date'),
                risk['priority'],
                risk['effort'],
                json.dumps(risk['reasons'])
            ))
    
    def _save_metrics(self, cursor, run_id: int, metrics: Dict[str, Any]):
        """Save metrics to database"""
        for metric_type, metric_data in metrics.items():
            if isinstance(metric_data, dict):
                for metric_name, metric_value in metric_data.items():
                    cursor.execute("""
                        INSERT INTO metrics (run_id, metric_type, metric_name, metric_value)
                        VALUES (?, ?, ?, ?)
                    """, (
                        run_id,
                        metric_type,
                        metric_name,
                        json.dumps(metric_value)
                    ))
    
    def load_run(self, run_id: int) -> Optional[Any]:
        """Load a complete analysis run from the database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Load run metadata
            cursor.execute("SELECT * FROM runs WHERE id = ?", (run_id,))
            run_row = cursor.fetchone()
            
            if not run_row:
                return None
            
            # Create state object
            from graph import WorkflowState
            state = WorkflowState()
            state.run_id = str(run_id)
            state.timestamp = datetime.fromisoformat(run_row['timestamp'])
            state.settings = json.loads(run_row['settings']) if run_row['settings'] else {}
            state.raw_csv = run_row['raw_csv']
            state.raw_text = run_row['raw_text']
            state.run_report_md = run_row['run_report_md']
            
            # Load tasks
            cursor.execute("SELECT * FROM tasks WHERE run_id = ?", (run_id,))
            task_rows = cursor.fetchall()
            state.rows = []
            
            for task_row in task_rows:
                task = {
                    'task_id': task_row['task_id'],
                    'title': task_row['title'],
                    'owner': task_row['owner'],
                    'status': task_row['status'],
                    'start_date': task_row['start_date'],
                    'due_date': task_row['due_date'],
                    'priority': task_row['priority'],
                    'effort': task_row['effort'],
                    'notes': task_row['notes'],
                    'dependency_ids': []
                }
                state.rows.append(task)
            
            # Load dependencies
            cursor.execute("SELECT * FROM dependencies WHERE run_id = ?", (run_id,))
            dep_rows = cursor.fetchall()
            
            for dep_row in dep_rows:
                task_id = dep_row['task_id']
                dep_id = dep_row['dependency_id']
                
                # Find the task and add dependency
                for task in state.rows:
                    if task['task_id'] == task_id:
                        task['dependency_ids'].append(dep_id)
                        break
            
            # Load bottlenecks
            cursor.execute("SELECT * FROM bottlenecks WHERE run_id = ?", (run_id,))
            bottleneck_rows = cursor.fetchall()
            state.bottlenecks = []
            
            for bottleneck_row in bottleneck_rows:
                bottleneck = {
                    'task_id': bottleneck_row['task_id'],
                    'title': bottleneck_row['title'],
                    'owner': bottleneck_row['owner'],
                    'type': bottleneck_row['type'],
                    'score': bottleneck_row['score'],
                    'reason': bottleneck_row['reason'],
                    'details': json.loads(bottleneck_row['details']) if bottleneck_row['details'] else {}
                }
                state.bottlenecks.append(bottleneck)
            
            # Load recommendations
            cursor.execute("SELECT * FROM recommendations WHERE run_id = ?", (run_id,))
            rec_rows = cursor.fetchall()
            state.recommendations = []
            
            # Group recommendations by task
            rec_by_task = {}
            for rec_row in rec_rows:
                task_id = rec_row['task_id']
                if task_id not in rec_by_task:
                    rec_by_task[task_id] = {
                        'task_id': task_id,
                        'title': rec_row['title'],
                        'owner': rec_row['owner'],
                        'recommendations': []
                    }
                
                rec_by_task[task_id]['recommendations'].append({
                    'title': rec_row['recommendation_title'],
                    'type': rec_row['type'],
                    'priority': rec_row['priority'],
                    'rationale': rec_row['rationale'],
                    'expected_effect': rec_row['expected_effect']
                })
            
            state.recommendations = list(rec_by_task.values())
            
            # Load risks
            cursor.execute("SELECT * FROM risks WHERE run_id = ?", (run_id,))
            risk_rows = cursor.fetchall()
            state.risk_forecast = []
            
            for risk_row in risk_rows:
                risk = {
                    'task_id': risk_row['task_id'],
                    'title': risk_row['title'],
                    'owner': risk_row['owner'],
                    'status': risk_row['status'],
                    'risk_level': risk_row['risk_level'],
                    'risk_score': risk_row['risk_score'],
                    'due_date': risk_row['due_date'],
                    'priority': risk_row['priority'],
                    'effort': risk_row['effort'],
                    'reasons': json.loads(risk_row['reasons']) if risk_row['reasons'] else []
                }
                state.risk_forecast.append(risk)
            
            # Load metrics
            cursor.execute("SELECT * FROM metrics WHERE run_id = ?", (run_id,))
            metric_rows = cursor.fetchall()
            state.metrics = {}
            
            for metric_row in metric_rows:
                metric_type = metric_row['metric_type']
                metric_name = metric_row['metric_name']
                metric_value = json.loads(metric_row['metric_value'])
                
                if metric_type not in state.metrics:
                    state.metrics[metric_type] = {}
                state.metrics[metric_type][metric_name] = metric_value
            
            conn.close()
            print(f"Loaded run {run_id} from database")
            return state
            
        except Exception as e:
            print(f"Error loading run: {e}")
            conn.close()
            return None
    
    def get_all_runs(self) -> List[Dict[str, Any]]:
        """Get all analysis runs"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, timestamp, num_tasks, num_owners, num_bottlenecks, num_risks, created_at
                FROM runs
                ORDER BY created_at DESC
            """)
            
            runs = []
            for row in cursor.fetchall():
                runs.append({
                    'id': row['id'],
                    'timestamp': row['timestamp'],
                    'num_tasks': row['num_tasks'],
                    'num_owners': row['num_owners'],
                    'num_bottlenecks': row['num_bottlenecks'],
                    'num_risks': row['num_risks'],
                    'created_at': row['created_at']
                })
            
            conn.close()
            return runs
            
        except Exception as e:
            print(f"Error getting runs: {e}")
            conn.close()
            return []
    
    def delete_run(self, run_id: int) -> bool:
        """Delete a specific run"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Delete in reverse order of dependencies
            cursor.execute("DELETE FROM metrics WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM risks WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM recommendations WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM bottlenecks WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM dependencies WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM tasks WHERE run_id = ?", (run_id,))
            cursor.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            
            conn.commit()
            conn.close()
            print(f"Deleted run {run_id}")
            return True
            
        except Exception as e:
            print(f"Error deleting run: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def get_run_statistics(self) -> Dict[str, Any]:
        """Get overall statistics about stored runs"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Total runs
            cursor.execute("SELECT COUNT(*) as total_runs FROM runs")
            total_runs = cursor.fetchone()['total_runs']
            
            # Total tasks across all runs
            cursor.execute("SELECT SUM(num_tasks) as total_tasks FROM runs")
            total_tasks = cursor.fetchone()['total_tasks'] or 0
            
            # Average bottlenecks per run
            cursor.execute("SELECT AVG(num_bottlenecks) as avg_bottlenecks FROM runs")
            avg_bottlenecks = cursor.fetchone()['avg_bottlenecks'] or 0
            
            # Average risks per run
            cursor.execute("SELECT AVG(num_risks) as avg_risks FROM runs")
            avg_risks = cursor.fetchone()['avg_risks'] or 0
            
            conn.close()
            return {
                'total_runs': total_runs,
                'total_tasks': total_tasks,
                'avg_bottlenecks_per_run': round(avg_bottlenecks, 2),
                'avg_risks_per_run': round(avg_risks, 2)
            }
            
        except Exception as e:
            print(f"Error getting statistics: {e}")
            conn.close()
            return {}
    
    def close(self):
        """Close database connection (no-op since we use per-operation connections)"""
        print("Database connections are managed per operation")
