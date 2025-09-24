"""
Risk forecasting system for OpsPilot
Predicts tasks likely to slip based on heuristics and patterns
"""

import networkx as nx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import math

class RiskForecaster:
    """Forecasts risks in workflow execution using heuristics"""
    
    def __init__(self):
        self.risk_factors = [
            'dependency_depth',
            'owner_overload',
            'unresolved_blockers',
            'historical_patterns',
            'deadline_pressure',
            'effort_complexity'
        ]
    
    def forecast_risks(self, graph: nx.DiGraph, rows: List[Dict[str, Any]], 
                      metrics: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Forecast risks for all tasks in the workflow"""
        risks = []
        
        if not graph or not rows:
            return risks
        
        # Create task lookup
        task_lookup = {row['task_id']: row for row in rows}
        
        # Calculate risk scores for each task
        for row in rows:
            task_id = row['task_id']
            
            # Skip completed tasks
            if row['status'] == 'done':
                continue
            
            risk_score = self._calculate_risk_score(
                task_id, row, graph, task_lookup, metrics, settings
            )
            
            if risk_score > 0.3:  # Only include tasks with significant risk
                risk_reasons = self._identify_risk_factors(
                    task_id, row, graph, task_lookup, metrics, settings
                )
                
                risks.append({
                    'task_id': task_id,
                    'title': row['title'],
                    'owner': row['owner'],
                    'status': row['status'],
                    'risk_score': risk_score,
                    'risk_level': self._categorize_risk_level(risk_score),
                    'reasons': risk_reasons,
                    'due_date': row.get('due_date'),
                    'priority': row['priority'],
                    'effort': row.get('effort', 3)
                })
        
        # Sort by risk score (highest first)
        risks.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return risks
    
    def _calculate_risk_score(self, task_id: str, task: Dict[str, Any], 
                            graph: nx.DiGraph, task_lookup: Dict[str, Any],
                            metrics: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """Calculate comprehensive risk score for a task"""
        risk_factors = []
        
        # 1. Dependency depth risk
        depth_risk = self._calculate_dependency_depth_risk(task_id, graph, task_lookup)
        risk_factors.append(('dependency_depth', depth_risk, 0.25))
        
        # 2. Owner overload risk
        owner_risk = self._calculate_owner_overload_risk(task, metrics)
        risk_factors.append(('owner_overload', owner_risk, 0.20))
        
        # 3. Unresolved blockers risk
        blocker_risk = self._calculate_blocker_risk(task, graph, task_lookup)
        risk_factors.append(('unresolved_blockers', blocker_risk, 0.20))
        
        # 4. Deadline pressure risk
        deadline_risk = self._calculate_deadline_pressure_risk(task, settings)
        risk_factors.append(('deadline_pressure', deadline_risk, 0.15))
        
        # 5. Effort complexity risk
        complexity_risk = self._calculate_effort_complexity_risk(task)
        risk_factors.append(('effort_complexity', complexity_risk, 0.10))
        
        # 6. Historical patterns risk
        historical_risk = self._calculate_historical_pattern_risk(task, metrics)
        risk_factors.append(('historical_patterns', historical_risk, 0.10))
        
        # Calculate weighted risk score
        total_score = 0.0
        total_weight = 0.0
        
        for factor_name, score, weight in risk_factors:
            total_score += score * weight
            total_weight += weight
        
        final_score = total_score / total_weight if total_weight > 0 else 0.0
        
        # Apply priority multiplier
        priority_multiplier = {'high': 1.5, 'med': 1.0, 'low': 0.7}.get(task['priority'], 1.0)
        final_score *= priority_multiplier
        
        return min(final_score, 1.0)  # Cap at 1.0
    
    def _calculate_dependency_depth_risk(self, task_id: str, graph: nx.DiGraph, 
                                      task_lookup: Dict[str, Any]) -> float:
        """Calculate risk based on dependency depth"""
        try:
            # Find longest path to this task
            predecessors = list(nx.ancestors(graph, task_id))
            depth = len(predecessors)
            
            # Check if any dependencies are blocked or delayed
            blocked_deps = 0
            for dep_id in predecessors:
                if dep_id in task_lookup:
                    dep_task = task_lookup[dep_id]
                    if dep_task['status'] in ['blocked', 'todo']:
                        blocked_deps += 1
            
            # Calculate depth risk (exponential growth)
            depth_risk = min(depth / 5.0, 1.0)  # Normalize to 0-1
            
            # Add blocked dependency penalty
            blocked_penalty = min(blocked_deps / max(depth, 1), 1.0)
            
            return min(depth_risk + blocked_penalty * 0.5, 1.0)
            
        except:
            return 0.0
    
    def _calculate_owner_overload_risk(self, task: Dict[str, Any], metrics: Dict[str, Any]) -> float:
        """Calculate risk based on owner workload"""
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        owner_stats = owner_load.get('owner_stats', {})
        
        task_owner = task['owner']
        owner_load_score = load_scores.get(task_owner, 0)
        owner_stat = owner_stats.get(task_owner, {})
        
        # Base risk from owner load
        max_load = max(load_scores.values()) if load_scores else 1
        load_risk = owner_load_score / max_load if max_load > 0 else 0
        
        # Additional risk from owner's task characteristics
        in_progress_tasks = owner_stat.get('in_progress', 0)
        blocked_tasks = owner_stat.get('blocked', 0)
        high_priority_tasks = owner_stat.get('high_priority', 0)
        
        # Penalty for overloaded owner
        overload_penalty = 0
        if in_progress_tasks > 3:
            overload_penalty += 0.2
        if blocked_tasks > 1:
            overload_penalty += 0.3
        if high_priority_tasks > 2:
            overload_penalty += 0.2
        
        return min(load_risk + overload_penalty, 1.0)
    
    def _calculate_blocker_risk(self, task: Dict[str, Any], graph: nx.DiGraph, 
                              task_lookup: Dict[str, Any]) -> float:
        """Calculate risk from unresolved blockers"""
        if task['status'] == 'blocked':
            return 1.0  # Maximum risk for blocked tasks
        
        # Check if any dependencies are blocked
        task_id = task['task_id']
        predecessors = list(graph.predecessors(task_id))
        
        blocked_dependencies = 0
        for dep_id in predecessors:
            if dep_id in task_lookup:
                dep_task = task_lookup[dep_id]
                if dep_task['status'] == 'blocked':
                    blocked_dependencies += 1
        
        # Risk increases with number of blocked dependencies
        if predecessors:
            blocker_risk = blocked_dependencies / len(predecessors)
        else:
            blocker_risk = 0
        
        return blocker_risk
    
    def _calculate_deadline_pressure_risk(self, task: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """Calculate risk from deadline pressure"""
        due_date = task.get('due_date')
        if not due_date:
            return 0.0
        
        try:
            due_date_obj = datetime.strptime(due_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            days_until_due = (due_date_obj - today).days
            
            # Risk increases as deadline approaches
            if days_until_due < 0:
                return 1.0  # Overdue
            elif days_until_due <= 3:
                return 0.9  # Very soon
            elif days_until_due <= 7:
                return 0.7  # Soon
            elif days_until_due <= 14:
                return 0.4  # Moderate
            else:
                return 0.1  # Low pressure
            
        except:
            return 0.0
    
    def _calculate_effort_complexity_risk(self, task: Dict[str, Any]) -> float:
        """Calculate risk based on task effort and complexity"""
        effort = task.get('effort', 3)
        
        # Higher effort tasks are riskier
        effort_risk = (effort - 1) / 4.0  # Normalize 1-5 to 0-1
        
        # Additional risk for high-effort tasks
        if effort >= 4:
            return min(effort_risk + 0.2, 1.0)
        
        return effort_risk
    
    def _calculate_historical_pattern_risk(self, task: Dict[str, Any], metrics: Dict[str, Any]) -> float:
        """Calculate risk based on historical patterns"""
        # This is a simplified version - in a real system, you'd analyze historical data
        
        # Check if task is aging
        aging_metrics = metrics.get('aging', {})
        aging_tasks = aging_metrics.get('aging_tasks', [])
        
        for aging_task in aging_tasks:
            if aging_task['task_id'] == task['task_id']:
                # Task is already aging, high risk
                return 0.8
        
        # Check owner's historical performance (simplified)
        owner = task['owner']
        owner_load = metrics.get('owner_load', {})
        owner_stats = owner_load.get('owner_stats', {})
        owner_stat = owner_stats.get(owner, {})
        
        # Risk based on owner's current workload patterns
        overdue_tasks = owner_stat.get('overdue', 0)
        blocked_tasks = owner_stat.get('blocked', 0)
        
        if overdue_tasks > 0:
            return 0.6  # Owner has overdue tasks
        elif blocked_tasks > 1:
            return 0.4  # Owner has multiple blocked tasks
        
        return 0.0
    
    def _identify_risk_factors(self, task_id: str, task: Dict[str, Any], 
                             graph: nx.DiGraph, task_lookup: Dict[str, Any],
                             metrics: Dict[str, Any], settings: Dict[str, Any]) -> List[str]:
        """Identify specific risk factors for a task"""
        risk_factors = []
        
        # Check dependency depth
        predecessors = list(nx.ancestors(graph, task_id))
        if len(predecessors) > 3:
            risk_factors.append(f"Deep dependency chain ({len(predecessors)} dependencies)")
        
        # Check for blocked dependencies
        blocked_deps = [dep for dep in predecessors if dep in task_lookup and task_lookup[dep]['status'] == 'blocked']
        if blocked_deps:
            risk_factors.append(f"Blocked dependencies ({len(blocked_deps)} tasks)")
        
        # Check owner overload
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        owner_stats = owner_load.get('owner_stats', {})
        
        task_owner = task['owner']
        owner_load_score = load_scores.get(task_owner, 0)
        owner_stat = owner_stats.get(task_owner, {})
        
        if owner_load_score > 5:
            risk_factors.append(f"Owner overload (load score: {owner_load_score:.1f})")
        
        if owner_stat.get('in_progress', 0) > 3:
            risk_factors.append(f"Owner has {owner_stat['in_progress']} tasks in progress")
        
        if owner_stat.get('blocked', 0) > 0:
            risk_factors.append(f"Owner has {owner_stat['blocked']} blocked tasks")
        
        # Check deadline pressure
        due_date = task.get('due_date')
        if due_date:
            try:
                due_date_obj = datetime.strptime(due_date, '%Y-%m-%d').date()
                today = datetime.now().date()
                days_until_due = (due_date_obj - today).days
                
                if days_until_due < 0:
                    risk_factors.append(f"Overdue by {abs(days_until_due)} days")
                elif days_until_due <= 3:
                    risk_factors.append(f"Due in {days_until_due} days")
            except:
                pass
        
        # Check task complexity
        effort = task.get('effort', 3)
        if effort >= 4:
            risk_factors.append(f"High effort task ({effort}/5)")
        
        # Check if task is aging
        aging_metrics = metrics.get('aging', {})
        aging_tasks = aging_metrics.get('aging_tasks', [])
        for aging_task in aging_tasks:
            if aging_task['task_id'] == task_id:
                risk_factors.append(f"Task aging ({aging_task['days_in_progress']} days in progress)")
                break
        
        return risk_factors
    
    def _categorize_risk_level(self, risk_score: float) -> str:
        """Categorize risk score into risk level"""
        if risk_score >= 0.8:
            return "Critical"
        elif risk_score >= 0.6:
            return "High"
        elif risk_score >= 0.4:
            return "Medium"
        elif risk_score >= 0.2:
            return "Low"
        else:
            return "Minimal"
    
    def get_risk_summary(self, risks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics for risk forecast"""
        if not risks:
            return {
                'total_risks': 0,
                'by_level': {},
                'by_owner': {},
                'avg_risk_score': 0,
                'critical_count': 0
            }
        
        # Count by risk level
        by_level = {}
        by_owner = {}
        risk_scores = []
        critical_count = 0
        
        for risk in risks:
            level = risk['risk_level']
            owner = risk['owner']
            score = risk['risk_score']
            
            by_level[level] = by_level.get(level, 0) + 1
            by_owner[owner] = by_owner.get(owner, 0) + 1
            risk_scores.append(score)
            
            if level == "Critical":
                critical_count += 1
        
        avg_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
        
        return {
            'total_risks': len(risks),
            'by_level': by_level,
            'by_owner': by_owner,
            'avg_risk_score': avg_risk_score,
            'critical_count': critical_count,
            'max_risk_score': max(risk_scores) if risk_scores else 0,
            'min_risk_score': min(risk_scores) if risk_scores else 0
        }
