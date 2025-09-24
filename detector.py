"""
Bottleneck detection algorithms for OpsPilot
Identifies workflow bottlenecks using various metrics and heuristics
"""

import networkx as nx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np

class BottleneckDetector:
    """Detects bottlenecks in workflow graphs using multiple algorithms"""
    
    def __init__(self):
        self.bottleneck_types = [
            'high_betweenness',
            'overloaded_owner',
            'aging_task',
            'dependency_chokepoint',
            'critical_path',
            'circular_dependency'
        ]
    
    def detect_bottlenecks(self, graph: nx.DiGraph, rows: List[Dict[str, Any]], 
                         metrics: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect all types of bottlenecks in the workflow"""
        bottlenecks = []
        
        if not graph or graph.number_of_nodes() == 0:
            return bottlenecks
        
        # Create task lookup for quick access
        task_lookup = {row['task_id']: row for row in rows}
        
        # 1. High betweenness centrality bottlenecks
        bottlenecks.extend(self._detect_centrality_bottlenecks(graph, task_lookup, metrics))
        
        # 2. Owner overload bottlenecks
        bottlenecks.extend(self._detect_owner_overload_bottlenecks(graph, task_lookup, metrics, settings))
        
        # 3. Aging task bottlenecks
        bottlenecks.extend(self._detect_aging_bottlenecks(graph, task_lookup, metrics, settings))
        
        # 4. Dependency chokepoint bottlenecks
        bottlenecks.extend(self._detect_dependency_bottlenecks(graph, task_lookup, metrics))
        
        # 5. Critical path bottlenecks
        bottlenecks.extend(self._detect_critical_path_bottlenecks(graph, task_lookup, metrics))
        
        # 6. Circular dependency bottlenecks
        bottlenecks.extend(self._detect_circular_dependencies(graph, task_lookup))
        
        # Sort by score (highest first)
        bottlenecks.sort(key=lambda x: x['score'], reverse=True)
        
        return bottlenecks
    
    def _detect_centrality_bottlenecks(self, graph: nx.DiGraph, task_lookup: Dict[str, Any], 
                                     metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bottlenecks based on betweenness centrality"""
        bottlenecks = []
        
        centrality_metrics = metrics.get('centrality', {})
        betweenness = centrality_metrics.get('betweenness', {})
        
        if not betweenness:
            return bottlenecks
        
        # Find nodes with high betweenness centrality
        max_betweenness = max(betweenness.values()) if betweenness else 0
        threshold = max_betweenness * 0.7  # Top 30% of betweenness scores
        
        for task_id, score in betweenness.items():
            if score >= threshold and task_id in task_lookup:
                task = task_lookup[task_id]
                
                # Calculate normalized score
                normalized_score = score / max_betweenness if max_betweenness > 0 else 0
                
                bottlenecks.append({
                    'task_id': task_id,
                    'title': task['title'],
                    'owner': task['owner'],
                    'type': 'high_betweenness',
                    'score': normalized_score,
                    'reason': f"High betweenness centrality ({score:.3f}) - many paths flow through this task",
                    'details': {
                        'betweenness_score': score,
                        'normalized_score': normalized_score,
                        'predecessors': list(graph.predecessors(task_id)),
                        'successors': list(graph.successors(task_id))
                    }
                })
        
        return bottlenecks
    
    def _detect_owner_overload_bottlenecks(self, graph: nx.DiGraph, task_lookup: Dict[str, Any], 
                                         metrics: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bottlenecks due to owner overload"""
        bottlenecks = []
        
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        owner_stats = owner_load.get('owner_stats', {})
        
        if not load_scores:
            return bottlenecks
        
        # Find overloaded owners
        max_load = max(load_scores.values()) if load_scores else 0
        avg_load = sum(load_scores.values()) / len(load_scores) if load_scores else 0
        threshold = max(avg_load * 1.5, max_load * 0.7)  # 50% above average or 70% of max
        
        for owner, load_score in load_scores.items():
            if load_score >= threshold:
                owner_stat = owner_stats.get(owner, {})
                
                # Find tasks for this owner
                owner_tasks = [task for task in task_lookup.values() if task['owner'] == owner]
                
                for task in owner_tasks:
                    task_id = task['task_id']
                    
                    # Calculate task-specific overload score
                    task_score = self._calculate_task_overload_score(task, owner_stat, load_score, max_load)
                    
                    if task_score > 0.3:  # Only include significant overloads
                        bottlenecks.append({
                            'task_id': task_id,
                            'title': task['title'],
                            'owner': owner,
                            'type': 'overloaded_owner',
                            'score': task_score,
                            'reason': f"Owner {owner} is overloaded (load score: {load_score:.2f})",
                            'details': {
                                'owner_load_score': load_score,
                                'owner_stats': owner_stat,
                                'task_priority': task['priority'],
                                'task_effort': task.get('effort', 3),
                                'task_status': task['status']
                            }
                        })
        
        return bottlenecks
    
    def _detect_aging_bottlenecks(self, graph: nx.DiGraph, task_lookup: Dict[str, Any], 
                                metrics: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bottlenecks due to aging tasks"""
        bottlenecks = []
        
        aging_metrics = metrics.get('aging', {})
        aging_tasks = aging_metrics.get('aging_tasks', [])
        
        if not aging_tasks:
            return bottlenecks
        
        # Calculate aging scores
        max_days = max(task['days_in_progress'] for task in aging_tasks) if aging_tasks else 1
        
        for aging_task in aging_tasks:
            task_id = aging_task['task_id']
            days_in_progress = aging_task['days_in_progress']
            
            # Calculate normalized aging score
            aging_score = min(days_in_progress / max_days, 1.0)
            
            # Weight by priority and effort
            priority_weight = {'high': 1.5, 'med': 1.0, 'low': 0.7}.get(aging_task['priority'], 1.0)
            effort_weight = aging_task['effort'] / 5.0
            
            final_score = aging_score * priority_weight * effort_weight
            
            bottlenecks.append({
                'task_id': task_id,
                'title': aging_task['title'],
                'owner': aging_task['owner'],
                'type': 'aging_task',
                'score': final_score,
                'reason': f"Task stuck in progress for {days_in_progress} days",
                'details': {
                    'days_in_progress': days_in_progress,
                    'priority': aging_task['priority'],
                    'effort': aging_task['effort'],
                    'aging_threshold': aging_metrics.get('aging_threshold', 5)
                }
            })
        
        return bottlenecks
    
    def _detect_dependency_bottlenecks(self, graph: nx.DiGraph, task_lookup: Dict[str, Any], 
                                     metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bottlenecks due to dependency chokepoints"""
        bottlenecks = []
        
        dependency_metrics = metrics.get('dependencies', {})
        fan_in = dependency_metrics.get('fan_in', {})
        fan_out = dependency_metrics.get('fan_out', {})
        
        if not fan_in or not fan_out:
            return bottlenecks
        
        # Find high fan-in nodes (many dependencies)
        max_fan_in = max(fan_in.values()) if fan_in else 0
        high_fan_in_threshold = max(2, max_fan_in * 0.6)
        
        for task_id, fan_in_count in fan_in.items():
            if fan_in_count >= high_fan_in_threshold and task_id in task_lookup:
                task = task_lookup[task_id]
                
                # Calculate chokepoint score
                chokepoint_score = fan_in_count / max_fan_in if max_fan_in > 0 else 0
                
                bottlenecks.append({
                    'task_id': task_id,
                    'title': task['title'],
                    'owner': task['owner'],
                    'type': 'dependency_chokepoint',
                    'score': chokepoint_score,
                    'reason': f"High dependency fan-in ({fan_in_count} dependencies) creates chokepoint",
                    'details': {
                        'fan_in_count': fan_in_count,
                        'fan_out_count': fan_out.get(task_id, 0),
                        'dependencies': list(graph.predecessors(task_id)),
                        'dependents': list(graph.successors(task_id))
                    }
                })
        
        return bottlenecks
    
    def _detect_critical_path_bottlenecks(self, graph: nx.DiGraph, task_lookup: Dict[str, Any], 
                                        metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bottlenecks on critical paths"""
        bottlenecks = []
        
        dependency_metrics = metrics.get('dependencies', {})
        critical_paths = dependency_metrics.get('critical_paths', [])
        
        if not critical_paths:
            return bottlenecks
        
        # Score tasks based on their position in critical paths
        for task_id, betweenness_score in critical_paths[:5]:  # Top 5 critical tasks
            if task_id in task_lookup:
                task = task_lookup[task_id]
                
                # Calculate critical path score
                critical_score = betweenness_score
                
                bottlenecks.append({
                    'task_id': task_id,
                    'title': task['title'],
                    'owner': task['owner'],
                    'type': 'critical_path',
                    'score': critical_score,
                    'reason': f"Task is on critical path (betweenness: {betweenness_score:.3f})",
                    'details': {
                        'betweenness_score': betweenness_score,
                        'path_position': 'critical',
                        'predecessors': list(graph.predecessors(task_id)),
                        'successors': list(graph.successors(task_id))
                    }
                })
        
        return bottlenecks
    
    def _detect_circular_dependencies(self, graph: nx.DiGraph, task_lookup: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect circular dependencies"""
        bottlenecks = []
        
        try:
            # Find strongly connected components (circular dependencies)
            scc = list(nx.strongly_connected_components(graph))
            circular_deps = [list(component) for component in scc if len(component) > 1]
            
            for cycle in circular_deps:
                # Each task in the cycle is a bottleneck
                for task_id in cycle:
                    if task_id in task_lookup:
                        task = task_lookup[task_id]
                        
                        bottlenecks.append({
                            'task_id': task_id,
                            'title': task['title'],
                            'owner': task['owner'],
                            'type': 'circular_dependency',
                            'score': 0.8,  # High score for circular dependencies
                            'reason': f"Task is part of circular dependency chain: {' -> '.join(cycle)}",
                            'details': {
                                'cycle': cycle,
                                'cycle_length': len(cycle),
                                'cycle_members': [task_lookup.get(tid, {}).get('title', tid) for tid in cycle]
                            }
                        })
        
        except Exception as e:
            print(f"Error detecting circular dependencies: {e}")
        
        return bottlenecks
    
    def _calculate_task_overload_score(self, task: Dict[str, Any], owner_stats: Dict[str, Any], 
                                     load_score: float, max_load: float) -> float:
        """Calculate how much a specific task contributes to owner overload"""
        # Base score from owner's overall load
        base_score = load_score / max_load if max_load > 0 else 0
        
        # Weight by task characteristics
        priority_weight = {'high': 1.5, 'med': 1.0, 'low': 0.7}.get(task['priority'], 1.0)
        effort_weight = task.get('effort', 3) / 5.0
        status_weight = {'in_progress': 1.5, 'blocked': 1.2, 'todo': 1.0, 'done': 0.1}.get(task['status'], 1.0)
        
        # Combine weights
        task_weight = priority_weight * effort_weight * status_weight
        
        return min(base_score * task_weight, 1.0)
    
    def get_bottleneck_summary(self, bottlenecks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics for detected bottlenecks"""
        if not bottlenecks:
            return {
                'total_bottlenecks': 0,
                'by_type': {},
                'by_owner': {},
                'avg_score': 0,
                'high_priority_count': 0
            }
        
        # Count by type
        by_type = {}
        for bottleneck in bottlenecks:
            btype = bottleneck['type']
            by_type[btype] = by_type.get(btype, 0) + 1
        
        # Count by owner
        by_owner = {}
        for bottleneck in bottlenecks:
            owner = bottleneck['owner']
            by_owner[owner] = by_owner.get(owner, 0) + 1
        
        # Calculate statistics
        scores = [b['score'] for b in bottlenecks]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        # Count high-priority bottlenecks
        high_priority_count = len([b for b in bottlenecks if b['score'] > 0.7])
        
        return {
            'total_bottlenecks': len(bottlenecks),
            'by_type': by_type,
            'by_owner': by_owner,
            'avg_score': avg_score,
            'high_priority_count': high_priority_count,
            'max_score': max(scores) if scores else 0,
            'min_score': min(scores) if scores else 0
        }
