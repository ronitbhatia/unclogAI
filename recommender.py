"""
LLM-powered recommendation system for OpsPilot
Generates actionable recommendations for detected bottlenecks
"""

import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

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

class RecommendationEngine:
    """Generates actionable recommendations for workflow bottlenecks"""
    
    def __init__(self):
        self.llm = LLMEngine() if LLM_AVAILABLE else None
        self.recommendation_types = [
            'reassign',
            'split_task',
            'escalate',
            'renegotiate_deadline',
            'add_resources',
            'remove_dependencies',
            'prioritize'
        ]
    
    def generate_recommendations(self, bottlenecks: List[Dict[str, Any]], 
                               graph: Any, rows: List[Dict[str, Any]], 
                               metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for all detected bottlenecks"""
        recommendations = []
        
        if not bottlenecks:
            return recommendations
        
        # Create task and owner lookups
        task_lookup = {row['task_id']: row for row in rows}
        owner_tasks = {}
        for row in rows:
            owner = row['owner']
            if owner not in owner_tasks:
                owner_tasks[owner] = []
            owner_tasks[owner].append(row)
        
        # Generate recommendations for each bottleneck
        for bottleneck in bottlenecks:
            task_id = bottleneck['task_id']
            task = task_lookup.get(task_id, {})
            
            # Generate recommendations using LLM
            llm_recommendations = self._generate_llm_recommendations(
                bottleneck, task, task_lookup, owner_tasks, metrics
            )
            
            # Add rule-based recommendations as fallback
            rule_recommendations = self._generate_rule_based_recommendations(
                bottleneck, task, task_lookup, owner_tasks, metrics
            )
            
            # Combine and deduplicate recommendations
            all_recommendations = llm_recommendations + rule_recommendations
            unique_recommendations = self._deduplicate_recommendations(all_recommendations)
            
            recommendations.append({
                'task_id': task_id,
                'title': task.get('title', 'Unknown Task'),
                'owner': task.get('owner', 'Unknown'),
                'bottleneck_type': bottleneck['type'],
                'bottleneck_score': bottleneck['score'],
                'recommendations': unique_recommendations[:3]  # Limit to top 3
            })
        
        return recommendations
    
    def _generate_llm_recommendations(self, bottleneck: Dict[str, Any], task: Dict[str, Any],
                                    task_lookup: Dict[str, Any], owner_tasks: Dict[str, List[Dict[str, Any]]],
                                    metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations using LLM"""
        try:
            if not LLM_AVAILABLE or not self.llm:
                return []
            
            # Prepare context for LLM
            context = self._prepare_llm_context(bottleneck, task, task_lookup, owner_tasks, metrics)
            
            # Generate prompt
            prompt = PROMPTS.get('recommendations', 'Generate recommendations for: {bottleneck_reason}').format(
                task_id=bottleneck['task_id'],
                task_title=task.get('title', 'Unknown'),
                owner=task.get('owner', 'Unknown'),
                bottleneck_type=bottleneck['type'],
                bottleneck_reason=bottleneck['reason'],
                context=context
            )
            
            # Get LLM response
            response = self.llm.run_llm(prompt, max_new_tokens=256, temperature=0.3)
            
            # Parse JSON response
            recommendations = self._parse_llm_recommendations(response)
            
            return recommendations
            
        except Exception as e:
            print(f"Error generating LLM recommendations: {e}")
            return []
    
    def _generate_rule_based_recommendations(self, bottleneck: Dict[str, Any], task: Dict[str, Any],
                                           task_lookup: Dict[str, Any], owner_tasks: Dict[str, List[Dict[str, Any]]],
                                           metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate rule-based recommendations as fallback"""
        recommendations = []
        
        bottleneck_type = bottleneck['type']
        task_owner = task.get('owner', 'Unknown')
        
        # Owner overload recommendations
        if bottleneck_type == 'overloaded_owner':
            recommendations.extend(self._get_owner_overload_recommendations(
                task, owner_tasks, metrics
            ))
        
        # Aging task recommendations
        elif bottleneck_type == 'aging_task':
            recommendations.extend(self._get_aging_task_recommendations(
                task, bottleneck, owner_tasks
            ))
        
        # Dependency chokepoint recommendations
        elif bottleneck_type == 'dependency_chokepoint':
            recommendations.extend(self._get_dependency_recommendations(
                task, bottleneck, task_lookup
            ))
        
        # Critical path recommendations
        elif bottleneck_type == 'critical_path':
            recommendations.extend(self._get_critical_path_recommendations(
                task, bottleneck, owner_tasks
            ))
        
        # Circular dependency recommendations
        elif bottleneck_type == 'circular_dependency':
            recommendations.extend(self._get_circular_dependency_recommendations(
                task, bottleneck, task_lookup
            ))
        
        return recommendations
    
    def _get_owner_overload_recommendations(self, task: Dict[str, Any], owner_tasks: Dict[str, List[Dict[str, Any]]],
                                          metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for owner overload"""
        recommendations = []
        task_owner = task.get('owner', 'Unknown')
        
        # Find available owners with lower load
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        current_load = load_scores.get(task_owner, 0)
        
        # Find owners with lower load
        available_owners = []
        for owner, load in load_scores.items():
            if load < current_load * 0.7:  # 30% less load
                available_owners.append((owner, load))
        
        if available_owners:
            best_owner = min(available_owners, key=lambda x: x[1])
            recommendations.append({
                'title': f'Reassign to {best_owner[0]}',
                'rationale': f'Owner {best_owner[0]} has lower workload ({best_owner[1]:.2f} vs {current_load:.2f})',
                'expected_effect': 'Reduces overload and balances workload',
                'type': 'reassign',
                'priority': 'high'
            })
        
        # Split task recommendation
        if task.get('effort', 3) >= 4:  # High effort task
            recommendations.append({
                'title': 'Split task into subtasks',
                'rationale': f'High effort task ({task.get("effort", 3)}/5) can be broken down',
                'expected_effect': 'Reduces individual task complexity',
                'type': 'split_task',
                'priority': 'medium'
            })
        
        # Escalate recommendation
        if task.get('priority') == 'high':
            recommendations.append({
                'title': 'Escalate to management',
                'rationale': 'High priority task needs management attention',
                'expected_effect': 'Ensures proper resource allocation',
                'type': 'escalate',
                'priority': 'high'
            })
        
        return recommendations
    
    def _get_aging_task_recommendations(self, task: Dict[str, Any], bottleneck: Dict[str, Any],
                                      owner_tasks: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Generate recommendations for aging tasks"""
        recommendations = []
        
        # Check if task is blocked
        if task.get('status') == 'blocked':
            recommendations.append({
                'title': 'Identify and resolve blockers',
                'rationale': 'Task is blocked and needs unblocking',
                'expected_effect': 'Removes impediments to progress',
                'type': 'escalate',
                'priority': 'high'
            })
        
        # Reassign if owner is overloaded
        task_owner = task.get('owner', 'Unknown')
        owner_task_count = len(owner_tasks.get(task_owner, []))
        
        if owner_task_count > 5:  # Owner has many tasks
            recommendations.append({
                'title': 'Reassign to less busy owner',
                'rationale': f'Owner has {owner_task_count} tasks, may be overloaded',
                'expected_effect': 'Reduces individual owner workload',
                'type': 'reassign',
                'priority': 'medium'
            })
        
        # Renegotiate deadline
        recommendations.append({
            'title': 'Renegotiate deadline',
            'rationale': f'Task has been in progress for {bottleneck.get("details", {}).get("days_in_progress", 0)} days',
            'expected_effect': 'Sets realistic expectations',
            'type': 'renegotiate_deadline',
            'priority': 'medium'
        })
        
        return recommendations
    
    def _get_dependency_recommendations(self, task: Dict[str, Any], bottleneck: Dict[str, Any],
                                      task_lookup: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for dependency chokepoints"""
        recommendations = []
        
        details = bottleneck.get('details', {})
        fan_in_count = details.get('fan_in_count', 0)
        
        if fan_in_count > 3:  # High dependency count
            recommendations.append({
                'title': 'Break down dependencies',
                'rationale': f'Task has {fan_in_count} dependencies, creating chokepoint',
                'expected_effect': 'Reduces dependency complexity',
                'type': 'remove_dependencies',
                'priority': 'high'
            })
        
        # Prioritize this task
        recommendations.append({
            'title': 'Increase priority',
            'rationale': 'Critical chokepoint task should be prioritized',
            'expected_effect': 'Ensures timely completion of critical path',
            'type': 'prioritize',
            'priority': 'high'
        })
        
        return recommendations
    
    def _get_critical_path_recommendations(self, task: Dict[str, Any], bottleneck: Dict[str, Any],
                                        owner_tasks: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Generate recommendations for critical path tasks"""
        recommendations = []
        
        # Ensure adequate resources
        recommendations.append({
            'title': 'Allocate dedicated resources',
            'rationale': 'Critical path task needs dedicated attention',
            'expected_effect': 'Ensures timely completion of critical path',
            'type': 'add_resources',
            'priority': 'high'
        })
        
        # Daily check-ins
        recommendations.append({
            'title': 'Implement daily check-ins',
            'rationale': 'Critical path requires close monitoring',
            'expected_effect': 'Early detection of issues',
            'type': 'escalate',
            'priority': 'medium'
        })
        
        return recommendations
    
    def _get_circular_dependency_recommendations(self, task: Dict[str, Any], bottleneck: Dict[str, Any],
                                              task_lookup: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for circular dependencies"""
        recommendations = []
        
        details = bottleneck.get('details', {})
        cycle = details.get('cycle', [])
        
        recommendations.append({
            'title': 'Break circular dependency',
            'rationale': f'Task is part of circular dependency: {" -> ".join(cycle)}',
            'expected_effect': 'Eliminates blocking circular dependency',
            'type': 'remove_dependencies',
            'priority': 'high'
        })
        
        recommendations.append({
            'title': 'Redesign workflow',
            'rationale': 'Circular dependencies indicate workflow design issues',
            'expected_effect': 'Creates more efficient workflow',
            'type': 'escalate',
            'priority': 'high'
        })
        
        return recommendations
    
    def _prepare_llm_context(self, bottleneck: Dict[str, Any], task: Dict[str, Any],
                           task_lookup: Dict[str, Any], owner_tasks: Dict[str, List[Dict[str, Any]]],
                           metrics: Dict[str, Any]) -> str:
        """Prepare context information for LLM"""
        context_parts = []
        
        # Task information
        context_parts.append(f"Task: {task.get('title', 'Unknown')}")
        context_parts.append(f"Owner: {task.get('owner', 'Unknown')}")
        context_parts.append(f"Status: {task.get('status', 'Unknown')}")
        context_parts.append(f"Priority: {task.get('priority', 'Unknown')}")
        context_parts.append(f"Effort: {task.get('effort', 3)}/5")
        
        # Owner workload
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        task_owner = task.get('owner', 'Unknown')
        owner_load_score = load_scores.get(task_owner, 0)
        context_parts.append(f"Owner load score: {owner_load_score:.2f}")
        
        # Owner's other tasks
        owner_task_count = len(owner_tasks.get(task_owner, []))
        context_parts.append(f"Owner has {owner_task_count} total tasks")
        
        # Dependencies
        details = bottleneck.get('details', {})
        if 'dependencies' in details:
            deps = details['dependencies']
            context_parts.append(f"Dependencies: {len(deps)} tasks")
        
        return "\n".join(context_parts)
    
    def _parse_llm_recommendations(self, response: str) -> List[Dict[str, Any]]:
        """Parse LLM response to extract recommendations"""
        try:
            # Extract JSON from response
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                
                if isinstance(data, list):
                    recommendations = []
                    for item in data:
                        if isinstance(item, dict) and 'title' in item:
                            recommendations.append({
                                'title': item.get('title', ''),
                                'rationale': item.get('rationale', ''),
                                'expected_effect': item.get('expected_effect', ''),
                                'type': item.get('type', 'unknown'),
                                'priority': item.get('priority', 'medium')
                            })
                    return recommendations
            
            return []
            
        except Exception as e:
            print(f"Error parsing LLM recommendations: {e}")
            return []
    
    def _deduplicate_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate recommendations"""
        seen = set()
        unique_recommendations = []
        
        for rec in recommendations:
            # Create a key based on title and type
            key = (rec.get('title', ''), rec.get('type', ''))
            if key not in seen:
                seen.add(key)
                unique_recommendations.append(rec)
        
        return unique_recommendations
    
    def get_recommendation_summary(self, recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics for recommendations"""
        if not recommendations:
            return {
                'total_recommendations': 0,
                'by_type': {},
                'by_priority': {},
                'avg_priority_score': 0
            }
        
        # Count by type
        by_type = {}
        by_priority = {}
        priority_scores = {'high': 3, 'medium': 2, 'low': 1}
        
        total_priority_score = 0
        total_recs = 0
        
        for rec_group in recommendations:
            for rec in rec_group.get('recommendations', []):
                rec_type = rec.get('type', 'unknown')
                priority = rec.get('priority', 'medium')
                
                by_type[rec_type] = by_type.get(rec_type, 0) + 1
                by_priority[priority] = by_priority.get(priority, 0) + 1
                
                total_priority_score += priority_scores.get(priority, 2)
                total_recs += 1
        
        avg_priority_score = total_priority_score / total_recs if total_recs > 0 else 0
        
        return {
            'total_recommendations': total_recs,
            'by_type': by_type,
            'by_priority': by_priority,
            'avg_priority_score': avg_priority_score
        }
