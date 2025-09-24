"""
Network graph builder for OpsPilot
Constructs networkx graphs and computes centrality metrics
"""

import networkx as nx
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict, Counter

class GraphBuilder:
    """Builds and analyzes workflow graphs using networkx"""
    
    def __init__(self):
        self.graph = None
    
    def build_graph(self, rows: List[Dict[str, Any]]) -> nx.DiGraph:
        """Build a directed graph from task rows"""
        G = nx.DiGraph()
        
        # Add nodes (tasks)
        for row in rows:
            task_id = row['task_id']
            G.add_node(task_id, **row)
        
        # Add edges (dependencies)
        for row in rows:
            task_id = row['task_id']
            dependencies = row.get('dependency_ids', [])
            
            for dep_id in dependencies:
                if dep_id in G.nodes:
                    G.add_edge(dep_id, task_id)  # dep_id -> task_id (dependency flows forward)
        
        self.graph = G
        return G
    
    def compute_metrics(self, graph: nx.DiGraph, rows: List[Dict[str, Any]], settings: Dict[str, Any]) -> Dict[str, Any]:
        """Compute comprehensive metrics for the graph"""
        if not graph or graph.number_of_nodes() == 0:
            return {}
        
        metrics = {}
        
        # Basic graph metrics
        metrics['basic'] = self._compute_basic_metrics(graph)
        
        # Centrality metrics
        metrics['centrality'] = self._compute_centrality_metrics(graph)
        
        # Owner load metrics
        metrics['owner_load'] = self._compute_owner_load_metrics(graph, rows, settings)
        
        # Aging metrics
        metrics['aging'] = self._compute_aging_metrics(graph, rows, settings)
        
        # Dependency metrics
        metrics['dependencies'] = self._compute_dependency_metrics(graph, rows)
        
        return metrics
    
    def _compute_basic_metrics(self, graph: nx.DiGraph) -> Dict[str, Any]:
        """Compute basic graph statistics"""
        return {
            'num_nodes': graph.number_of_nodes(),
            'num_edges': graph.number_of_edges(),
            'density': nx.density(graph),
            'is_connected': nx.is_weakly_connected(graph),
            'num_components': nx.number_weakly_connected_components(graph),
            'avg_clustering': nx.average_clustering(graph.to_undirected()) if graph.number_of_nodes() > 0 else 0
        }
    
    def _compute_centrality_metrics(self, graph: nx.DiGraph) -> Dict[str, Any]:
        """Compute various centrality measures"""
        if graph.number_of_nodes() == 0:
            return {}
        
        try:
            # Betweenness centrality
            betweenness = nx.betweenness_centrality(graph)
            
            # In-degree and out-degree centrality
            in_degree_centrality = dict(graph.in_degree())
            out_degree_centrality = dict(graph.out_degree())
            
            # Normalize degree centralities
            max_in_degree = max(in_degree_centrality.values()) if in_degree_centrality else 1
            max_out_degree = max(out_degree_centrality.values()) if out_degree_centrality else 1
            
            in_degree_norm = {k: v / max_in_degree for k, v in in_degree_centrality.items()}
            out_degree_norm = {k: v / max_out_degree for k, v in out_degree_centrality.items()}
            
            # PageRank centrality
            try:
                pagerank = nx.pagerank(graph, alpha=0.85)
            except:
                pagerank = {node: 0 for node in graph.nodes()}
            
            return {
                'betweenness': betweenness,
                'in_degree': in_degree_centrality,
                'out_degree': out_degree_centrality,
                'in_degree_norm': in_degree_norm,
                'out_degree_norm': out_degree_norm,
                'pagerank': pagerank
            }
            
        except Exception as e:
            print(f"Error computing centrality metrics: {e}")
            return {}
    
    def _compute_owner_load_metrics(self, graph: nx.DiGraph, rows: List[Dict[str, Any]], settings: Dict[str, Any]) -> Dict[str, Any]:
        """Compute owner workload metrics"""
        owner_load = defaultdict(list)
        owner_stats = defaultdict(lambda: {
            'total_tasks': 0,
            'in_progress': 0,
            'blocked': 0,
            'high_priority': 0,
            'high_effort': 0,
            'due_soon': 0,
            'overdue': 0
        })
        
        due_soon_days = settings.get('due_soon_days', 7)
        today = datetime.now().date()
        
        for row in rows:
            owner = row['owner']
            status = row['status']
            priority = row['priority']
            effort = row.get('effort', 3)
            due_date = row.get('due_date')
            
            # Basic counts
            owner_stats[owner]['total_tasks'] += 1
            
            if status == 'in_progress':
                owner_stats[owner]['in_progress'] += 1
            elif status == 'blocked':
                owner_stats[owner]['blocked'] += 1
            
            if priority == 'high':
                owner_stats[owner]['high_priority'] += 1
            
            if effort >= 4:  # High effort
                owner_stats[owner]['high_effort'] += 1
            
            # Due date analysis
            if due_date:
                try:
                    due_date_obj = datetime.strptime(due_date, '%Y-%m-%d').date()
                    days_until_due = (due_date_obj - today).days
                    
                    if days_until_due <= due_soon_days and days_until_due >= 0:
                        owner_stats[owner]['due_soon'] += 1
                    elif days_until_due < 0:
                        owner_stats[owner]['overdue'] += 1
                except:
                    pass
            
            # Store task details for owner
            owner_load[owner].append({
                'task_id': row['task_id'],
                'title': row['title'],
                'status': status,
                'priority': priority,
                'effort': effort,
                'due_date': due_date
            })
        
        # Compute load scores
        load_scores = {}
        for owner, stats in owner_stats.items():
            # Weighted load score
            load_score = (
                stats['in_progress'] * 2.0 +  # In progress tasks
                stats['blocked'] * 1.5 +      # Blocked tasks
                stats['high_priority'] * 1.5 + # High priority
                stats['high_effort'] * 1.2 +   # High effort
                stats['due_soon'] * 1.0 +      # Due soon
                stats['overdue'] * 2.0         # Overdue
            )
            load_scores[owner] = load_score
        
        return {
            'owner_stats': dict(owner_stats),
            'owner_load': dict(owner_load),
            'load_scores': load_scores,
            'max_load': max(load_scores.values()) if load_scores else 0,
            'avg_load': sum(load_scores.values()) / len(load_scores) if load_scores else 0
        }
    
    def _compute_aging_metrics(self, graph: nx.DiGraph, rows: List[Dict[str, Any]], settings: Dict[str, Any]) -> Dict[str, Any]:
        """Compute task aging metrics"""
        aging_threshold = settings.get('aging_threshold', 5)
        today = datetime.now().date()
        
        aging_tasks = []
        status_durations = defaultdict(list)
        
        for row in rows:
            task_id = row['task_id']
            status = row['status']
            start_date = row.get('start_date')
            
            if start_date and status == 'in_progress':
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                    days_in_progress = (today - start_date_obj).days
                    
                    if days_in_progress > aging_threshold:
                        aging_tasks.append({
                            'task_id': task_id,
                            'title': row['title'],
                            'owner': row['owner'],
                            'days_in_progress': days_in_progress,
                            'priority': row['priority'],
                            'effort': row.get('effort', 3)
                        })
                    
                    status_durations[status].append(days_in_progress)
                    
                except:
                    pass
        
        # Compute average cycle times
        avg_cycle_times = {}
        for status, durations in status_durations.items():
            if durations:
                avg_cycle_times[status] = sum(durations) / len(durations)
        
        return {
            'aging_tasks': aging_tasks,
            'num_aging': len(aging_tasks),
            'avg_cycle_times': avg_cycle_times,
            'aging_threshold': aging_threshold
        }
    
    def _compute_dependency_metrics(self, graph: nx.DiGraph, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute dependency-related metrics"""
        if graph.number_of_nodes() == 0:
            return {}
        
        # Dependency depth analysis
        depths = {}
        for node in graph.nodes():
            # Find longest path to this node
            try:
                # Get all predecessors and compute max depth
                predecessors = list(nx.ancestors(graph, node))
                if predecessors:
                    # Simple depth calculation - could be improved with actual path finding
                    depths[node] = len(predecessors)
                else:
                    depths[node] = 0
            except:
                depths[node] = 0
        
        # Critical path analysis (simplified)
        critical_paths = []
        try:
            # Find nodes with high betweenness centrality as potential critical path nodes
            betweenness = nx.betweenness_centrality(graph)
            high_betweenness = [(node, score) for node, score in betweenness.items() if score > 0.1]
            critical_paths = sorted(high_betweenness, key=lambda x: x[1], reverse=True)[:5]
        except:
            pass
        
        # Dependency fan-in/fan-out analysis
        fan_in = dict(graph.in_degree())
        fan_out = dict(graph.out_degree())
        
        return {
            'depths': depths,
            'max_depth': max(depths.values()) if depths else 0,
            'avg_depth': sum(depths.values()) / len(depths) if depths else 0,
            'critical_paths': critical_paths,
            'fan_in': fan_in,
            'fan_out': fan_out,
            'max_fan_in': max(fan_in.values()) if fan_in else 0,
            'max_fan_out': max(fan_out.values()) if fan_out else 0
        }
    
    def get_node_attributes(self, graph: nx.DiGraph, node_id: str) -> Dict[str, Any]:
        """Get all attributes for a specific node"""
        if graph.has_node(node_id):
            return graph.nodes[node_id]
        return {}
    
    def get_predecessors(self, graph: nx.DiGraph, node_id: str) -> List[str]:
        """Get all predecessor nodes (dependencies)"""
        if graph.has_node(node_id):
            return list(graph.predecessors(node_id))
        return []
    
    def get_successors(self, graph: nx.DiGraph, node_id: str) -> List[str]:
        """Get all successor nodes (dependents)"""
        if graph.has_node(node_id):
            return list(graph.successors(node_id))
        return []
    
    def find_cycles(self, graph: nx.DiGraph) -> List[List[str]]:
        """Find cycles in the dependency graph"""
        try:
            cycles = list(nx.simple_cycles(graph))
            return cycles
        except:
            return []
    
    def get_strongly_connected_components(self, graph: nx.DiGraph) -> List[List[str]]:
        """Get strongly connected components (circular dependencies)"""
        try:
            scc = list(nx.strongly_connected_components(graph))
            # Filter out single-node components
            return [list(component) for component in scc if len(component) > 1]
        except:
            return []
