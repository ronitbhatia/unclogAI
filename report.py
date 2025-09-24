"""
Report generation and export system for OpsPilot
Creates markdown reports and CSV exports for analysis results
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from io import StringIO

class ReportGenerator:
    """Generates comprehensive reports and exports for workflow analysis"""
    
    def __init__(self):
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def generate_full_report(self, state: Any) -> str:
        """Generate complete markdown report"""
        report_sections = []
        
        # Header
        report_sections.append(self._generate_header())
        
        # Dashboard
        report_sections.append(self._generate_dashboard_section(state))
        
        # Bottlenecks
        report_sections.append(self._generate_bottlenecks_section(state))
        
        # Recommendations
        report_sections.append(self._generate_recommendations_section(state))
        
        # Risk Forecast
        report_sections.append(self._generate_risks_section(state))
        
        # Graph Summary
        report_sections.append(self._generate_graph_section(state))
        
        return "\n\n".join(report_sections)
    
    def generate_dashboard(self, state: Any) -> str:
        """Generate dashboard section"""
        return self._generate_dashboard_section(state)
    
    def generate_bottlenecks_report(self, state: Any) -> str:
        """Generate bottlenecks section"""
        return self._generate_bottlenecks_section(state)
    
    def generate_recommendations_report(self, state: Any) -> str:
        """Generate recommendations section"""
        return self._generate_recommendations_section(state)
    
    def generate_risks_report(self, state: Any) -> str:
        """Generate risks section"""
        return self._generate_risks_section(state)
    
    def generate_graph_summary(self, state: Any) -> str:
        """Generate graph summary section"""
        return self._generate_graph_section(state)
    
    def _generate_header(self) -> str:
        """Generate report header"""
        return f"""# UnclogAI Analysis Report
Generated: {self.timestamp}

---
"""
    
    def _generate_dashboard_section(self, state: Any) -> str:
        """Generate dashboard with KPIs"""
        if not hasattr(state, 'rows') or not state.rows:
            return "## Dashboard\n\nNo data available for dashboard."
        
        # Basic metrics
        total_tasks = len(state.rows)
        owners = set(row['owner'] for row in state.rows)
        total_owners = len(owners)
        
        # Status breakdown
        status_counts = {}
        for row in state.rows:
            status = row['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Priority breakdown
        priority_counts = {}
        for row in state.rows:
            priority = row['priority']
            priority_counts[priority] = priority_counts.get(priority, 0) + 1
        
        # Bottleneck counts
        bottleneck_count = len(state.bottlenecks) if hasattr(state, 'bottlenecks') else 0
        risk_count = len(state.risk_forecast) if hasattr(state, 'risk_forecast') else 0
        
        # Graph metrics
        graph_metrics = state.metrics.get('basic', {}) if hasattr(state, 'metrics') else {}
        num_nodes = graph_metrics.get('num_nodes', 0)
        num_edges = graph_metrics.get('num_edges', 0)
        density = graph_metrics.get('density', 0)
        
        return f"""## Dashboard

### Key Metrics
- **Total Tasks**: {total_tasks}
- **Team Members**: {total_owners}
- **Bottlenecks Detected**: {bottleneck_count}
- **At-Risk Tasks**: {risk_count}

### Task Status Distribution
{self._format_status_chart(status_counts)}

### Priority Distribution
{self._format_priority_chart(priority_counts)}

### Workflow Graph
- **Nodes**: {num_nodes}
- **Dependencies**: {num_edges}
- **Density**: {density:.3f}

### Team Members
{', '.join(sorted(owners))}
"""
    
    def _generate_bottlenecks_section(self, state: Any) -> str:
        """Generate bottlenecks section"""
        if not hasattr(state, 'bottlenecks') or not state.bottlenecks:
            return "## Bottlenecks\n\nNo bottlenecks detected."
        
        bottlenecks = state.bottlenecks
        summary = self._get_bottleneck_summary(bottlenecks)
        
        report = f"""## Bottlenecks

### Summary
- **Total Bottlenecks**: {summary['total_bottlenecks']}
- **High Priority**: {summary['high_priority_count']}
- **Average Score**: {summary['avg_score']:.2f}

### Bottleneck Types
"""
        
        for btype, count in summary['by_type'].items():
            report += f"- **{btype.replace('_', ' ').title()}**: {count}\n"
        
        report += "\n### Detailed Bottlenecks\n\n"
        
        for i, bottleneck in enumerate(bottlenecks[:10], 1):  # Top 10
            report += f"""#### {i}. {bottleneck['title']}
- **Task ID**: {bottleneck['task_id']}
- **Owner**: {bottleneck['owner']}
- **Type**: {bottleneck['type'].replace('_', ' ').title()}
- **Score**: {bottleneck['score']:.2f}
- **Reason**: {bottleneck['reason']}

"""
        
        if len(bottlenecks) > 10:
            report += f"... and {len(bottlenecks) - 10} more bottlenecks\n"
        
        return report
    
    def _generate_recommendations_section(self, state: Any) -> str:
        """Generate recommendations section"""
        if not hasattr(state, 'recommendations') or not state.recommendations:
            return "## Recommendations\n\nNo recommendations available."
        
        recommendations = state.recommendations
        summary = self._get_recommendation_summary(recommendations)
        
        report = f"""## Recommendations

### Summary
- **Total Recommendations**: {summary['total_recommendations']}
- **Average Priority**: {summary['avg_priority_score']:.2f}

### Recommendation Types
"""
        
        for rtype, count in summary['by_type'].items():
            report += f"- **{rtype.replace('_', ' ').title()}**: {count}\n"
        
        report += "\n### Detailed Recommendations\n\n"
        
        for rec_group in recommendations[:5]:  # Top 5 tasks
            report += f"""#### {rec_group['title']}
- **Task ID**: {rec_group['task_id']}
- **Owner**: {rec_group['owner']}
- **Bottleneck Type**: {rec_group['bottleneck_type'].replace('_', ' ').title()}
- **Bottleneck Score**: {rec_group['bottleneck_score']:.2f}

**Recommendations:**
"""
            
            for j, rec in enumerate(rec_group['recommendations'], 1):
                report += f"{j}. **{rec['title']}**\n"
                report += f"   - *Rationale*: {rec['rationale']}\n"
                report += f"   - *Expected Effect*: {rec['expected_effect']}\n"
                report += f"   - *Priority*: {rec['priority']}\n\n"
        
        return report
    
    def _generate_risks_section(self, state: Any) -> str:
        """Generate risks section"""
        if not hasattr(state, 'risk_forecast') or not state.risk_forecast:
            return "## At-Risk Tasks\n\nNo at-risk tasks identified."
        
        risks = state.risk_forecast
        summary = self._get_risk_summary(risks)
        
        report = f"""## At-Risk Tasks

### Summary
- **Total At-Risk Tasks**: {summary['total_risks']}
- **Critical Risk**: {summary['critical_count']}
- **Average Risk Score**: {summary['avg_risk_score']:.2f}

### Risk Levels
"""
        
        for level, count in summary['by_level'].items():
            report += f"- **{level}**: {count}\n"
        
        report += "\n### Detailed Risk Analysis\n\n"
        
        for i, risk in enumerate(risks[:10], 1):  # Top 10
            report += f"""#### {i}. {risk['title']}
- **Task ID**: {risk['task_id']}
- **Owner**: {risk['owner']}
- **Status**: {risk['status']}
- **Risk Level**: {risk['risk_level']}
- **Risk Score**: {risk['risk_score']:.2f}
- **Due Date**: {risk.get('due_date', 'Not set')}

**Risk Factors:**
"""
            
            for reason in risk['reasons']:
                report += f"- {reason}\n"
            
            report += "\n"
        
        if len(risks) > 10:
            report += f"... and {len(risks) - 10} more at-risk tasks\n"
        
        return report
    
    def _generate_graph_section(self, state: Any) -> str:
        """Generate graph summary section"""
        if not hasattr(state, 'graph') or not state.graph:
            return "## Graph View\n\nNo graph data available."
        
        graph = state.graph
        metrics = state.metrics if hasattr(state, 'metrics') else {}
        
        # Basic graph info
        num_nodes = graph.number_of_nodes()
        num_edges = graph.number_of_edges()
        
        # Centrality info
        centrality_metrics = metrics.get('centrality', {})
        betweenness = centrality_metrics.get('betweenness', {})
        
        # Find most central nodes
        if betweenness:
            top_central = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:5]
        else:
            top_central = []
        
        # Dependency info
        dependency_metrics = metrics.get('dependencies', {})
        max_depth = dependency_metrics.get('max_depth', 0)
        avg_depth = dependency_metrics.get('avg_depth', 0)
        
        report = f"""## Graph View

### Graph Structure
- **Nodes (Tasks)**: {num_nodes}
- **Edges (Dependencies)**: {num_edges}
- **Max Dependency Depth**: {max_depth}
- **Average Dependency Depth**: {avg_depth:.1f}

### Most Central Tasks
"""
        
        for i, (task_id, score) in enumerate(top_central, 1):
            report += f"{i}. **{task_id}** (centrality: {score:.3f})\n"
        
        # Owner load info
        owner_load = metrics.get('owner_load', {})
        load_scores = owner_load.get('load_scores', {})
        
        if load_scores:
            report += "\n### Owner Workload\n"
            sorted_owners = sorted(load_scores.items(), key=lambda x: x[1], reverse=True)
            for owner, load in sorted_owners:
                report += f"- **{owner}**: {load:.2f}\n"
        
        return report
    
    def _format_status_chart(self, status_counts: Dict[str, int]) -> str:
        """Format status distribution as text chart"""
        if not status_counts:
            return "No status data available."
        
        total = sum(status_counts.values())
        chart = ""
        
        for status, count in sorted(status_counts.items()):
            percentage = (count / total) * 100
            bar = "█" * int(percentage / 5)  # Scale to 20 chars max
            chart += f"- **{status.title()}**: {count} ({percentage:.1f}%) {bar}\n"
        
        return chart
    
    def _format_priority_chart(self, priority_counts: Dict[str, int]) -> str:
        """Format priority distribution as text chart"""
        if not priority_counts:
            return "No priority data available."
        
        total = sum(priority_counts.values())
        chart = ""
        
        for priority, count in sorted(priority_counts.items()):
            percentage = (count / total) * 100
            bar = "█" * int(percentage / 5)  # Scale to 20 chars max
            chart += f"- **{priority.title()}**: {count} ({percentage:.1f}%) {bar}\n"
        
        return chart
    
    def _get_bottleneck_summary(self, bottlenecks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get bottleneck summary statistics"""
        if not bottlenecks:
            return {
                'total_bottlenecks': 0,
                'by_type': {},
                'high_priority_count': 0,
                'avg_score': 0
            }
        
        by_type = {}
        high_priority_count = 0
        scores = []
        
        for bottleneck in bottlenecks:
            btype = bottleneck['type']
            score = bottleneck['score']
            
            by_type[btype] = by_type.get(btype, 0) + 1
            scores.append(score)
            
            if score > 0.7:
                high_priority_count += 1
        
        avg_score = sum(scores) / len(scores) if scores else 0
        
        return {
            'total_bottlenecks': len(bottlenecks),
            'by_type': by_type,
            'high_priority_count': high_priority_count,
            'avg_score': avg_score
        }
    
    def _get_recommendation_summary(self, recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get recommendation summary statistics"""
        if not recommendations:
            return {
                'total_recommendations': 0,
                'by_type': {},
                'avg_priority_score': 0
            }
        
        by_type = {}
        priority_scores = {'high': 3, 'medium': 2, 'low': 1}
        total_priority_score = 0
        total_recs = 0
        
        for rec_group in recommendations:
            for rec in rec_group.get('recommendations', []):
                rec_type = rec.get('type', 'unknown')
                priority = rec.get('priority', 'medium')
                
                by_type[rec_type] = by_type.get(rec_type, 0) + 1
                total_priority_score += priority_scores.get(priority, 2)
                total_recs += 1
        
        avg_priority_score = total_priority_score / total_recs if total_recs > 0 else 0
        
        return {
            'total_recommendations': total_recs,
            'by_type': by_type,
            'avg_priority_score': avg_priority_score
        }
    
    def _get_risk_summary(self, risks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get risk summary statistics"""
        if not risks:
            return {
                'total_risks': 0,
                'by_level': {},
                'critical_count': 0,
                'avg_risk_score': 0
            }
        
        by_level = {}
        critical_count = 0
        risk_scores = []
        
        for risk in risks:
            level = risk['risk_level']
            score = risk['risk_score']
            
            by_level[level] = by_level.get(level, 0) + 1
            risk_scores.append(score)
            
            if level == "Critical":
                critical_count += 1
        
        avg_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
        
        return {
            'total_risks': len(risks),
            'by_level': by_level,
            'critical_count': critical_count,
            'avg_risk_score': avg_risk_score
        }
    
    # CSV Export Methods
    def export_bottlenecks_csv(self, state: Any) -> str:
        """Export bottlenecks as CSV"""
        if not hasattr(state, 'bottlenecks') or not state.bottlenecks:
            return "task_id,title,owner,type,score,reason\n"
        
        df = pd.DataFrame(state.bottlenecks)
        return df.to_csv(index=False)
    
    def export_recommendations_csv(self, state: Any) -> str:
        """Export recommendations as CSV"""
        if not hasattr(state, 'recommendations') or not state.recommendations:
            return "task_id,title,owner,recommendation_title,type,priority,rationale,expected_effect\n"
        
        rows = []
        for rec_group in state.recommendations:
            for rec in rec_group.get('recommendations', []):
                rows.append({
                    'task_id': rec_group['task_id'],
                    'title': rec_group['title'],
                    'owner': rec_group['owner'],
                    'recommendation_title': rec['title'],
                    'type': rec['type'],
                    'priority': rec['priority'],
                    'rationale': rec['rationale'],
                    'expected_effect': rec['expected_effect']
                })
        
        df = pd.DataFrame(rows)
        return df.to_csv(index=False)
    
    def export_risks_csv(self, state: Any) -> str:
        """Export risks as CSV"""
        if not hasattr(state, 'risk_forecast') or not state.risk_forecast:
            return "task_id,title,owner,status,risk_level,risk_score,due_date,priority,effort,reasons\n"
        
        # Flatten reasons list
        rows = []
        for risk in state.risk_forecast:
            rows.append({
                'task_id': risk['task_id'],
                'title': risk['title'],
                'owner': risk['owner'],
                'status': risk['status'],
                'risk_level': risk['risk_level'],
                'risk_score': risk['risk_score'],
                'due_date': risk.get('due_date', ''),
                'priority': risk['priority'],
                'effort': risk['effort'],
                'reasons': '; '.join(risk['reasons'])
            })
        
        df = pd.DataFrame(rows)
        return df.to_csv(index=False)
