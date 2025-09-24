"""
LangGraph workflow orchestration for OpsPilot
Defines the state and workflow nodes for bottleneck detection
"""

from typing import Dict, List, Optional, Any, TypedDict
from dataclasses import dataclass, field
from datetime import datetime
import networkx as nx

from ingest import DataIngester
from graph_builder import GraphBuilder
from detector import BottleneckDetector
from recommender import RecommendationEngine
from forecaster import RiskForecaster
from report import ReportGenerator

@dataclass
class WorkflowState:
    """State object for the LangGraph workflow"""
    raw_csv: Optional[str] = None
    raw_text: Optional[str] = None
    rows: List[Dict[str, Any]] = field(default_factory=list)
    graph: Optional[nx.DiGraph] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    bottlenecks: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    risk_forecast: List[Dict[str, Any]] = field(default_factory=list)
    run_report_md: str = ""
    settings: Dict[str, Any] = field(default_factory=dict)
    run_id: Optional[str] = None
    timestamp: Optional[datetime] = None

class WorkflowNode:
    """Base class for workflow nodes"""
    
    def __init__(self):
        self.name = self.__class__.__name__.lower()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """Execute the node logic"""
        return self.execute(state)
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Override in subclasses"""
        return state

class IngestParseNode(WorkflowNode):
    """Parse CSV and text input into structured rows"""
    
    def __init__(self):
        super().__init__()
        self.ingester = DataIngester()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Parse raw data into structured rows"""
        try:
            rows = []
            
            # Parse CSV if provided
            if state.raw_csv:
                csv_rows = self.ingester.parse_csv(state.raw_csv)
                rows.extend(csv_rows)
            
            # Parse text if provided
            if state.raw_text:
                text_rows = self.ingester.parse_text(state.raw_text)
                rows.extend(text_rows)
            
            state.rows = rows
            print(f"Parsed {len(rows)} tasks from input data")
            
        except Exception as e:
            print(f"Error in ingest_parse: {e}")
            state.rows = []
        
        return state

class BuildGraphNode(WorkflowNode):
    """Build networkx graph and compute metrics"""
    
    def __init__(self):
        super().__init__()
        self.graph_builder = GraphBuilder()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Build graph and compute metrics"""
        try:
            if not state.rows:
                print("No rows to build graph from")
                return state
            
            # Build graph
            state.graph = self.graph_builder.build_graph(state.rows)
            
            # Compute metrics
            state.metrics = self.graph_builder.compute_metrics(
                state.graph, 
                state.rows,
                state.settings
            )
            
            print(f"Built graph with {state.graph.number_of_nodes()} nodes and {state.graph.number_of_edges()} edges")
            
        except Exception as e:
            print(f"Error in build_graph: {e}")
            state.graph = None
            state.metrics = {}
        
        return state

class DetectBottlenecksNode(WorkflowNode):
    """Detect bottlenecks using various algorithms"""
    
    def __init__(self):
        super().__init__()
        self.detector = BottleneckDetector()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Detect bottlenecks in the workflow"""
        try:
            if not state.graph or not state.rows:
                print("No graph or rows to analyze")
                return state
            
            state.bottlenecks = self.detector.detect_bottlenecks(
                state.graph,
                state.rows,
                state.metrics,
                state.settings
            )
            
            print(f"Detected {len(state.bottlenecks)} bottlenecks")
            
        except Exception as e:
            print(f"Error in detect_bottlenecks: {e}")
            state.bottlenecks = []
        
        return state

class RecommendActionsNode(WorkflowNode):
    """Generate recommendations for bottlenecks"""
    
    def __init__(self):
        super().__init__()
        self.recommender = RecommendationEngine()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Generate recommendations for detected bottlenecks"""
        try:
            if not state.bottlenecks:
                print("No bottlenecks to generate recommendations for")
                return state
            
            state.recommendations = self.recommender.generate_recommendations(
                state.bottlenecks,
                state.graph,
                state.rows,
                state.metrics
            )
            
            print(f"Generated recommendations for {len(state.recommendations)} bottlenecks")
            
        except Exception as e:
            print(f"Error in recommend_actions: {e}")
            state.recommendations = []
        
        return state

class ForecastRisksNode(WorkflowNode):
    """Forecast risks using heuristics"""
    
    def __init__(self):
        super().__init__()
        self.forecaster = RiskForecaster()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Forecast risks in the workflow"""
        try:
            if not state.graph or not state.rows:
                print("No graph or rows to forecast risks for")
                return state
            
            state.risk_forecast = self.forecaster.forecast_risks(
                state.graph,
                state.rows,
                state.metrics,
                state.settings
            )
            
            print(f"Forecasted risks for {len(state.risk_forecast)} tasks")
            
        except Exception as e:
            print(f"Error in forecast_risks: {e}")
            state.risk_forecast = []
        
        return state

class PackageReportNode(WorkflowNode):
    """Generate final report and markdown"""
    
    def __init__(self):
        super().__init__()
        self.report_gen = ReportGenerator()
    
    def execute(self, state: WorkflowState) -> WorkflowState:
        """Generate final report"""
        try:
            state.run_report_md = self.report_gen.generate_full_report(state)
            state.timestamp = datetime.now()
            
            print("Generated final report")
            
        except Exception as e:
            print(f"Error in package_report: {e}")
            state.run_report_md = f"Error generating report: {e}"
        
        return state

class WorkflowOrchestrator:
    """Orchestrates the LangGraph workflow"""
    
    def __init__(self):
        self.nodes = {
            'ingest_parse': IngestParseNode(),
            'build_graph': BuildGraphNode(),
            'detect_bottlenecks': DetectBottlenecksNode(),
            'recommend_actions': RecommendActionsNode(),
            'forecast_risks': ForecastRisksNode(),
            'package_report': PackageReportNode()
        }
        
        # Define the workflow graph
        self.workflow = {
            'ingest_parse': ['build_graph'],
            'build_graph': ['detect_bottlenecks', 'forecast_risks'],
            'detect_bottlenecks': ['recommend_actions'],
            'recommend_actions': ['package_report'],
            'forecast_risks': ['package_report']
        }
    
    def run_workflow(self, state: WorkflowState) -> WorkflowState:
        """Execute the complete workflow"""
        print("Starting workflow execution...")
        
        # Execute nodes in order
        current_state = state
        
        # Step 1: Parse input data
        current_state = self.nodes['ingest_parse'](current_state)
        
        # Step 2: Build graph and compute metrics
        current_state = self.nodes['build_graph'](current_state)
        
        # Step 3: Detect bottlenecks
        current_state = self.nodes['detect_bottlenecks'](current_state)
        
        # Step 4: Generate recommendations
        current_state = self.nodes['recommend_actions'](current_state)
        
        # Step 5: Forecast risks
        current_state = self.nodes['forecast_risks'](current_state)
        
        # Step 6: Package final report
        current_state = self.nodes['package_report'](current_state)
        
        print("Workflow execution completed")
        return current_state

def run_workflow_analysis(state: WorkflowState) -> WorkflowState:
    """Main entry point for workflow analysis"""
    orchestrator = WorkflowOrchestrator()
    return orchestrator.run_workflow(state)
