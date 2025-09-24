"""
OpsPilot - Workflow Bottleneck Detection
Main Gradio application for Hugging Face Spaces deployment
"""

import gradio as gr
import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import tempfile

# Import with error handling
try:
    from graph import WorkflowState, run_workflow_analysis
    from storage import StorageManager
    from report import ReportGenerator
    FULL_FEATURES = True
except ImportError as e:
    print(f"Warning: Some features unavailable: {e}")
    FULL_FEATURES = False

# Initialize storage with error handling
if FULL_FEATURES:
    storage = StorageManager()
    report_gen = ReportGenerator()
else:
    storage = None
    report_gen = None

def process_uploaded_file(file) -> Optional[str]:
    """Process uploaded CSV/JSON file and return content as string"""
    if file is None:
        return None
    
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file.name)
            return df.to_csv(index=False)
        elif file.name.endswith('.json'):
            with open(file.name, 'r') as f:
                data = json.load(f)
            return json.dumps(data, indent=2)
    except Exception as e:
        print(f"Error processing file: {e}")
        return None

def run_analysis(csv_content: str, text_content: str, settings: Dict) -> Tuple[str, str, str, str, str, str]:
    """Run the complete workflow analysis"""
    try:
        if not FULL_FEATURES:
            return run_simple_analysis(csv_content, text_content, settings)
        
        # Create workflow state
        state = WorkflowState(
            raw_csv=csv_content if csv_content else None,
            raw_text=text_content if text_content else None,
            settings=settings
        )
        
        # Run the LangGraph workflow
        final_state = run_workflow_analysis(state)
        
        # Generate reports
        dashboard_md = report_gen.generate_dashboard(final_state)
        bottlenecks_md = report_gen.generate_bottlenecks_report(final_state)
        recommendations_md = report_gen.generate_recommendations_report(final_state)
        risks_md = report_gen.generate_risks_report(final_state)
        graph_md = report_gen.generate_graph_summary(final_state)
        
        # Save run to storage
        run_id = storage.save_run(final_state)
        
        return dashboard_md, bottlenecks_md, recommendations_md, risks_md, graph_md, f"Analysis completed! Run ID: {run_id}"
        
    except Exception as e:
        error_msg = f"Error during analysis: {str(e)}"
        return error_msg, error_msg, error_msg, error_msg, error_msg, error_msg

def run_simple_analysis(csv_content: str, text_content: str, settings: Dict) -> Tuple[str, str, str, str, str, str]:
    """Simple analysis without full pipeline"""
    try:
        # Parse CSV if provided
        rows = []
        if csv_content:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content))
            rows = df.to_dict('records')
        
        # Basic analysis
        total_tasks = len(rows)
        owners = list(set(row.get('owner', 'Unknown') for row in rows)) if rows else []
        total_owners = len(owners)
        
        # Simple bottleneck detection
        bottlenecks = []
        for row in rows:
            if row.get('status') == 'blocked':
                bottlenecks.append({
                    'task_id': row.get('task_id', 'unknown'),
                    'title': row.get('title', 'Unknown'),
                    'owner': row.get('owner', 'Unknown'),
                    'type': 'blocked_task',
                    'score': 0.8,
                    'reason': 'Task is blocked'
                })
        
        # Check for overloaded owners
        owner_task_counts = {}
        for row in rows:
            owner = row.get('owner', 'Unknown')
            owner_task_counts[owner] = owner_task_counts.get(owner, 0) + 1
        
        # Find overloaded owners
        overloaded_owners = [owner for owner, count in owner_task_counts.items() if count > 3]
        
        # Add overloaded owner bottlenecks
        for owner in overloaded_owners:
            bottlenecks.append({
                'task_id': f'owner-{owner}',
                'title': f'Owner {owner} overloaded',
                'owner': owner,
                'type': 'overloaded_owner',
                'score': 0.7,
                'reason': f'Owner has {owner_task_counts[owner]} tasks'
            })
        
        # Generate reports
        dashboard = f"""## Dashboard

### Key Metrics
- **Total Tasks**: {total_tasks}
- **Team Members**: {total_owners}
- **Bottlenecks Detected**: {len(bottlenecks)}
- **Overloaded Owners**: {len(overloaded_owners)}

### Team Members
{', '.join(owners) if owners else 'None'}

### Task Distribution
{chr(10).join([f"- **{owner}**: {count} tasks" for owner, count in owner_task_counts.items()])}"""

        bottlenecks_report = "## Bottlenecks\n\n"
        if bottlenecks:
            for i, bottleneck in enumerate(bottlenecks, 1):
                bottlenecks_report += f"{i}. **{bottleneck['title']}** - {bottleneck['reason']}\n"
        else:
            bottlenecks_report += "No bottlenecks detected."

        recommendations = "## Recommendations\n\n"
        if bottlenecks:
            recommendations += "1. **Review blocked tasks** - Check dependencies and blockers\n"
            recommendations += "2. **Reassign if needed** - Consider moving tasks to available team members\n"
            if overloaded_owners:
                recommendations += f"3. **Balance workload** - {', '.join(overloaded_owners)} have too many tasks\n"
        else:
            recommendations += "Workflow appears to be running smoothly!"

        risks = "## At-Risk Tasks\n\n"
        if overloaded_owners:
            risks += f"**Overloaded Owners**: {', '.join(overloaded_owners)} may struggle to complete tasks on time.\n\n"
        risks += "**Blocked Tasks**: Tasks with 'blocked' status need immediate attention."

        graph = "## Graph View\n\n"
        graph += f"Simple workflow with {total_tasks} tasks across {total_owners} team members.\n\n"
        graph += f"**Workload Distribution**:\n"
        for owner, count in owner_task_counts.items():
            graph += f"- {owner}: {count} tasks\n"

        run_log = f"Analysis completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        run_log += f"Found {len(bottlenecks)} bottlenecks and {len(overloaded_owners)} overloaded owners."
        
        return dashboard, bottlenecks_report, recommendations, risks, graph, run_log
        
    except Exception as e:
        error_msg = f"Error during analysis: {str(e)}"
        return error_msg, error_msg, error_msg, error_msg, error_msg, error_msg

def export_markdown(dashboard: str, bottlenecks: str, recommendations: str, risks: str, graph: str) -> str:
    """Export all reports as a single markdown file"""
    full_report = f"""# UnclogAI Analysis Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Dashboard
{dashboard}

## Bottlenecks
{bottlenecks}

## Recommendations
{recommendations}

## Risk Forecast
{risks}

## Graph Summary
{graph}
"""
    return full_report

def export_csvs(state: WorkflowState) -> Tuple[str, str, str]:
    """Export bottlenecks, recommendations, and risks as CSV"""
    bottlenecks_csv = report_gen.export_bottlenecks_csv(state)
    recommendations_csv = report_gen.export_recommendations_csv(state)
    risks_csv = report_gen.export_risks_csv(state)
    return bottlenecks_csv, recommendations_csv, risks_csv

def get_previous_runs() -> List[Tuple[str, str]]:
    """Get list of previous runs for dropdown"""
    if not FULL_FEATURES or not storage:
        return []
    
    try:
        runs = storage.get_all_runs()
        return [(f"Run {r['id']} - {r['timestamp']}", str(r['id'])) for r in runs]
    except:
        return []

def load_previous_run(run_id: str) -> Tuple[str, str, str, str, str, str]:
    """Load a previous run and display results"""
    if not run_id or not FULL_FEATURES or not storage:
        return "", "", "", "", "", "No run selected or storage unavailable"
    
    try:
        state = storage.load_run(int(run_id))
        if not state:
            return "", "", "", "", "", "Run not found"
        
        # Generate reports for loaded state
        dashboard_md = report_gen.generate_dashboard(state)
        bottlenecks_md = report_gen.generate_bottlenecks_report(state)
        recommendations_md = report_gen.generate_recommendations_report(state)
        risks_md = report_gen.generate_risks_report(state)
        graph_md = report_gen.generate_graph_summary(state)
        
        return dashboard_md, bottlenecks_md, recommendations_md, risks_md, graph_md, f"Loaded run {run_id}"
        
    except Exception as e:
        error_msg = f"Error loading run: {str(e)}"
        return error_msg, error_msg, error_msg, error_msg, error_msg, error_msg

# Create Gradio interface
def create_interface():
    with gr.Blocks(title="UnclogAI - Workflow Bottleneck Detection", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# UnclogAI - Workflow Bottleneck Detection")
        gr.Markdown("Upload your workflow data or paste updates to detect bottlenecks and get actionable recommendations.")
        
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("## Data Input")
                
                # File upload
                file_upload = gr.File(
                    label="Upload CSV/JSON file",
                    file_types=[".csv", ".json"]
                )
                
                # Text input
                text_input = gr.Textbox(
                    label="Or paste workflow updates",
                    placeholder="Paste your workflow updates here...",
                    lines=5
                )
                
                # Settings
                gr.Markdown("## Settings")
                due_soon_days = gr.Number(
                    value=7, label="Due Soon Window (days)", minimum=1, maximum=30
                )
                aging_threshold = gr.Number(
                    value=5, label="Aging Threshold (days)", minimum=1, maximum=30
                )
                owner_load_threshold = gr.Number(
                    value=3, label="Owner Load Threshold", minimum=1, maximum=10
                )
                
                # Analysis button
                analyze_btn = gr.Button("Analyze Workflow", variant="primary", size="lg")
                
                # Previous runs
                gr.Markdown("## Previous Runs")
                runs_dropdown = gr.Dropdown(
                    choices=get_previous_runs(),
                    label="Load Previous Run",
                    interactive=True
                )
                load_run_btn = gr.Button("Load Run", variant="secondary")
                
            with gr.Column(scale=2):
                # Results tabs
                with gr.Tabs():
                    with gr.Tab("Dashboard"):
                        dashboard_output = gr.Markdown()
                    
                    with gr.Tab("Bottlenecks"):
                        bottlenecks_output = gr.Markdown()
                    
                    with gr.Tab("Recommendations"):
                        recommendations_output = gr.Markdown()
                    
                    with gr.Tab("At-Risk Tasks"):
                        risks_output = gr.Markdown()
                    
                    with gr.Tab("Graph View"):
                        graph_output = gr.Markdown()
                    
                    with gr.Tab("Run Log"):
                        run_log_output = gr.Markdown()
                
                # Export buttons
                gr.Markdown("## Export Results")
                with gr.Row():
                    export_md_btn = gr.Button("Export Markdown", variant="secondary")
                    export_csv_btn = gr.Button("Export CSVs", variant="secondary")
                    copy_summary_btn = gr.Button("Copy Summary", variant="secondary")
                
                export_output = gr.Textbox(label="Export Output", lines=3, interactive=False)
        
        # Event handlers
        def on_analyze(file, text, due_soon, aging, load_thresh):
            settings = {
                "due_soon_days": due_soon,
                "aging_threshold": aging,
                "owner_load_threshold": load_thresh
            }
            
            csv_content = None
            if file:
                try:
                    if file.endswith('.csv'):
                        df = pd.read_csv(file)
                        csv_content = df.to_csv(index=False)
                    elif file.endswith('.json'):
                        with open(file, 'r') as f:
                            data = json.load(f)
                        csv_content = json.dumps(data, indent=2)
                except Exception as e:
                    return "", "", "", "", "", f"Error processing file: {e}"
            
            return run_analysis(csv_content, text, settings)
        
        def on_export_markdown(dashboard, bottlenecks, recommendations, risks, graph):
            return export_markdown(dashboard, bottlenecks, recommendations, risks, graph)
        
        def on_load_run(run_id):
            return load_previous_run(run_id)
        
        # Connect events
        analyze_btn.click(
            fn=on_analyze,
            inputs=[file_upload, text_input, due_soon_days, aging_threshold, owner_load_threshold],
            outputs=[dashboard_output, bottlenecks_output, recommendations_output, risks_output, graph_output, run_log_output]
        )
        
        load_run_btn.click(
            fn=on_load_run,
            inputs=[runs_dropdown],
            outputs=[dashboard_output, bottlenecks_output, recommendations_output, risks_output, graph_output, run_log_output]
        )
        
        export_md_btn.click(
            fn=on_export_markdown,
            inputs=[dashboard_output, bottlenecks_output, recommendations_output, risks_output, graph_output],
            outputs=[export_output]
        )
        
        copy_summary_btn.click(
            fn=lambda d: f"Summary copied to clipboard!\n\n{d[:500]}..." if d else "No data to copy",
            inputs=[dashboard_output],
            outputs=[export_output]
        )
    
    return demo

# Demo data info
def get_demo_info():
    return """
## 🎯 Demo Data Available

A sample dataset is included at `demo_data/ops_sample.csv` with:
- 15 tasks across 4 team members
- Mixed statuses (todo, in_progress, blocked, done)
- Dependencies and deadlines
- Overloaded owner scenario
- Deep dependency chain

Upload this file to see OpsPilot in action!
"""

if __name__ == "__main__":
    # Initialize storage if available
    if FULL_FEATURES and storage:
        try:
            storage.initialize()
            print("✅ Database initialized")
        except Exception as e:
            print(f"⚠️ Database initialization failed: {e}")
    
    # Create and launch interface
    demo = create_interface()
    
    print("🚀 Starting UnclogAI...")
    if FULL_FEATURES:
        print("✅ Full features enabled")
    else:
        print("⚠️ Running in simple mode - some features may be limited")
    
    # Try different ports if 7860 is busy
    ports_to_try = [7860, 7861, 7862, 7863, 7864]
    for port in ports_to_try:
        try:
            demo.launch(
                server_name="0.0.0.0",  # Use 0.0.0.0 for better compatibility
                server_port=port,
                share=True,  # Enable sharing to avoid localhost issues
                show_error=True,
                inbrowser=True,  # Open browser automatically
                quiet=False  # Show startup messages
            )
            break
        except OSError as e:
            if "address already in use" in str(e) and port < ports_to_try[-1]:
                print(f"Port {port} is busy, trying port {port + 1}...")
                continue
            else:
                raise e
