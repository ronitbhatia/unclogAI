# UnclogAI - Workflow Bottleneck Detection

A production-ready MVP for detecting workflow bottlenecks and generating actionable recommendations using local LLM inference.

## 🚀 Features

- **No API Keys Required**: Uses TinyLlama locally on CPU
- **Multi-format Data Ingestion**: CSV, JSON, and free-form text
- **Advanced Bottleneck Detection**: Network analysis, owner overload, aging tasks
- **AI-Powered Recommendations**: LLM-generated actionable suggestions
- **Risk Forecasting**: Identifies at-risk tasks and potential delays
- **Interactive Web Interface**: Clean, professional Gradio UI
- **Export Capabilities**: Markdown reports, CSV exports, and summaries
- **Persistent Storage**: SQLite database for analysis history

## 🎯 Problem Solved

UnclogAI helps teams identify and resolve workflow bottlenecks by:

1. **Ingesting** task data from various sources (CSV, JSON, text)
2. **Building** workflow dependency graphs
3. **Detecting** bottlenecks using network analysis and ML
4. **Generating** actionable recommendations using local LLM
5. **Forecasting** risks and potential delays
6. **Providing** clean, professional reports

## 🛠️ Technology Stack

- **Frontend**: Gradio (Python web framework)
- **Backend**: Python with LangGraph orchestration
- **LLM**: TinyLlama-1.1B-Chat (local inference)
- **Graph Analysis**: NetworkX
- **Database**: SQLite
- **Data Processing**: Pandas, NumPy

## 📦 Installation

### Prerequisites

- Python 3.8+
- 4GB+ RAM (for TinyLlama)
- 2GB+ disk space

### Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/unclogAI.git
cd unclogAI

# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
```

The app will be available at `http://localhost:7860`

### Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/unclogAI.git
cd unclogAI

# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
```

## 🎮 Usage

### 1. Upload Data
- Upload CSV/JSON files with task data
- Or paste workflow updates directly

### 2. Configure Settings
- **Due Soon Window**: Days to consider tasks as "due soon"
- **Aging Threshold**: Days to flag tasks as "aging"
- **Owner Load Threshold**: Maximum tasks per owner

### 3. Analyze Workflow
- Click "Analyze Workflow" to run the analysis
- View results in different tabs:
  - **Dashboard**: Key metrics and overview
  - **Bottlenecks**: Detected workflow issues
  - **Recommendations**: AI-generated suggestions
  - **At-Risk Tasks**: Potential delays and risks
  - **Graph View**: Workflow visualization

### 4. Export Results
- Export Markdown reports
- Export CSV data
- Copy summaries for sharing

## 📊 Data Format

### CSV Format
```csv
task_id,title,owner,status,priority,effort,start_date,due_date,notes
T-001,Implement user service,Alice,in_progress,high,medium,2024-01-01,2024-01-15,Core service
T-002,Setup database,Bob,completed,high,low,2023-12-20,2024-01-05,PostgreSQL
```

### JSON Format
```json
{
  "tasks": [
    {
      "task_id": "T-001",
      "title": "Implement user service",
      "owner": "Alice",
      "status": "in_progress",
      "priority": "high",
      "effort": "medium"
    }
  ]
}
```

## 🔧 Configuration

### Environment Variables
- `GRADIO_SERVER_PORT`: Port for the web interface (default: 7860)
- `DATABASE_PATH`: SQLite database path (default: ops_pilot.db)

### Settings
- **Due Soon Window**: 1-30 days (default: 7)
- **Aging Threshold**: 1-30 days (default: 5)
- **Owner Load Threshold**: 1-10 tasks (default: 3)

## 📈 Analysis Features

### Bottleneck Detection
- **Blocked Tasks**: Tasks with dependencies preventing progress
- **Overloaded Owners**: Team members with too many tasks
- **Aging Tasks**: Tasks stuck in progress for too long
- **Critical Path Analysis**: Tasks on the critical path

### Risk Assessment
- **Due Date Risks**: Tasks likely to miss deadlines
- **Resource Conflicts**: Competing demands on team members
- **Dependency Risks**: Tasks blocked by incomplete dependencies
- **Workload Imbalances**: Uneven distribution of work

### Recommendations
- **Priority Adjustments**: Suggested task reprioritization
- **Resource Reallocation**: Workload balancing suggestions
- **Deadline Negotiations**: Timeline adjustment recommendations
- **Process Improvements**: Workflow optimization suggestions

## 🏗️ Architecture

### Core Components

```
UnclogAI/
├── app.py                 # Main Gradio application
├── graph.py              # LangGraph workflow orchestration
├── ingest.py             # Data ingestion system
├── graph_builder.py      # NetworkX graph construction
├── detector.py           # Bottleneck detection algorithms
├── recommender.py        # LLM-powered recommendation engine
├── forecaster.py         # Risk forecasting system
├── report.py             # Report generation and export
├── storage.py            # SQLite persistence layer
├── llm.py                # TinyLlama LLM wrapper
├── prompts.py            # Prompt templates
├── demo_data/            # Sample datasets
│   └── ops_sample.csv
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

### Data Flow

1. **Data Ingestion** → Parse CSV/JSON/text input
2. **Graph Construction** → Build workflow dependency graph
3. **Bottleneck Detection** → Identify workflow issues
4. **LLM Analysis** → Generate recommendations
5. **Risk Forecasting** → Predict potential delays
6. **Report Generation** → Create actionable reports
7. **Storage** → Persist results to SQLite

## 🚀 Deployment

### Local Deployment
```bash
python app.py
```

### Hugging Face Spaces
1. Create a new Space
2. Upload the code files
3. Add `requirements.txt`
4. Set the main file to `app.py`

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 7860

CMD ["python", "app.py"]
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **TinyLlama**: For the efficient local LLM
- **Gradio**: For the beautiful web interface
- **NetworkX**: For graph analysis capabilities
- **LangGraph**: For workflow orchestration

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/unclogAI/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/unclogAI/discussions)
- **Email**: support@unclogai.com

## 🔮 Roadmap

- [ ] **Advanced Analytics**: More sophisticated bottleneck detection
- [ ] **Team Collaboration**: Multi-user support and sharing
- [ ] **Integration APIs**: Connect with project management tools
- [ ] **Mobile App**: Native mobile interface
- [ ] **AI Improvements**: Better recommendation algorithms
- [ ] **Custom Models**: Support for custom LLM models

---

**UnclogAI** - Making workflow bottlenecks visible and actionable! 🚀