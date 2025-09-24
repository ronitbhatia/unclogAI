"""
Prompt templates for OpsPilot LLM interactions
Contains all prompts used for text parsing and recommendation generation
"""

# System prompt for consistent AI behavior
SYSTEM_PROMPT = """You are an operations analyst AI. Use concise, verifiable, business-appropriate language. If data is missing, say 'unknown'. Prefer bullet points. Never invent tasks or dates."""

# Text-to-rows extraction prompt
TEXT_TO_ROWS_PROMPT = """From the update text below, extract tasks with fields:
task_id (slug), title, owner, status(one of: todo, in_progress, blocked, done), start_date(YYYY-MM-DD or null), due_date(YYYY-MM-DD or null), dependency_ids(list of task ids), priority(low/med/high), effort(1..5), notes.
Return strict JSON array only.

TEXT:
{raw_text}"""

# Recommendations generation prompt
RECOMMENDATIONS_PROMPT = """You are a workflow optimizer. Given a task and context (owner load, dependencies, due dates), propose 1–3 concrete actions. Focus on realism: reassign to available owners, split, escalate, or renegotiate. Each action: {{title, rationale, expected_effect(<=20 words)}}. Return strict JSON.

Task: {task_title}
Owner: {owner}
Bottleneck Type: {bottleneck_type}
Reason: {bottleneck_reason}

Context:
{context}

Return JSON array of recommendations:"""

# Risk analysis prompt
RISK_ANALYSIS_PROMPT = """Analyze the following task for potential risks and provide a risk assessment:

Task: {task_title}
Owner: {owner}
Status: {status}
Priority: {priority}
Effort: {effort}
Due Date: {due_date}

Context:
{context}

Provide risk factors and mitigation suggestions in JSON format."""

# Bottleneck analysis prompt
BOTTLENECK_ANALYSIS_PROMPT = """Analyze the following bottleneck and provide detailed insights:

Task: {task_title}
Owner: {owner}
Bottleneck Type: {bottleneck_type}
Score: {score}
Reason: {reason}

Context:
{context}

Provide analysis and potential root causes in JSON format."""

# Workflow optimization prompt
WORKFLOW_OPTIMIZATION_PROMPT = """Analyze the following workflow data and suggest optimizations:

Tasks: {num_tasks}
Owners: {num_owners}
Bottlenecks: {num_bottlenecks}
Risks: {num_risks}

Context:
{context}

Provide high-level optimization recommendations in JSON format."""

# All prompts dictionary
PROMPTS = {
    'system': SYSTEM_PROMPT,
    'text_to_rows': TEXT_TO_ROWS_PROMPT,
    'recommendations': RECOMMENDATIONS_PROMPT,
    'risk_analysis': RISK_ANALYSIS_PROMPT,
    'bottleneck_analysis': BOTTLENECK_ANALYSIS_PROMPT,
    'workflow_optimization': WORKFLOW_OPTIMIZATION_PROMPT
}

# Prompt validation and formatting helpers
def format_prompt(prompt_name: str, **kwargs) -> str:
    """Format a prompt with the given parameters"""
    if prompt_name not in PROMPTS:
        raise ValueError(f"Unknown prompt: {prompt_name}")
    
    prompt_template = PROMPTS[prompt_name]
    
    try:
        return prompt_template.format(**kwargs)
    except KeyError as e:
        raise ValueError(f"Missing parameter for prompt {prompt_name}: {e}")

def validate_prompt_params(prompt_name: str, params: dict) -> bool:
    """Validate that all required parameters are provided for a prompt"""
    if prompt_name not in PROMPTS:
        return False
    
    prompt_template = PROMPTS[prompt_name]
    
    # Extract parameter names from the template
    import re
    param_names = re.findall(r'\{(\w+)\}', prompt_template)
    
    # Check if all parameters are provided
    for param_name in param_names:
        if param_name not in params:
            return False
    
    return True

# Specific prompt builders for common use cases
def build_text_extraction_prompt(text: str) -> str:
    """Build a prompt for extracting tasks from free-form text"""
    return format_prompt('text_to_rows', raw_text=text)

def build_recommendation_prompt(task_title: str, owner: str, bottleneck_type: str, 
                               bottleneck_reason: str, context: str) -> str:
    """Build a prompt for generating recommendations"""
    return format_prompt('recommendations',
                        task_title=task_title,
                        owner=owner,
                        bottleneck_type=bottleneck_type,
                        bottleneck_reason=bottleneck_reason,
                        context=context)

def build_risk_analysis_prompt(task_title: str, owner: str, status: str, 
                              priority: str, effort: int, due_date: str, 
                              context: str) -> str:
    """Build a prompt for risk analysis"""
    return format_prompt('risk_analysis',
                        task_title=task_title,
                        owner=owner,
                        status=status,
                        priority=priority,
                        effort=effort,
                        due_date=due_date,
                        context=context)

def build_bottleneck_analysis_prompt(task_title: str, owner: str, 
                                   bottleneck_type: str, score: float, 
                                   reason: str, context: str) -> str:
    """Build a prompt for bottleneck analysis"""
    return format_prompt('bottleneck_analysis',
                        task_title=task_title,
                        owner=owner,
                        bottleneck_type=bottleneck_type,
                        score=score,
                        reason=reason,
                        context=context)

def build_workflow_optimization_prompt(num_tasks: int, num_owners: int, 
                                      num_bottlenecks: int, num_risks: int, 
                                      context: str) -> str:
    """Build a prompt for workflow optimization"""
    return format_prompt('workflow_optimization',
                        num_tasks=num_tasks,
                        num_owners=num_owners,
                        num_bottlenecks=num_bottlenecks,
                        num_risks=num_risks,
                        context=context)

# Prompt examples for testing
EXAMPLE_PROMPTS = {
    'text_extraction': {
        'input': 'John is working on the API integration. Sarah has the database migration blocked. Mike needs to finish the frontend by Friday.',
        'expected_output': 'JSON array with extracted tasks'
    },
    'recommendations': {
        'input': 'Task: API Integration, Owner: John, Type: overloaded_owner, Reason: Owner has 5 tasks in progress',
        'expected_output': 'JSON array with actionable recommendations'
    },
    'risk_analysis': {
        'input': 'Task: Database Migration, Owner: Sarah, Status: blocked, Priority: high, Due: 2024-01-15',
        'expected_output': 'JSON with risk factors and mitigation'
    }
}

# Prompt quality metrics
PROMPT_METRICS = {
    'text_to_rows': {
        'max_tokens': 512,
        'temperature': 0.1,
        'expected_fields': ['task_id', 'title', 'owner', 'status', 'priority', 'effort']
    },
    'recommendations': {
        'max_tokens': 256,
        'temperature': 0.3,
        'expected_fields': ['title', 'rationale', 'expected_effect']
    },
    'risk_analysis': {
        'max_tokens': 200,
        'temperature': 0.2,
        'expected_fields': ['risk_factors', 'mitigation']
    }
}

def get_prompt_config(prompt_name: str) -> dict:
    """Get configuration for a specific prompt"""
    return PROMPT_METRICS.get(prompt_name, {
        'max_tokens': 256,
        'temperature': 0.2,
        'expected_fields': []
    })
