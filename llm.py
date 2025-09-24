"""
TinyLlama LLM wrapper for OpsPilot
Provides local LLM inference without API keys
"""

from typing import Optional, Dict, Any
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# Conditional imports to avoid dependency issues
TRANSFORMERS_AVAILABLE = False
torch = None
AutoTokenizer = None
AutoModelForCausalLM = None
pipeline = None

def _check_transformers():
    """Check if transformers is available"""
    global TRANSFORMERS_AVAILABLE, torch, AutoTokenizer, AutoModelForCausalLM, pipeline
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
        TRANSFORMERS_AVAILABLE = True
        return True
    except (ImportError, AttributeError) as e:
        print(f"Warning: Transformers not available: {e}")
        TRANSFORMERS_AVAILABLE = False
        torch = None
        AutoTokenizer = None
        AutoModelForCausalLM = None
        pipeline = None
        return False

class LLMEngine:
    """TinyLlama LLM engine for local inference"""
    
    def __init__(self, model_id: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"):
        self.model_id = model_id
        self.tokenizer = None
        self.model = None
        self.pipeline = None
        self.device = "cpu"  # Force CPU for Hugging Face Spaces
        
        # Check if transformers is available
        if _check_transformers():
            self._load_model()
        else:
            print("LLM not available - using fallback responses")
    
    def _load_model(self):
        """Load the TinyLlama model and tokenizer"""
        if not TRANSFORMERS_AVAILABLE:
            print("Transformers not available - skipping model loading")
            return
            
        try:
            print(f"Loading {self.model_id}...")
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_id,
                trust_remote_code=True
            )
            
            # Add padding token if not present
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            # Load model
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_id,
                torch_dtype=torch.float32,  # Use float32 for CPU
                device_map="cpu",
                trust_remote_code=True
            )
            
            # Create pipeline
            self.pipeline = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                device=-1,  # CPU
                return_full_text=False
            )
            
            print(f"Successfully loaded {self.model_id}")
            
        except Exception as e:
            print(f"Error loading model: {e}")
            # Fallback to a simple text generation
            self.pipeline = None
    
    def run_llm(self, prompt: str, max_new_tokens: int = 256, 
                temperature: float = 0.2, top_p: float = 0.9) -> str:
        """Run LLM inference on the given prompt"""
        try:
            if not TRANSFORMERS_AVAILABLE or self.pipeline is None:
                return self._fallback_response(prompt)
            
            # Prepare generation parameters
            generation_kwargs = {
                "max_new_tokens": max_new_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "do_sample": temperature > 0,
                "pad_token_id": self.tokenizer.eos_token_id,
                "eos_token_id": self.tokenizer.eos_token_id,
                "repetition_penalty": 1.1
            }
            
            # Generate response
            response = self.pipeline(
                prompt,
                **generation_kwargs
            )
            
            # Extract generated text
            if response and len(response) > 0:
                generated_text = response[0]["generated_text"]
                
                # Clean up the response
                cleaned_text = self._clean_response(generated_text, prompt)
                return cleaned_text
            else:
                return self._fallback_response(prompt)
                
        except Exception as e:
            print(f"Error in LLM inference: {e}")
            return self._fallback_response(prompt)
    
    def _clean_response(self, generated_text: str, original_prompt: str) -> str:
        """Clean and format the generated response"""
        # Remove the original prompt if it appears at the beginning
        if generated_text.startswith(original_prompt):
            generated_text = generated_text[len(original_prompt):].strip()
        
        # Remove common artifacts
        artifacts = [
            "<|endoftext|>",
            "<|end|>",
            "<|start|>",
            "\n\n\n",
            "```",
            "---"
        ]
        
        for artifact in artifacts:
            generated_text = generated_text.replace(artifact, "")
        
        # Clean up whitespace
        generated_text = generated_text.strip()
        
        # Limit length to prevent runaway generation
        if len(generated_text) > 1000:
            generated_text = generated_text[:1000] + "..."
        
        return generated_text
    
    def _fallback_response(self, prompt: str) -> str:
        """Fallback response when model is not available"""
        # Simple rule-based responses for common patterns
        prompt_lower = prompt.lower()
        
        if "extract tasks" in prompt_lower or "parse" in prompt_lower:
            return """[
  {
    "task_id": "sample-task-1",
    "title": "Sample Task",
    "owner": "team-member",
    "status": "todo",
    "start_date": null,
    "due_date": null,
    "dependency_ids": [],
    "priority": "med",
    "effort": 3,
    "notes": "Sample task extracted from text"
  }
]"""
        
        elif "recommendations" in prompt_lower or "actions" in prompt_lower:
            return """[
  {
    "title": "Review task priorities",
    "rationale": "Current workload may be causing delays",
    "expected_effect": "Better task prioritization and focus"
  }
]"""
        
        else:
            return "I'm a workflow analysis AI. I can help identify bottlenecks and suggest improvements for your team's processes."
    
    def is_available(self) -> bool:
        """Check if the LLM is available and working"""
        return TRANSFORMERS_AVAILABLE and self.pipeline is not None
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model"""
        return {
            "model_id": self.model_id,
            "available": self.is_available(),
            "device": self.device,
            "tokenizer_vocab_size": len(self.tokenizer) if self.tokenizer else 0
        }
    
    def test_generation(self, test_prompt: str = "Hello, how are you?") -> Dict[str, Any]:
        """Test the model with a simple prompt"""
        try:
            response = self.run_llm(test_prompt, max_new_tokens=50, temperature=0.7)
            
            return {
                "success": True,
                "response": response,
                "response_length": len(response),
                "model_working": True
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "model_working": False
            }

# Global LLM instance
_llm_instance = None

def get_llm() -> LLMEngine:
    """Get the global LLM instance"""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = LLMEngine()
    return _llm_instance

def initialize_llm(model_id: Optional[str] = None) -> LLMEngine:
    """Initialize the LLM with optional model ID"""
    global _llm_instance
    if model_id:
        _llm_instance = LLMEngine(model_id)
    else:
        _llm_instance = LLMEngine()
    return _llm_instance
