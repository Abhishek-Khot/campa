# asgp/providers/litellm_provider.py
"""
LiteLLM provider wrapper for ASGP
Handles completion, JSON parsing, embeddings, safety checks
"""
import litellm
from typing import List, Dict, Any, Optional
import json
import asyncio
import re

class LiteLLMProvider:
    """
    Unified LLM interface using LiteLLM
    Supports all LiteLLM-compatible models
    """
    
    @staticmethod
    async def complete(
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int = 2048,
        temperature: float = 0.0,
        timeout: int = 30
    ) -> str:
        """
        Standard text completion with retry logic
        
        Args:
            messages: OpenAI-format message list
            model: Any litellm model string
            max_tokens: Max tokens in response
            temperature: 0.0 to 2.0
            timeout: Request timeout in seconds
            
        Returns:
            Completion text
        """
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=timeout
                )
                return response.choices[0].message.content
                
            except litellm.exceptions.RateLimitError as e:
                if attempt < 2:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s
                    await asyncio.sleep(wait_time)
                    continue
                raise RuntimeError(f"Rate limit exceeded after 3 attempts: {e}")
                
            except litellm.exceptions.Timeout:
                raise RuntimeError(f"LLM request timeout after {timeout}s")
                
            except Exception as e:
                raise RuntimeError(f"LLM completion failed: {str(e)}")
    
    @staticmethod
    async def complete_json(
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int = 2048,
        temperature: float = 0.0
    ) -> Dict[str, Any]:
        """
        JSON-mode completion with automatic retry on parse failure
        
        Args:
            messages: OpenAI-format message list
            model: Any litellm model string
            max_tokens: Max tokens in response
            temperature: 0.0 to 2.0
            
        Returns:
            Parsed JSON dictionary
        """
        # First attempt - normal completion
        response_text = await LiteLLMProvider.complete(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        # Try to parse JSON
        try:
            cleaned = LiteLLMProvider._clean_json_response(response_text)
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            # Retry with explicit JSON instruction
            retry_messages = messages + [
                {
                    "role": "assistant",
                    "content": response_text
                },
                {
                    "role": "user",
                    "content": "Your response was not valid JSON. Return ONLY a valid JSON object with no explanation or markdown formatting."
                }
            ]
            
            retry_response = await LiteLLMProvider.complete(
                messages=retry_messages,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature
            )
            
            try:
                cleaned = LiteLLMProvider._clean_json_response(retry_response)
                return json.loads(cleaned)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"LLM failed to return valid JSON after retry. "
                    f"Original: {response_text[:200]}... "
                    f"Retry: {retry_response[:200]}..."
                )
    
    @staticmethod
    def _clean_json_response(text: str) -> str:
        """
        Remove markdown fences and extract JSON object/array
        """
        # Remove markdown code blocks
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        
        # Find first { or [ and last } or ]
        start_brace = text.find('{')
        start_bracket = text.find('[')
        
        if start_brace == -1 and start_bracket == -1:
            return text.strip()
        
        if start_brace != -1 and (start_bracket == -1 or start_brace < start_bracket):
            start = start_brace
            end = text.rfind('}') + 1
        else:
            start = start_bracket
            end = text.rfind(']') + 1
        
        return text[start:end].strip() if end > start else text.strip()
    
    @staticmethod
    async def embed(
        text: str,
        model: str = "text-embedding-3-small"
    ) -> List[float]:
        """
        Generate text embedding vector
        
        Args:
            text: Text to embed
            model: Embedding model name
            
        Returns:
            Embedding vector as list of floats
        """
        try:
            response = await litellm.aembedding(
                model=model,
                input=text
            )
            return response.data[0]['embedding']
        except Exception as e:
            raise RuntimeError(f"Embedding generation failed: {str(e)}")
    
    @staticmethod
    async def safety_check(
        prompt: str,
        model: str = "gpt-4o-mini"
    ) -> bool:
        """
        Binary safety classification (Layer 2 of safety guard)
        
        Args:
            prompt: User prompt to check
            model: Cheap/fast model for classification
            
        Returns:
            True if safe (read-only), False if potentially harmful
        """
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a database safety classifier. "
                    "Determine if a query is READ-ONLY or contains WRITE operations. "
                    "Answer with ONLY 'YES' if read-only, 'NO' if it modifies data."
                )
            },
            {
                "role": "user",
                "content": f"Is this query read-only?\n\n{prompt}"
            }
        ]
        
        try:
            response = await LiteLLMProvider.complete(
                messages=messages,
                model=model,
                max_tokens=10,
                temperature=0.0,
                timeout=3
            )
            
            answer = response.strip().upper()
            return answer.startswith('YES')
            
        except:
            # On error, fail closed (assume unsafe)
            return False
