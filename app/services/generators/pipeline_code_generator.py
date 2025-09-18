import json
import re
from app.services.llm_service import LLMService

class PipelineCodeGenerator:
    """
    Service for retrieving a data sample and generating transformation code using LLM.
    """
    def __init__(self):
        self.llm = LLMService()

    def generate_code(self, spec: dict, data_preview: dict) -> str:
        """
        Generate transformation code based on the pipeline specification and database info.
        Args:
            spec (dict): The pipeline specification.
            db_info (dict): Information about the source/destination databases.
        Returns:
            str: Generated transformation code.
        """

        # Construct the prompt for the LLM
        prompt = f"""
        Given the following pipeline specification:
        {json.dumps(spec, indent=2)}
        
        And the following data preview:
        {json.dumps(data_preview, indent=2)}
        
        Generate Python code to perform the specified transformations and load the data into the destination.
        Ensure the code is secure and follows best practices.
        Return only the code inside a single Python fenced block. Do not include markdown formatting, explanations, text, or comments before or after.
        """

        response = self.llm.response_create(
            model="gpt-4.1",
            input=prompt,
            temperature=0,
        )

        return self.extract_python_code(response.output_text)

    def extract_python_code(self, llm_response: str) -> str:
        # Extract code between triple backticks with 'python'
        match = re.search(r"```python(.*?)```", llm_response, re.DOTALL)
        if match:
            return match.group(1).strip()
        return llm_response.strip()  # fallback: return whole response
