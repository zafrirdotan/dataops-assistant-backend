import json
import re
from urllib import response
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
        # Todo: improve the file path handling
        prompt = f"""
        Given the following pipeline specification:
        {json.dumps(spec, indent=2)}
        
        And the following data preview:
        {json.dumps(data_preview, indent=2)}
        
        The input data files are located in '../../data/'. Please use this path in the generated code.
        
        Generate Python code to perform the specified transformations and load the data into the destination.
        Also, generate a requirements.txt listing all necessary Python packages.
        Return only two code blocks: one with Python code (```python ... ```), and one with requirements.txt (```requirements.txt ... ```).
        Do not include explanations or extra text.
        """

        response = self.llm.response_create(
            model="gpt-4.1",
            input=prompt,
            temperature=0,
        )

        python_code = self.extract_code_block(response.output_text, "python")
        requirements = self.extract_code_block(response.output_text, "requirements.txt")
        return python_code, requirements

    def extract_code_block(self, llm_response: str, block_type: str) -> str:
        # Extract code between triple backticks with block_type
        pattern = rf"```{block_type}(.*?)```"
        match = re.search(pattern, llm_response, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""
