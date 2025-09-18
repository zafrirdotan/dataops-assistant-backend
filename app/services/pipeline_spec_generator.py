import json
from app.services.llm_service import LLMService

ETL_SPEC_SCHEMA = {
    "type": "object",
    "properties": {
        "source_type": {
            "type": "string",
            "description": "The source of the data this will be a file path, database connection string, API endpoint, etc.",
            "enum": ["localFileCSV","localFileJSON",  "PostgreSQL", "api"],
        },
        "source_path": {
            "type": "string",
            "description": "The path to the source file (if source is localFile)",
        },
        "destination_type": {
            "type": "string",
            "description": "The destination for the data",
            "enum": ["sqlLite", "PostgreSQL", "file"],
        },
        "destination_name": {
            "type": "string",
            "description": "The name of the destination table or file without extension",
        },
        "transformation": {
            "type": "string",
            "description": "Transformations to apply to the data in python",
        },
        "schedule": {
            "type": "string",
            "description": "Cron schedule for the pipeline",
        }
    },
    "required": ["source_type", "source_path", "destination_type", "destination_name", "transformation", "schedule"],
    "additionalProperties": False,
}

class PipelineSpecGenerator:
    """
    Service for generating pipeline specifications (specs) for ML/data pipelines.
    """
    def __init__(self):
        self.llm = LLMService()

    def generate_spec(self, user_input: str) -> dict:
        """
        Generate a pipeline specification from user input or requirements.
        Args:
            user_input (str): Description or requirements for the pipeline.
        Returns:
            dict: A dictionary representing the pipeline specification.
        """
        response = self.llm.response_create(
            model = "gpt-4.1",
            input = f"Generate a pipeline spec for: {user_input}",
            temperature = 0,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "extract_json",
                    "strict": True,
                    "schema": ETL_SPEC_SCHEMA,
                }
            }
        )
        # Ensure the response is always a list, then convert each item to a dict
        return json.loads(response.output_text)
