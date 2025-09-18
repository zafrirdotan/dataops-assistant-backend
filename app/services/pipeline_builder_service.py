import jsonschema

from app.services.prompt_guard_service import PromptGuardService
from app.services.llm_service import LLMService
from app.services.pipeline_spec_generator import PipelineSpecGenerator
from app.services.pipeline_spec_generator import ETL_SPEC_SCHEMA
from app.services.source.local_file_service import LocalFileService

class PipelineBuilderService:
    def __init__(self):
        self.guard = PromptGuardService()
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator()
        self.local_file_service = LocalFileService()
        # Add other initializations as needed

    def build_pipeline(self, user_input: str) -> dict:
        # 2. Generate JSON spec
        spec = self.spec_gen.generate_spec(user_input)

        # 3. Validate schema
        if not self.validate_spec_schema(spec):
            return {"error": "Spec schema validation failed."}

        # 4. Try connecting to source/destination
        db_info = self.connect_to_source(spec)
        if not db_info.get("success"):
            return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}

        # 5. Generate pipeline code
        # code = self.generate_pipeline_code(spec, db_info)
        # if not code:
        #     return {"error": "Pipeline code generation failed."}

        # # 6. Create and run unit test
        # test_result = self.create_and_run_unittest(code)
        # if not test_result.get("success"):
        #     return {"error": "Unit test failed.", "details": test_result.get("details")}

        # # 7. Deploy
        # deploy_result = self.deploy_pipeline(code)
        # if not deploy_result.get("success"):
        #     return {"error": "Deployment failed.", "details": deploy_result.get("details")}

        # # 8. E2E tests
        # e2e_result = self.run_e2e_tests(deploy_result)
        # if not e2e_result.get("success"):
        #     return {"error": "E2E tests failed.", "details": e2e_result.get("details")}

        return {
            "success": True,
            "spec": spec,
            # "code": code,
            # "unit_test": test_result,
            # "deployment": deploy_result,
            # "e2e_test": e2e_result
        }

    def validate_spec_schema(self, spec: dict) -> bool:
        # Validate spec against ETL_SPEC_SCHEMA using jsonschema
        try:
            jsonschema.validate(instance=spec, schema=ETL_SPEC_SCHEMA)
            return self.validate_source_path(spec)
        except ImportError:
            print("jsonschema package is not installed.")
            return False
        except jsonschema.ValidationError as e:
            print(f"Schema validation error: {e}")
            return False


    def validate_source_path(self, spec: dict) -> None:
        # If source_type is localFileCSV or localFileJSON, ensure source_path exists
        match spec.get("source_type"):
            case "localFileCSV":
                if not spec.get("source_path", "").endswith('.csv'):
                    return False
            case "localFileJSON":
                if not spec.get("source_path", "").endswith('.json'):
                    return False
            case _:
                pass
        return True

    def connect_to_source(self, spec: dict) -> dict:
        # Try connecting to source/destination based on spec

        match spec.get("source_type"):
            case "PostgreSQL":
                pass
            case "localFileCSV":
                data = self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18")
                if data is not None:
                    data_preview = data.head().to_dict(orient="records")
                    return {"success": True, "data_preview": data_preview}
                else:
                    return {"success": False, "details": "No recent data files found."}
            case "localFileJSON":
                if self.local_file_service.check_file_exists(spec.get("source_path")):
                    return {"success": True, "data_preview": data_preview}
                else:
                    return {"success": False, "details": "No recent data files found."}
            case "sqlLite":
                pass
            case "api":
                pass

        return {"success": True}

    def generate_pipeline_code(self, spec: dict, db_info: dict) -> str:
        # TODO: Implement code generation logic
        return "# pipeline code"

    def create_and_run_unittest(self, code: str) -> dict:
        # TODO: Implement unit test generation and execution
        return {"success": True}

    def deploy_pipeline(self, code: str) -> dict:
        # TODO: Implement deployment logic
        return {"success": True}

    def run_e2e_tests(self, deploy_result: dict) -> dict:
        # TODO: Implement E2E test logic
        return {"success": True}
