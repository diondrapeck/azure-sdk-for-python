import os 
from dotenv import load_dotenv

from azure.identity import DefaultAzureCredential

from azure.ai.evaluation import (
    ContentSafetyEvaluator,
    SexualMultimodalEvaluator,
    SexualEvaluator,
    ProtectedMaterialMultimodalEvaluator,
    ProtectedMaterialEvaluator,
    F1ScoreEvaluator,
    FluencyEvaluator,
    GroundednessEvaluator,
    GroundednessProEvaluator,
    RetrievalEvaluator,
    evaluate,
)
from azure.ai.evaluation._constants import DefectRate, Sum, Mean

load_dotenv()
model_config = {
    "azure_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT"),
    "api_key": os.environ.get("AZURE_OPENAI_KEY"),
    "azure_deployment": os.environ.get("AZURE_OPENAI_DEPLOYMENT"),
}
azure_ai_project = {
    "subscription_id": os.environ.get("AZURE_SUBSCRIPTION_ID"),
    "resource_group_name": os.environ.get("AZURE_RESOURCE_GROUP"),
    "project_name": os.environ.get("AZURE_PROJECT_NAME"),
}
credential = DefaultAzureCredential()

groundedness_eval = GroundednessEvaluator(model_config) # non-content safety
f1_score_eval = F1ScoreEvaluator() # non-content safety
sexual_eval = SexualEvaluator(azure_ai_project=azure_ai_project, credential=credential) # content safety
sexual_eval.aggregator = Sum() # non-content safety

# run the evaluation
result = evaluate(
    data="tests/e2etests/data/evaluate_test_data.jsonl",
    evaluators={"grounded": groundedness_eval, "f1_score": f1_score_eval, "sexual": sexual_eval},
)
print("Printing result")
# print(result)