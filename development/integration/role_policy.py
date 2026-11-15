"""Deployment-owned economic defaults; no task can raise its own model tier."""
MODEL = "gpt-6-luna"
REASONING = {"orchestrator": "medium", "builder": "high", "reviewer": "high", "librarian": "low", "researcher": "medium"}
MODELS = {role: ("gpt-6.1-sol" if role == "builder" else MODEL) for role in REASONING}
