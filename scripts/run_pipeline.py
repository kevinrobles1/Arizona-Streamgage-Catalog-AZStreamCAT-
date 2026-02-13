from pathlib import Path
from src.pipeline import run_pipeline

if __name__ == "__main__":
    res = run_pipeline(Path("config/config.yaml"))
    print("Pipeline finished:")
    for k, v in res.items():
        print(f"  {k}: {v}")
