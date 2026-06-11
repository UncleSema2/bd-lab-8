import json
import tempfile
import time
from http.client import RemoteDisconnected
from urllib.error import URLError
from urllib.request import Request, urlopen


class DataMartClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def load_training_data(self, spark, input_path: str, min_non_null_ratio: float, target_rows: int, seed: int):
        response = self._post(
            "/v1/training-data",
            {
                "inputPath": input_path,
                "minNonNullRatio": min_non_null_ratio,
                "targetRows": target_rows,
                "seed": seed,
            },
        )
        data = response["data"]
        rows = data["rows"]
        if not rows:
            raise ValueError("Data mart returned empty dataset")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".jsonl", delete=False) as f:
            path = f.name
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")

        return (
            spark.read.json(path),
            data["featureCols"],
            int(data["totalRows"]),
            int(data["workingRows"]),
        )

    def save_training_results(self, predictions_df, metrics: dict) -> None:
        self._post(
            "/v1/training-results",
            {
                "predictions": [row.asDict() for row in predictions_df.toLocalIterator()],
                "metrics": [metrics],
            },
        )

    def _post(self, path: str, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        parsed = None
        last_error = None
        for _ in range(30):
            try:
                with urlopen(request, timeout=300) as response:
                    parsed = json.loads(response.read().decode("utf-8"))
                break
            except (URLError, RemoteDisconnected, ConnectionError) as error:
                last_error = error
                time.sleep(2)

        if parsed is None:
            raise RuntimeError(f"Data mart unavailable: {last_error}")

        if parsed.get("status") != "ok":
            raise RuntimeError(parsed.get("error", "Data mart request failed"))

        return parsed
