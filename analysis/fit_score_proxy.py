from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


FIXED_COLUMNS = {
    "ID",
    "Player",
    "Team",
    "Position",
    "RkOv",
    "Status",
    "Opponent",
    "Score",
    "Ros",
    "+/-",
    "%D",
    "ADP",
    "GP",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fit proxy formulas for Fantrax Score based on visible stat columns."
    )
    parser.add_argument(
        "--input-dir",
        default="downloaddata",
        help="Directory containing players*.csv files",
    )
    parser.add_argument(
        "--output-json",
        default="exports/score_proxy_models.json",
        help="Where to save fitted season models as JSON",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=8000,
        help="Gradient descent iterations",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.05,
        help="Gradient descent learning rate",
    )
    parser.add_argument(
        "--ridge",
        type=float,
        default=0.001,
        help="L2 regularization strength",
    )
    return parser.parse_args()


def parse_optional_float(value: str | None) -> float:
    if value is None:
        return 0.0
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "-"}:
        return 0.0
    return float(cleaned)


def season_slug_from_filename(path: Path) -> str:
    suffix = path.stem.replace("players", "")
    if len(suffix) == 4 and suffix.isdigit():
        return f"{2000 + int(suffix[:2])}-{2000 + int(suffix[2:])}"
    if len(suffix) == 3 and suffix.isdigit():
        return f"{2000 + int(suffix[:2])}-{2000 + int(suffix[1:])}"
    raise ValueError(f"Could not derive season slug from {path.name}")


def player_csv_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("players*.csv"))


def load_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {csv_path}")
        return reader.fieldnames, list(reader)


def stat_columns(fieldnames: list[str]) -> list[str]:
    return [name for name in fieldnames if name not in FIXED_COLUMNS]


def column_max(rows: list[dict[str, str]], stat_key: str) -> float:
    values = [parse_optional_float(row.get(stat_key)) for row in rows]
    maximum = max(values, default=0.0)
    return maximum if maximum > 0 else 1.0


def build_design_matrix(rows: list[dict[str, str]], stats: list[str]) -> tuple[list[list[float]], list[float], dict[str, float]]:
    maxima = {stat: column_max(rows, stat) for stat in stats}
    x_matrix: list[list[float]] = []
    y_vector: list[float] = []

    for row in rows:
        x_matrix.append([
            parse_optional_float(row.get(stat)) / maxima[stat]
            for stat in stats
        ])
        y_vector.append(parse_optional_float(row.get("Score")))

    return x_matrix, y_vector, maxima


def predict_row(intercept: float, weights: list[float], row: list[float]) -> float:
    total = intercept
    for value, weight in zip(row, weights):
        total += value * weight
    return total


def fit_linear_model(
    x_matrix: list[list[float]],
    y_vector: list[float],
    iterations: int,
    learning_rate: float,
    ridge: float,
) -> tuple[float, list[float]]:
    feature_count = len(x_matrix[0]) if x_matrix else 0
    intercept = sum(y_vector) / len(y_vector) if y_vector else 0.0
    weights = [0.0 for _ in range(feature_count)]
    row_count = max(len(x_matrix), 1)

    for _ in range(iterations):
        intercept_gradient = 0.0
        weight_gradients = [0.0 for _ in range(feature_count)]

        for row, target in zip(x_matrix, y_vector):
            prediction = predict_row(intercept, weights, row)
            error = prediction - target
            intercept_gradient += error
            for index, value in enumerate(row):
                weight_gradients[index] += error * value

        intercept -= learning_rate * (2.0 * intercept_gradient / row_count)
        for index in range(feature_count):
            ridge_term = 2.0 * ridge * weights[index]
            weights[index] -= learning_rate * (
                2.0 * weight_gradients[index] / row_count + ridge_term
            )

    return intercept, weights


def model_metrics(intercept: float, weights: list[float], x_matrix: list[list[float]], y_vector: list[float]) -> tuple[float, float]:
    if not y_vector:
        return 0.0, 0.0

    predictions = [predict_row(intercept, weights, row) for row in x_matrix]
    mean_y = sum(y_vector) / len(y_vector)
    ss_tot = sum((value - mean_y) ** 2 for value in y_vector)
    ss_res = sum((actual - predicted) ** 2 for actual, predicted in zip(y_vector, predictions))
    mae = sum(abs(actual - predicted) for actual, predicted in zip(y_vector, predictions)) / len(y_vector)
    r_squared = 0.0 if math.isclose(ss_tot, 0.0) else 1.0 - (ss_res / ss_tot)
    return r_squared, mae


def raw_coefficients(weights: list[float], maxima: dict[str, float], stats: list[str]) -> dict[str, float]:
    return {
        stat: weight / maxima[stat]
        for stat, weight in zip(stats, weights)
    }


def formula_string(intercept: float, coefficients: dict[str, float]) -> str:
    parts = [f"{intercept:.4f}"]
    ranked = sorted(coefficients.items(), key=lambda item: abs(item[1]), reverse=True)
    for stat, coefficient in ranked:
        sign = "+" if coefficient >= 0 else "-"
        parts.append(f" {sign} {abs(coefficient):.4f}*{stat}")
    return "".join(parts)


def fit_season_model(csv_path: Path, iterations: int, learning_rate: float, ridge: float) -> dict[str, Any]:
    fieldnames, rows = load_rows(csv_path)
    stats = stat_columns(fieldnames)
    x_matrix, y_vector, maxima = build_design_matrix(rows, stats)
    intercept, weights = fit_linear_model(
        x_matrix,
        y_vector,
        iterations=iterations,
        learning_rate=learning_rate,
        ridge=ridge,
    )
    r_squared, mae = model_metrics(intercept, weights, x_matrix, y_vector)
    coefficients = raw_coefficients(weights, maxima, stats)
    ranked_coefficients = sorted(
        coefficients.items(),
        key=lambda item: abs(item[1]),
        reverse=True,
    )

    return {
        "season_slug": season_slug_from_filename(csv_path),
        "source_file": str(csv_path),
        "row_count": len(rows),
        "r_squared": round(r_squared, 4),
        "mae": round(mae, 4),
        "intercept": round(intercept, 6),
        "coefficients": {key: round(value, 6) for key, value in coefficients.items()},
        "top_coefficients": [
            {"stat_key": key, "coefficient": round(value, 6)}
            for key, value in ranked_coefficients[:10]
        ],
        "formula": formula_string(intercept, coefficients),
    }


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    files = player_csv_files(input_dir)
    if not files:
        raise FileNotFoundError(f"No players*.csv files found in {input_dir}")

    models = [
        fit_season_model(
            csv_path,
            iterations=args.iterations,
            learning_rate=args.learning_rate,
            ridge=args.ridge,
        )
        for csv_path in files
    ]

    output_path.write_text(
        json.dumps(models, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    for model in models:
        print("")
        print(f"Season: {model['season_slug']}")
        print(f"R²: {model['r_squared']}")
        print(f"MAE: {model['mae']}")
        print("Top coefficients:")
        for item in model["top_coefficients"]:
            print(f"  {item['stat_key']}: {item['coefficient']}")

    print("")
    print(f"Saved models to {output_path}")


if __name__ == "__main__":
    main()

