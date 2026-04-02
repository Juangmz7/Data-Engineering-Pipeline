# Data Engineering Pipeline

This repository contains a Python-based data engineering project organized into three main areas:

- `RealTimeProcessing`: components for reading and validating real-time data inputs.
- `BatchProcessing`: components for batch data transformation and output writing.
- `shared`: shared contracts, utilities, and validation logic used across the project.

This project is for the Data Engineering course in UCLL.

## Project Status

The intended execution model for this project is Apache Airflow. The long-term goal is to run these processing components inside an Airflow DAG.

At the moment, the DAG has not been declared yet, so the repository is currently structured as a set of reusable modules plus automated tests. Until the Airflow orchestration layer is added, the main way to validate the project is by running the test suite.

## Prerequisites

- Python 3.10 or newer
- `pip`
- PowerShell if you are working on Windows

## 1. Create the Virtual Environment

From the project root:

```powershell
python -m venv .venv
```

## 2. Activate the Virtual Environment

In Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

To deactivate it later:

```powershell
deactivate
```

## 3. Install Dependencies

Install the project dependencies from the root `requirements.txt` file:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The current `requirements.txt` already includes the runtime and testing dependencies, including `pytest`.

## 4. Run the Project

There is no single application entry point yet, because the project is being prepared for future execution through Apache Airflow.

Once the Airflow DAG is implemented, the orchestration layer will be responsible for running the processing flow. For now, the recommended way to run and validate the project locally is through the tests.

Run the full test suite:

```powershell
python -m pytest
```

Run tests for a specific module:

```powershell
python -m pytest .\BatchProcessing\test
python -m pytest .\RealTimeProcessing\test
python -m pytest .\shared\test
```

## 5. Project Structure

```text
Data-Engineering-Pipeline/
|-- BatchProcessing/
|   |-- src/
|   `-- test/
|-- RealTimeProcessing/
|   |-- src/
|   `-- test/
|-- shared/
|   |-- contracts/
|   |-- util/
|   |-- validator/
|   `-- test/
|-- requirements.txt
`-- README.md
```

## Notes

- The `.venv` virtual environment is already ignored in `.gitignore`.
- If `Activate.ps1` is blocked by PowerShell execution policies, you can start a temporary session with a relaxed policy:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```
