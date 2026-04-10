.PHONY: install lint format test run

install:
	pip install -r requirements.txt

lint:
	ruff check src tests app.py

format:
	ruff format src tests app.py

test:
	pytest -q

run:
	streamlit run app.py
