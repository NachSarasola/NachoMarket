#!/bin/bash
rsync -avz --progress \
  --exclude='venv/' \
  --exclude='__pycache__/' \
  --exclude='.git/' \
  --exclude='*.pyc' \
  --exclude='*.pyo' \
  --exclude='data/ trades. jsonl' \
  --exclude='data/nachomarket. log' \
  --exclude='data/state. json' \
  --exclude='data/reviews/' \
  --exclude='.pytest_cache/' \
  --exclude='.claude/' \
  --exclude='plan. pdf' \
  ./ "dublin:nachomarket/"