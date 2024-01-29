#!/bin/zsh

# Install dependencies
source ./venv/bin/activate
pip install -r requirements.txt

# Run the app
python main.py
