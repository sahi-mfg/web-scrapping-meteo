#!/usr/bin/env sh

# Check if the venv directory exists
if [ ! -d "./venv" ]
then
    # Create the virtual environment
    python3 -m venv venv
fi

# Activate the virtual environment
# shellcheck disable=SC2039
source ./venv/bin/activate


# Install dependencies
pip install -r requirements.txt

# Run the program
python main.py
