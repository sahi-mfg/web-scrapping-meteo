install:
	@echo "Installing project dependencies"
	@pip install --upgrade pip
	@pip install -r requirements.txt
	@echo "Done"


run:
	@echo "Running the program"
	@python3 main.py
	@echo "Done"


