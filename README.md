# MySQL Script Runner

A simple Python application to execute MySQL `.sql` scripts in a specified order.

## Prerequisites
- Python 3.8 or higher
- MySQL server

## Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/mysql-script-runner.git
   cd mysql-script-runner
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure database connection in `config.py`.

4. Add your MySQL scripts to the `scripts/` folder and specify their order in `SCRIPT_ORDER` in `config.py`.

## Run
Execute the application:
```bash
python main.py
```

## Notes
- Ensure your MySQL server is running and accessible.
- Scripts are executed in the order specified in `SCRIPT_ORDER`.