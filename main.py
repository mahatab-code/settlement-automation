name: Daily Settlement Automation

on:
  workflow_dispatch:
  schedule:
    - cron: '0 3 * * *'   # 9:00 AM Bangladesh Time (UTC 03:00)

jobs:
  run-bot:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Chrome and ChromeDriver
        run: |
          sudo apt-get update
          sudo apt-get install -y google-chrome-stable
          # Install ChromeDriver
          sudo apt-get install -y chromium-chromedriver
          # Verify installations
          google-chrome --version
          chromedriver --version

      - name: Install Python Dependencies
        run: |
          pip install --upgrade pip
          pip install selenium pandas sqlalchemy pytz webdriver-manager
          # If you have requirements.txt
          # pip install -r requirements.txt

      - name: Run main.py
        env:
          COMPANY_EMAIL: ${{ secrets.COMPANY_EMAIL }}
          COMPANY_PASSWORD: ${{ secrets.COMPANY_PASSWORD }}
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          python main.py
