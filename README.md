# QuerySense AI 🧠📊
**QuerySense AI** is an intelligent, conversational analytics assistant designed to democratize data access. It allows users to query large Import/Export databases using natural language, automatically generating SQL queries, fetching results, and presenting them through interactive charts and AI-generated business insights.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Flask](https://img.shields.io/badge/Flask-2.0%2B-green)
![Gemini](https://img.shields.io/badge/AI-Google%20Gemini-orange)
![Chart.js](https://img.shields.io/badge/Frontend-Chart.js-pink)
![Tailwind](https://img.shields.io/badge/Style-TailwindCSS-cyan)

* **Natural Language to SQL:** Uses **Google Gemini 2.5 Flash** to translate English questions (e.g., *"Show me top 10 zinc importers"*) into optimized T-SQL queries.
* **Intelligent Analysis:** Beyond raw data, the agent generates a text summary explaining market trends, pricing insights, and key players based on the retrieved data.
* **Dynamic Visualization:** Automatically selects the best visualization (Bar, Line, or Pie charts) based on the data structure (Time-series vs. Categorical).
* **Large Dataset Handling:** Includes a background job system to handle large Excel exports (10k+ rows) without freezing the UI.
* **Context Awareness:** Distinguishes between Import and Export contexts automatically and handles fuzzy matching for product/company names.
* **Interactive UI:** A clean, chat-based interface built with HTML5, Tailwind CSS, and vanilla JavaScript.

## 🛠️ Tech Stack

* **Backend:** Python, Flask, SQLAlchemy, PyODBC
* **AI Engine:** Google Generative AI (Gemini)
* **Database:** MS SQL Server
* **Frontend:** HTML5, Tailwind CSS (CDN), Chart.js (CDN), SheetJS
* **Data Processing:** Pandas, Difflib, XlsxWriter

## 📂 Project Structure

```
QuerySense-AI/
├── agent.py            # Core logic: LLM configuration, SQL generation, DB connection
├── app.py              # Flask application entry point and API routes
├── .env                # Environment variables (API keys, DB creds)
├── templates/
│   └── index.html      # Main frontend UI
├── exports/            # Directory for generated Excel files
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

## ⚙️ Installation & Setup

**1. Clone the Repository**
```
git clone [https://github.com/yourusername/querysense-ai.git](https://github.com/yourusername/querysense-ai.git)
cd querysense-ai
```

**2. Create a Virtual Environment**
```
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate
```

**3. Install Dependencies**

Create a requirements.txt file (if not present) and install packages:
```
pip install -r  requirements.txt
```

**4. Database Drivers**

Ensure you have the ODBC Driver 17 for SQL Server installed on your machine to allow Python to connect to MSSQL.

**5. Configuration (.env)**

Create a .env file in the root directory and add your credentials. Use the template below:
```
# Database Configuration
DB_SERVER=your_server_address
DB_NAME=your_database_name
DB_USERNAME=your_db_username
DB_PASSWORD=your_db_password
DB_DRIVER=ODBC Driver 17 for SQL Server

# Google Gemini API Key
GOOGLE_API_KEY=your_google_api_key
```

## ▶️ Usage

**1. Run the Flask Application:**
```
python app.py
```
**2. Access the Interface:** Open your browser and navigate to: ```http://127.0.0.1:5005```

**3. Start Querying:** Try asking questions like:

* *"Analyze the top 15 companies exporting rice."*
* *"Compare import trends of steel for 2023 vs 2024."*
* *"What is the total value of aluminum exports last month?"*
* *"Download full data for zinc imports."*

## 🧠 Database Requirements

The agent is designed to work with specific SQL views. Ensure your database has the following views or modify ```agent.py``` to match your schema:

1. ```View_Clean_Imports```: Should contain ```BE_Date```, ```[Importer/Exporter_Name]```, ```Product_Name```, ```Total_Value_INR```, ```QUANTITY_KG```.
2. ```View_Clean_Exports```: Should contain ```SB_Date```, ```[Importer/Exporter_Name]```, ```Product_Name```, ```Total_Value_INR```, ```QUANTITY_KG```.

## 🛡️ Security Note

* **Read-Only:** The agent executes generated SQL queries. Ensure the database user provided in the ```.env``` file has **read-only permissions** to prevent accidental data modification or dropping of tables.

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements.

## 📄 License

This project is open-source and available under the MIT License.
