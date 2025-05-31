# Car Price Predictor

A lightweight, fully local machine learning pipeline to predict car prices based on real online listings in Iran.  
This project scrapes real car ads from Bama.ir, processes the data, and trains a regression model to estimate vehicle prices based on features like brand, model, year, mileage, and city.

---

## 🔍 Features

- Web scraper for extracting car listings from [bama.ir](https://bama.ir)
- Clean and structured CSV data ready for analysis
- Exploratory data analysis (EDA) using Jupyter Notebooks
- Predictive modeling using Scikit-learn (linear regression & more)
- Modular, clean project structure suitable for real-world applications

---

## 💡 Future Plans

- Improve scraper with more robust error handling and pagination
- Add support for multiple car brands and filtering by city
- Build a simple front-end interface to let users input car details and get a predicted price
- Deploy the model as a small web app or API

---

## 🧠 Tech Stack

- Python
- BeautifulSoup + Requests
- Pandas & NumPy
- Scikit-learn
- Jupyter Notebook

---

## 🛠️ Folder Structure

```bash
car-price-predictor/
├── data/
├── notebooks/
├── scraper/
├── utils/
├── models/
├── main.py
├── requirements.txt
└── README.md
