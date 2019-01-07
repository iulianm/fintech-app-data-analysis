from flask import Flask, Response, request
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
from pandas_datareader import data as wb

app = Flask(__name__)

engine = create_engine(
    'mysql+mysqlconnector://investor:password@127.0.0.1:3306/financial_data', echo=False)

#### RETURNS ####

# STK - Stock
# SR  - Simple Return
# LR  - Logarithmic Return
# ARD - Average Return Daily
# ARA - Average Return Annual

#** Load Prices in MySQL db **#


@app.route('/prices/load-prices', methods=['POST'])
def loadPricesInDb():

    req_data = request.get_json()

    for key, value in req_data.items():
        # STK = wb.DataReader('AAPL', data_source='iex', start='2015-1-1')
        # STK.to_sql(name='AAPL', con=engine, if_exists='append', index=False)

        STK = wb.DataReader(value, data_source='iex',
                            start='2015-1-1', retry_count=3, pause=0.01)
        STK.to_sql(name=value.lower(), con=engine,
                   if_exists='append', index=False)
        print('Prices for ', {value}, ' position were loaded in the db')

    return("Prices for all positions were loaded")

#** Simple Returns / Average Returns **#


@app.route('/returns/calculate-simple-returns')
def calculateSimpleReturns():
    STK = pd.read_sql('SELECT * FROM aapl', con=engine)

    # STK['simple_return'] is a pandas.Series, a one-dimensional array -> it needs to be converted into a dataframe
    STK['simple_return'] = (STK['close'] / STK['close'].shift(1)) - 1
    SR = STK['simple_return'].to_frame()
    SR['date'] = SR['simple_return'].index

    AVD = SR.mean()
    # print('Daily average return', str(round(AVD, 5) * 100) + '%')
    AVA = SR.mean() * 250
    # print('Annual average return * 250', AVA)
    # print('Annual average return', str(round(AVA, 5) * 100) + '%')
    # print(str(round(AVA, 5) * 100) + '%')

    return Response(response=SR.to_json(orient="records"), mimetype='application/json')

#** Logarithmic Returns / Average Returns **#


@app.route('/returns/calculate-logarithmic-returns')
def calculateLogarithmicReturns():
    STK = pd.read_sql('SELECT * FROM aapl', con=engine)

    STK['log_return'] = np.log(STK['close'] / STK['close'].shift(1))
    LR = STK['simple_return'].to_frame()
    LR['date'] = LR['simple_return'].index

    AVD = LR.mean()
    AVA = LR.mean() * 250
    # print('Annual average return * 250', AVA)
    # print('Annual average return', str(round(AVA, 5) * 100) + '%')
    # print(str(round(AVA, 5) * 100) + '%')

    return Response(response=LR.to_json(orient="records"), mimetype='application/json')


if __name__ == "__main__":
    app.run(debug=True)
