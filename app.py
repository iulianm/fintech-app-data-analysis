from flask import Flask, Response, request, json
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
from pandas_datareader import data as wb

from flask_cors import CORS

app = Flask(__name__)
CORS(app)

engine = create_engine(
    'mysql+mysqlconnector://investor:password@127.0.0.1:3306/fintech', echo=False)

#### Variables Dictionary ####

# STK - Stock
# SR  - Simple Return
# LR  - Logarithmic Return
# ARD - Average Return Daily
# ARA - Average Return Annual
# PTF - Portfolio


#** Load Prices in MySQL db **#


@app.route('/prices/load-prices', methods=['POST'])
def loadPricesInDb():

    req_data = request.get_json()

    for key, value in req_data.items():
        STK = wb.DataReader(value, data_source='iex',
                            start='2015-1-1', retry_count=3, pause=0.01)
        STK['date'] = STK.index
        STK.to_sql(name=value.lower(), con=engine,
                   if_exists='append', index=False)

    return("Prices for all positions were loaded")

#** Simple Returns / Average Returns **#


@app.route('/returns/simple/<string:ticket>')
def getSimpleReturns(ticket, **kwargs):
    STK = pd.read_sql(
        'SELECT * FROM {ticket}'.format(ticket=ticket), con=engine)

    # STK['simple_return'] is a pandas.Series, a one-dimensional array -> it needs to be converted into a dataframe
    SR = (STK['close'] / STK['close'].shift(1)) - 1
    SR = SR.to_frame()
    SR['date'] = STK['date']

    AVD = SR.mean()
    # print('Daily average return', str(round(AVD, 5) * 100) + '%')
    AVA = SR.mean() * 250
    # print('Annual average return * 250', AVA)
    # print('Annual average return', str(round(AVA, 5) * 100) + '%')
    # print(str(round(AVA, 5) * 100) + '%')

    return Response(response=SR.to_json(orient="records"), mimetype='application/json')

#** Logarithmic Returns / Average Returns **#


@app.route('/returns/logarithmic/<string:ticket>')
def getLogarithmicReturns(ticket, **kwargs):
    STK = pd.read_sql(
        'SELECT * FROM {ticket}'.format(ticket=ticket), con=engine)

    LR = np.log(STK['close'] / STK['close'].shift(1))
    LR = LR.to_frame()
    LR['date'] = STK['date']

    AVD = LR.mean()
    AVA = LR.mean() * 250
    # print('Annual average return * 250', AVA)
    # print('Annual average return', str(round(AVA, 5) * 100) + '%')
    # print(str(round(AVA, 5) * 100) + '%')

    return Response(response=LR.to_json(orient="records"), mimetype='application/json')


@app.route('/returns/portfolio', methods=['POST'])
def loadClosePricesPortfolio():

    # get all the stock to be queried, form the request object
    req_data = request.get_json()

    PST = ''
    for key, value in req_data.items():
        PST = PST + value.lower() + "_close" + ", "

    # it is important to add also the date and extract the last comma at the end of the created string
    PST = 'date, ' + PST
    PST = PST[:-2]

    PTF = pd.read_sql(
        'SELECT {PST} FROM price_close'.format(PST=PST), con=engine)
    # set the index to equal 'date' column
    PTF.set_index('date', inplace=True)
    # normalize to 100
    PTF = ((PTF / PTF.iloc[0]) * 100)

    # get the max and min for all columns of the dataframe
    columnsMAX = PTF.max()
    columnsMIN = PTF.min()

    # compare the max and min of all these columns in order to get the max and min of the entire dataframe
    dfMAX = columnsMAX.max()
    dfMIN = columnsMIN.min()

    # create new column 'Date' with values of the existent index of the df
    PTF['date'] = PTF.index

    # data is a dictionary
    data = {
        'prices': PTF.to_dict('records'),
        'max': {'maxPrice': dfMAX},
        'min': {'minPrice': dfMIN}
    }

    return Response(response=json.dumps(data), mimetype='application/json')


if __name__ == "__main__":
    app.run(debug=True)
