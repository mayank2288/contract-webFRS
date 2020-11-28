from flask import Flask, request, render_template
from flask_restful import Resource
import pandas as pd

app = Flask(__name__)

@app.route('/')
def my_form():
   

    return render_template('form_ex.html')
@app.route('/home')
@app.route('/', methods = ['POST'])

def my_form_post():
        text = request.form['JDE_file']
        df = pd.read_csv(text)
        asa = str(df.shape)
        return asa





if __name__ == '__main__':
    app.run('localhost',5050)


