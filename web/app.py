from flask import Flask, jsonify, render_template, request
from flask import request
from bs4 import BeautifulSoup
import urllib
import json
app = Flask(__name__)

def product_by_id(product_id):
    """
    Get Product description by product id
    """
    host = "https://cymax.com/"

    r = urllib.urlopen(host + str(product_id) + '--C0.htm').read()
    soup = BeautifulSoup(r)

    product = soup.find_all("div", class_="product-item")
    product_description = product[0].find(class_="product-description").getText()
    product_img = product[0].find(class_="product-item-img")["src"]

    return product_description, product_img

def fetch_product_by_id(product_id):
   host = ["127.0.0.1"]
   rec_set = set()
   rec_set.add(158942)
   rec_set.add(494423)
   rec_set.add(501590)
   rec_set.add(440811)
   rec_set.add(494423)
   # cluster = Cluster(host)
   # session = cluster.connect("cmpt732")
   # item1 = session.execute('select productID2 from RecommendProducts where productID1 = %d' % product_id)
   # rec_set = set()
   # for item in item1:
   #     rec_set.add(item[0])
   # item2 = session.execute('select productID1 from RecommendProducts where productID2 = %d ALLOW FILTERING' % item[0])
   # for item in item2:
   #     rec_set.add(item[0])
   return rec_set
@app.route('/')
def index():
    return render_template('RecommendUI.html')

@app.route('/getRecommendationById', methods=['GET', 'POST'])
def getRecommendationById():
    product_id = request.form['prod_id']
    response_object = list()
    original_product_description,original_image = product_by_id(product_id)
    rec_set = fetch_product_by_id(product_id)
    response_object.append({'description': original_product_description, 'image': original_image})
    for item in rec_set:
        product_description, product_img = product_by_id(item)
        response_object.append({'description': product_description, 'image': product_img})
    return json.dumps(response_object)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')




