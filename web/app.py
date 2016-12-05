""" Handles Flask server requests"""
import urllib
import json
from bs4 import BeautifulSoup
from cassandra.cluster import Cluster
from flask import Flask, render_template, request


APP = Flask(__name__)
CONFIG_OBJECT = json.loads(open("../config.json").read())


def product_by_id(product_id):
    """
    Get Product description by product id
    :param product_id: Id of the product
    :return:
    """
    host = "https://cymax.com/"

    site_data = urllib.urlopen(host + str(product_id) + '--C0.htm').read()
    soup = BeautifulSoup(site_data)

    product = soup.find_all("div", class_="product-item")

    # if search result is more tha one item,
    # it's most likely returning all items
    if len(product) == 1:
        product_description = product[0].find(class_="product-description").getText()
        product_img = product[0].find(class_="product-item-img")["src"]

        return product_description, product_img


def fetch_product_by_id(product_id):
    """
    Fetch product from cassandra database
    :param product_id: Id of the product to fetch
    :return:
    """
    host = CONFIG_OBJECT["CassandraHosts"]

    cluster = Cluster(host)
    session = cluster.connect(CONFIG_OBJECT["KeySpace"])

    # Query table to find recommended items
    item1 = session.execute('select productID2 from '
                            + CONFIG_OBJECT["RecommendTable"] +
                            ' where productID1 = %d LIMIT 5' % product_id)
    rec_set = set()
    item2 = session.execute('select productID1 from '
                            + CONFIG_OBJECT["RecommendTable"] +
                            ' where productID2 = %d LIMIT 5' % product_id)
    for item in item2:
        rec_set.add(item[0])

    for item in item1:
        rec_set.add(item[0])
    return rec_set


@APP.route('/')
def index():
    """
    Function to be called for default request
    :return:
    """
    return render_template('RecommendUI.html')


@APP.route('/getRecommendationById', methods=['GET', 'POST'])
def recommend():
    """
    Handle recommend request
    :return:
    """
    product_id = request.form['prod_id']
    response_object = list()

    # Gets the original product
    podesc = product_by_id(product_id)

    # if product not found
    if podesc is not None:
        original_product_description, original_image = podesc[0], podesc[1]
        rec_set = fetch_product_by_id(int(product_id))

        response_object.append({'description': original_product_description,
                                'image': original_image})
        for item in rec_set:
            pdesc = product_by_id(item)

            # If product does not exist in cymax website
            if pdesc is not None:
                product_description = pdesc[0]
                product_img = pdesc[1]

                response_object.append({'description': product_description,
                                        'image': product_img})
        return json.dumps(response_object)
    else:
        return ""


if __name__ == '__main__':
    APP.run(debug=True, host='0.0.0.0')
