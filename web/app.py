from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pymysql

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='123456',  # ⚠️ 换成你真实的MySQL密码
        database='ecommerce_ads',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route('/')
def index():
    return render_template('index.html')

# API 1: 漏斗与趋势
@app.route('/api/funnel')
def get_funnel_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ads_funnel_daily ORDER BY dt")
            return jsonify({"code": 200, "data": cursor.fetchall()})
    finally:
        conn.close()

# API 2: TopN 商品
@app.route('/api/topn')
def get_topn_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(dt) AS max_dt FROM ads_topn_item_daily")
            max_dt = cursor.fetchone()['max_dt']
            cursor.execute("SELECT item_id, buy_cnt, `rank` FROM ads_topn_item_daily WHERE dt = %s ORDER BY `rank`", (max_dt,))
            return jsonify({"code": 200, "dt": max_dt, "data": cursor.fetchall()})
    finally:
        conn.close()

# API 3: 复购率
@app.route('/api/repurchase')
def get_repurchase_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ads_repurchase_rate")
            return jsonify({"code": 200, "data": cursor.fetchone()})
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
