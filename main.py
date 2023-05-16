# Original Source: https://github.com/irtiza07/python-cassandra-demo/blob/main/demo.py


# docker run --name test-cassandra-v2 -p 9042:9042  cassandra:latest
# docker exec -it test-cassandra-v2 bash
# pip3 install cassandra-driver

from cassandra.cluster import Cluster

cluster = Cluster(['0.0.0.0'], port=9042)
session = cluster.connect('store')

#! Reading Data From Cassandra [simple query]
print("Reading data simply..")
rows = session.execute('SELECT * FROM store.shopping_cart;')
for cart_row in rows:
    print(cart_row)
    print(f'id: {cart_row.userid}, item_count: {cart_row.item_count}.')

#! Reading Data From Cassandra [optimized query]
prepared_statement = session.prepare('SELECT * FROM store.shopping_cart WHERE userid=?')
userids = ["1", "2"]

print("Reading data using prepared statements..")
for userid in userids:
    cart = session.execute(prepared_statement, [userid]).one()
    print(cart)

# #! Writing data into cassandra
# session.execute("INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('3', 13, toTimeStamp(now()));")
# session.execute_async("INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('4', 14, toTimeStamp(now()));")

# rows = session.execute('SELECT * FROM store.shopping_cart;')
# for cart_row in rows:
#     print(cart_row)
#     print(f'id: {cart_row.userid}, item_count: {cart_row.item_count}.')

#! Updating data in cassandra
session.execute("ALTER TABLE store.shopping_cart ADD cart_type text;")
session.execute("INSERT INTO store.shopping_cart (userid, item_count, cart_type, last_update_timestamp) VALUES ('543', 10, 'online', toTimeStamp(now()));")
session.execute_async("INSERT INTO store.shopping_cart (userid, item_count, cart_type, last_update_timestamp) VALUES ('456', 25, 'in-person', toTimeStamp(now()));")

rows = session.execute('SELECT * FROM store.shopping_cart;')
for cart_row in rows:
    print(cart_row)
    print(f'id: {cart_row.userid}, item_count: {cart_row.item_count}.')
