#! /bin/bash
mongoimport --host mongobench --db mongobench --collection ordersCol --type json '/mongo-seed/Order.json'