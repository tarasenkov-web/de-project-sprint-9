import time
import uuid 
from datetime import datetime
from logging import Logger
from typing import List, Dict
from lib.kafka_connect import KafkaConsumer, KafkaProducer  
from lib.redis import RedisClient 
from dds_loader.repository import DdsRepository  

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger,
                 output_topic: str) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger
        self._output_topic = output_topic  

    def run(self) -> None:
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            h_user_pk = uuid.uuid4() 
            load_dt = datetime.utcnow()  
            load_src = 'orders-system-kafka' 

            user_id = order["user"]["id"]
            user = self._redis.get(user_id)
            user_name = user["name"]
            
            restaurant_id = order['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant["name"]
            self._dds_repository.h_user_insert(h_user_pk, user_id, load_dt, load_src)
            
            h_order_pk = uuid.uuid4() 
            order_id = order["id"] 
            order_dt = order["date"] 
            self._dds_repository.h_order_insert(h_order_pk, order_id, order_dt, load_dt, load_src)
            hk_order_user_pk = uuid.uuid4()  
            self._dds_repository.l_order_user_insert(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)

            h_restaurant_pk = uuid.uuid4() 
            self._dds_repository.h_restaurant_insert(h_restaurant_pk, restaurant_id, load_dt, load_src)

            cost = order["cost"] 
            payment = order["payment"]  
            hk_order_cost_hashdiff = uuid.uuid4()  
            self._dds_repository.s_order_cost_insert(h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)

            status = order["status"]  
            hk_order_status_hashdiff = uuid.uuid4()
            self._dds_repository.s_order_status_insert(h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)

            hk_user_names_hashdiff = uuid.uuid4()
            self._dds_repository.s_user_names_insert(h_user_pk, user_name, user_name, load_dt, load_src, hk_user_names_hashdiff)

            products_with_category_id = [] 

            for product in order["products"]:
                h_product_pk = uuid.uuid4() 
                product_id = product["id"]
                hk_order_product_pk = uuid.uuid4()
                self._dds_repository.h_product_insert(h_product_pk, product_id, load_dt, load_src)
                self._dds_repository.l_order_product_insert(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                h_category_pk = uuid.uuid4()  
                category_name = product["category"]
                self._dds_repository.h_category_insert(h_category_pk, category_name, load_dt, load_src)
                hk_product_category_pk = uuid.uuid4()  
                self._dds_repository.l_product_category_insert(hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                hk_product_restaurant_pk = uuid.uuid4() 
                self._dds_repository.l_product_restaurant_insert(hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                product_name = product["name"]
                hk_product_names_hashdiff = uuid.uuid4()
                self._dds_repository.s_product_names_insert(h_order_pk, product_name, load_dt, load_src, hk_product_names_hashdiff)

                products_with_category_id.append({
                    "id": product_id,
                    "name": product_name,
                    "category_id": h_category_pk,
                    "quantity": product["quantity"],
                    "price": product["price"],
                    "category": menu_item["category"]
                })
            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name),
                    "products": products_with_category_id
                }
            }

            self._producer.produce(self._output_topic, dst_msg)
            self._logger.info(f"{datetime.utcnow()}: Message sent to {self._output_topic}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    # def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
    #     items = []

    #     menu = restaurant["menu"]
    #     for it in order_items:
    #         menu_item = next(x for x in menu if x["_id"] == it["id"])
    #         dst_it = {
    #             "id": it["id"],
    #             "price": it["price"],
    #             "quantity": it["quantity"],
    #             "name": menu_item["name"],
    #             "category": menu_item["category"]
    #         }
    #         items.append(dst_it)

    #     return items
