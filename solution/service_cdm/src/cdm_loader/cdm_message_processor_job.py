from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 ) -> None:

        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: START")

            order = msg['payload']
            user_id = order["user"]["id"]
            restaurant_id = order['restaurant']['id']
            restaurant_name = self._redis.get(restaurant_id)["name"]
            user_name = self._redis.get(user_id)["name"]

            category_counts = {}
            product_counts = {}

            for product in order["products"]:
                category_id = product["category_id"]
                if category_id not in category_counts:
                    category_counts[category_id] = {
                        "category_name": product["category"],
                        "order_cnt": 0
                    }
                category_counts[category_id]["order_cnt"] += product["quantity"]

                product_id = product["id"]
                product_name = product["name"]
                if product_id not in product_counts:
                    product_counts[product_id] = {
                        "product_name": product_name,
                        "order_cnt": 0
                    }
                product_counts[product_id]["order_cnt"] += product["quantity"]

            for category_id, data in category_counts.items():
                self._dds_repository.user_category_counters_insert(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=data["category_name"],
                    order_cnt=data["order_cnt"],
                    load_dt=datetime.utcnow()
                )

            for product_id, data in product_counts.items():
                self._dds_repository.user_product_counters_insert(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=data["product_name"],
                    order_cnt=data["order_cnt"],
                    load_dt=datetime.utcnow()
                )

        self._logger.info(f"{datetime.utcnow()}: FINISH")
