from .companies_redis import RedisClient
from .workload import (
    create,
)

if __name__ == "__main__":
    RedisClient().set_init_data()
    create()
